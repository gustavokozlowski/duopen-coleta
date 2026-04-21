import os
import io
import json
import time
import logging
import tempfile
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests
import pandas as pd
import urllib3
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ── Configuração ──────────────────────────────────────────────────────────────

logging.basicConfig(
	level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
	format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
	datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper.painel_atual")

REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2.0
DELAY_PAGINAS = 0.3
TCE_PAGE_LIMIT = 1000
WAIT_TIMEOUT = 25
DOWNLOAD_TIMEOUT = 90

JANELA_DIAS = int(os.getenv("PAINEL_ATUAL_JANELA_DIAS", "730"))
PAINEL_ATUAL_CSV_URL = os.getenv("PAINEL_ATUAL_CSV_URL", "").strip()
PAINEL_OBRASGOV_URL = os.getenv(
	"PAINEL_OBRASGOV_URL",
	"https://dd-publico.serpro.gov.br/extensions/painel/ObrasgovbrIntervencao.html",
)
CHROME_HEADLESS = os.getenv("CHROME_HEADLESS", "True").lower() == "true"
CHROME_NO_SANDBOX = os.getenv("CHROME_NO_SANDBOX", "True").lower() == "true"

CACHE_DIR = Path(__file__).parent.parent.parent / "cache"
CACHE_PATH = CACHE_DIR / "painel_atual.json"

HEADERS = {
	"User-Agent": "duopen-coleta/1.0 (hackathon DUOPEN 2026)",
	"Accept": "*/*",
}

CSV_URLS = [
	# Portal de transparência de Macaé — seção de obras
	"https://transparencia.macae.rj.gov.br/obras/exportarcsv",
	"https://transparencia.macae.rj.gov.br/contratacoes/obras/exportarcsv",
	"https://transparencia.macae.rj.gov.br/obras/csv",

	# Sistema legado (porta 840)
	"https://sistemas.macae.rj.gov.br:840/transparencia/obras/exportarcsv",
	"https://sistemas.macae.rj.gov.br:840/transparencia/contratacoes/obras",

	# EGIM — painel direto
	"https://macae.rj.gov.br/egim/obras/exportar",
	"https://macae.rj.gov.br/egim/painel/csv",

	# GeoMacaé — dados de obras georreferenciadas
	"https://macae.rj.gov.br/geomacae/obras/download",
]
if PAINEL_ATUAL_CSV_URL:
	CSV_URLS.insert(0, PAINEL_ATUAL_CSV_URL)

CONTENT_TYPES_ACEITOS = {
	"text/csv",
	"application/vnd.ms-excel",
	"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

XML_NS = {
	"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
	"r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
	"rel": "http://schemas.openxmlformats.org/package/2006/relationships",
}


# ── Cliente HTTP ──────────────────────────────────────────────────────────────

def _get(url: str, params: dict = None) -> requests.Response:
	"""
	GET com retry (3x) e backoff exponencial (2s base).
	Usa verify=False para portais municipais com cert autoassinado.
	Lança RuntimeError após esgotar tentativas.
	"""
	for attempt in range(1, RETRY_ATTEMPTS + 1):
		resp = None
		try:
			resp = requests.get(
				url,
				params=params or {},
				headers=HEADERS,
				timeout=REQUEST_TIMEOUT,
				verify=False,
			)
			resp.raise_for_status()
			return resp

		except requests.exceptions.HTTPError as exc:
			status = resp.status_code if resp is not None else "?"
			if status == 404:
				log.debug("404 em %s", url)
			elif status in (401, 403):
				log.warning("HTTP %s em %s", status, url)
			else:
				log.warning("HTTP %s em %s", status, url)

			if status in (500, 502, 503, 504) and attempt < RETRY_ATTEMPTS:
				wait = RETRY_BACKOFF * (2 ** (attempt - 1))
				log.warning("Tentativa %s/%s falhou. Aguardando %ss...", attempt, RETRY_ATTEMPTS, wait)
				time.sleep(wait)
				continue

			raise RuntimeError(f"Falha HTTP em {url}: status={status}") from exc

		except (
			requests.exceptions.Timeout,
			requests.exceptions.ConnectionError,
			requests.exceptions.SSLError,
		) as exc:
			log.warning("Tentativa %s/%s falhou em %s: %s", attempt, RETRY_ATTEMPTS, url, exc)
			if attempt == RETRY_ATTEMPTS:
				raise RuntimeError(f"Falha após {RETRY_ATTEMPTS} tentativas: {url}") from exc
			wait = RETRY_BACKOFF * (2 ** (attempt - 1))
			log.info("Aguardando %ss antes da próxima tentativa...", wait)
			time.sleep(wait)


def _inicializar_driver(download_dir: str) -> webdriver.Chrome:
	"""Inicializa Chrome para automação e download automático de arquivos."""
	options = webdriver.ChromeOptions()
	if CHROME_HEADLESS:
		options.add_argument("--headless=new")
	if CHROME_NO_SANDBOX:
		options.add_argument("--no-sandbox")
		options.add_argument("--disable-dev-shm-usage")

	options.add_argument("--disable-gpu")
	options.add_argument("--window-size=1920,1080")
	options.add_argument("--disable-blink-features=AutomationControlled")
	options.add_experimental_option("excludeSwitches", ["enable-automation"])
	options.add_experimental_option("useAutomationExtension", False)
	options.add_experimental_option(
		"prefs",
		{
			"download.default_directory": download_dir,
			"download.prompt_for_download": False,
			"download.directory_upgrade": True,
			"safebrowsing.enabled": False,
		},
	)

	service = Service(ChromeDriverManager().install())
	driver = webdriver.Chrome(service=service, options=options)
	log.info("WebDriver Chrome inicializado para coleta no Obrasgov")
	return driver


def _clicar_em_algum(driver: webdriver.Chrome, xpaths: list[str], timeout: int = WAIT_TIMEOUT) -> bool:
	"""Tenta clicar no primeiro elemento visível/clicável de uma lista de XPaths."""
	for xpath in xpaths:
		try:
			elemento = WebDriverWait(driver, timeout).until(
				EC.element_to_be_clickable((By.XPATH, xpath))
			)
			driver.execute_script("arguments[0].click();", elemento)
			return True
		except Exception:
			continue
	return False


def _esperar_download(download_dir: str, timeout: int = DOWNLOAD_TIMEOUT) -> Optional[Path]:
	"""Aguarda o término do download e retorna o caminho do arquivo mais recente."""
	inicio = time.time()
	base = Path(download_dir)
	while (time.time() - inicio) < timeout:
		arquivos = [
			p
			for p in base.glob("*")
			if p.is_file() and not p.name.endswith(".crdownload") and not p.name.endswith(".tmp")
		]
		if arquivos:
			candidato = max(arquivos, key=lambda p: p.stat().st_mtime)
			tamanho_1 = candidato.stat().st_size
			time.sleep(1)
			tamanho_2 = candidato.stat().st_size
			if tamanho_1 == tamanho_2 and tamanho_2 > 512:
				return candidato
		time.sleep(1)
	return None


def _selecionar_municipio_macae(driver: webdriver.Chrome) -> None:
	"""Seleciona o município de Macaé no filtro superior do painel."""
	if not _clicar_em_algum(
		driver,
		[
			"//*[contains(@data-testid, 'collapsed-title-Munic')]",
			"//*[contains(@data-testid, 'collapsed-title-Município')]",
		],
	):
		raise RuntimeError("Não foi possível abrir o filtro de município.")

	try:
		caixa_busca = WebDriverWait(driver, WAIT_TIMEOUT).until(
			EC.visibility_of_element_located((By.XPATH, "//*[@data-testid='search-input-field']"))
		)
	except Exception:
		raise RuntimeError("Não foi possível localizar a busca de município.")

	caixa_busca.click()
	caixa_busca.send_keys(Keys.CONTROL, "a")
	caixa_busca.send_keys(Keys.BACKSPACE)
	caixa_busca.send_keys("Macaé")
	time.sleep(1)

	if not _clicar_em_algum(
		driver,
		[
			"//*[@data-testid='listbox.item'][contains(., 'Macaé')]",
			"//*[@data-testid='listbox.item'][contains(., 'Macae')]",
		],
		timeout=8,
	):
		# Fallback: enter pode aplicar o item pesquisado na lista.
		caixa_busca.send_keys(Keys.ENTER)

	if not _clicar_em_algum(
		driver,
		[
			"//*[@data-testid='actions-toolbar-confirm']",
		],
		timeout=3,
	):
		caixa_busca.send_keys(Keys.ENTER)
	time.sleep(2)


def _exportar_obrasgov_via_selenium() -> Optional[pd.DataFrame]:
	"""
	Abre o painel Obrasgov, filtra município de Macaé e exporta a Lista de Intervenções.
	"""
	log.info("Estratégia A1 — tentando Painel Obrasgov via Selenium")
	download_dir = tempfile.mkdtemp(prefix="obrasgov_macae_")
	driver = None

	try:
		driver = _inicializar_driver(download_dir)
		driver.get(PAINEL_OBRASGOV_URL)
		WebDriverWait(driver, WAIT_TIMEOUT).until(
			EC.presence_of_element_located((By.XPATH, "//*[contains(., 'Lista das Intervenções') or contains(., 'Intervenção') ]"))
		)

		_selecionar_municipio_macae(driver)

		# Garante que o botão de exportação no rodapé esteja visível.
		driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
		time.sleep(1)

		if not _clicar_em_algum(
			driver,
			[
				"//div[contains(@class,'card')][.//*[contains(normalize-space(), 'Lista das Intervenções')]]//button[contains(@class, 'export-data') and contains(., 'Baixar')]",
				"//div[contains(@class,'card')][.//*[contains(normalize-space(), 'Lista das Intervenções')]]//button[contains(., 'Baixar')]",
			],
		):
			raise RuntimeError("Não foi possível clicar no botão 'Baixar'.")

		arquivo = _esperar_download(download_dir)
		if arquivo is None:
			raise RuntimeError("Download não concluído no tempo esperado.")
		log.info("Arquivo exportado do Obrasgov: %s (%s bytes)", arquivo.name, arquivo.stat().st_size)

		conteudo = arquivo.read_bytes()
		df = _ler_arquivo(conteudo, content_type="")
		if df.empty:
			return None

		log.info("Estratégia A1 sucesso com %s registros (%s)", len(df), arquivo.name)
		return df

	except Exception as exc:
		log.warning("Estratégia A1 falhou: %s", exc)
		return None

	finally:
		if driver is not None:
			driver.quit()


def _normalizar_texto(valor: object) -> str:
	"""Normaliza texto para comparação case-insensitive e sem acentuação."""
	if valor is None:
		return ""
	texto = str(valor).strip().lower()
	texto = unicodedata.normalize("NFKD", texto)
	return "".join(ch for ch in texto if not unicodedata.combining(ch))


def _col_idx_excel(ref: str) -> int:
	"""Converte referência de coluna Excel (A, B, AA...) para índice 0-based."""
	col = "".join(ch for ch in ref if ch.isalpha()).upper()
	idx = 0
	for ch in col:
		idx = idx * 26 + (ord(ch) - ord("A") + 1)
	return max(idx - 1, 0)


def _ler_xlsx_sem_openpyxl(conteudo: bytes) -> pd.DataFrame:
	"""Lê XLSX via XML puro (fallback quando openpyxl não está instalado)."""
	with zipfile.ZipFile(io.BytesIO(conteudo)) as zf:
		workbook = ET.fromstring(zf.read("xl/workbook.xml"))
		sheets = workbook.findall("a:sheets/a:sheet", XML_NS)
		if not sheets:
			raise ValueError("XLSX sem planilhas.")

		first_sheet = sheets[0]
		rel_id = first_sheet.attrib.get(f"{{{XML_NS['r']}}}id")
		if not rel_id:
			raise ValueError("Relação da primeira planilha não encontrada.")

		rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
		target = None
		for rel in rels.findall("rel:Relationship", XML_NS):
			if rel.attrib.get("Id") == rel_id:
				target = rel.attrib.get("Target")
				break
		if not target:
			raise ValueError("Target da planilha não encontrado.")

		sheet_path = target if target.startswith("xl/") else f"xl/{target}"
		if sheet_path not in zf.namelist():
			sheet_path = sheet_path.replace("xl//", "xl/")
		if sheet_path not in zf.namelist():
			raise ValueError("XML da planilha não encontrado no XLSX.")

		shared = []
		if "xl/sharedStrings.xml" in zf.namelist():
			shared_xml = ET.fromstring(zf.read("xl/sharedStrings.xml"))
			for si in shared_xml.findall("a:si", XML_NS):
				texts = [t.text or "" for t in si.findall(".//a:t", XML_NS)]
				shared.append("".join(texts))

		sheet_xml = ET.fromstring(zf.read(sheet_path))
		rows_raw = []
		max_col = 0
		for row in sheet_xml.findall(".//a:sheetData/a:row", XML_NS):
			row_map: dict[int, str] = {}
			for cell in row.findall("a:c", XML_NS):
				ref = cell.attrib.get("r", "A1")
				col_idx = _col_idx_excel(ref)
				max_col = max(max_col, col_idx)
				ctype = cell.attrib.get("t", "")
				valor = ""

				if ctype == "inlineStr":
					is_node = cell.find("a:is", XML_NS)
					if is_node is not None:
						valor = "".join((t.text or "") for t in is_node.findall(".//a:t", XML_NS))
				else:
					v = cell.find("a:v", XML_NS)
					if v is not None and v.text is not None:
						if ctype == "s":
							idx = int(v.text)
							valor = shared[idx] if 0 <= idx < len(shared) else ""
						else:
							valor = v.text

				row_map[col_idx] = valor

			linha = [row_map.get(i, "") for i in range(max_col + 1)]
			rows_raw.append(linha)

	if not rows_raw:
		return pd.DataFrame()

	headers = [str(h).strip() if str(h).strip() else f"col_{i+1}" for i, h in enumerate(rows_raw[0])]
	data = rows_raw[1:]
	df = pd.DataFrame(data, columns=headers)

	# Remove colunas inteiramente vazias para reduzir ruído de exportações do Qlik.
	df = df.dropna(axis=1, how="all")
	df = df.loc[:, ~(df.apply(lambda c: c.astype(str).str.strip().eq("").all(), axis=0))]
	return df


# ── Estratégia A — CSV/XLSX direto ────────────────────────────────────────────

def _ler_arquivo(conteudo: bytes, content_type: str = "") -> pd.DataFrame:
	"""
	Tenta ler bytes como CSV ou XLSX.
	Testa encodings: utf-8-sig, latin-1, cp1252.
	Testa separadores CSV: ";", ",", "\t".
	Para XLSX: usar pd.read_excel() direto.
	Retorna DataFrame ou levanta ValueError se nenhum formato funcionar.
	"""
	content_type = (content_type or "").lower()
	eh_excel = (
		"openxmlformats-officedocument.spreadsheetml.sheet" in content_type
		or "application/vnd.ms-excel" in content_type
		or conteudo.startswith(b"PK\x03\x04")
	)

	if eh_excel:
		try:
			df_excel = pd.read_excel(io.BytesIO(conteudo), dtype=str)
			if df_excel.shape[1] >= 2 and len(df_excel) >= 1:
				return df_excel
		except Exception as exc:
			log.debug("Falha no read_excel padrão (%s). Tentando fallback XML...", exc)
			try:
				df_excel_fallback = _ler_xlsx_sem_openpyxl(conteudo)
				if df_excel_fallback.shape[1] >= 2 and len(df_excel_fallback) >= 1:
					return df_excel_fallback
			except Exception as fallback_exc:
				log.debug("Falha no fallback XML para XLSX: %s", fallback_exc)

	encodings = ["utf-8-sig", "latin-1", "cp1252"]
	separadores = [";", ",", "\t"]
	for enc in encodings:
		for sep in separadores:
			try:
				df = pd.read_csv(
					io.BytesIO(conteudo),
					sep=sep,
					encoding=enc,
					dtype=str,
					on_bad_lines="skip",
				)
				if df.shape[1] >= 2 and len(df) >= 1:
					return df
				log.debug("CSV inválido [enc=%s sep=%r]: shape=%s", enc, sep, df.shape)
			except Exception:
				continue

	raise ValueError("Não foi possível ler arquivo como CSV/XLSX válido.")


def _tentar_csv_direto() -> Optional[pd.DataFrame]:
	"""
	Tenta baixar CSV/XLSX de obra diretamente de URLs candidatas.
	Retorna DataFrame ou None se nenhuma URL funcionar.
	"""
	log.info("Estratégia A — tentando Painel Obrasgov e URL direta de CSV/XLSX")

	df_obrasgov = _exportar_obrasgov_via_selenium()
	if df_obrasgov is not None and not df_obrasgov.empty:
		return df_obrasgov

	for url in CSV_URLS:
		try:
			resp = _get(url)
		except RuntimeError as exc:
			log.debug("URL inválida ou indisponível: %s (%s)", url, exc)
			continue

		content_type = resp.headers.get("Content-Type", "").split(";")[0].strip().lower()
		content_len = len(resp.content)

		if content_len <= 500:
			log.debug("Conteúdo muito pequeno em %s (%s bytes)", url, content_len)
			continue

		if content_type and content_type not in CONTENT_TYPES_ACEITOS:
			if not url.lower().endswith((".csv", ".xlsx", ".xls")):
				log.debug("Content-Type não aceito em %s: %s", url, content_type)
				continue

		try:
			df = _ler_arquivo(resp.content, content_type=content_type)
			log.info("Estratégia A sucesso com %s registros (URL: %s)", len(df), url)
			return df
		except ValueError as exc:
			log.debug("Arquivo inválido em %s: %s", url, exc)
			continue

	log.warning("Estratégia A falhou: nenhuma URL direta retornou dados válidos")
	return None


# ── Estratégia B — Portal de Transparência ────────────────────────────────────

def _tentar_portal_transparencia() -> Optional[pd.DataFrame]:
	"""
	Reutiliza portal_macae.fetch_contratos() e filtra obras ativas.
	"""
	log.info("Estratégia B — tentando Portal de Transparência de Macaé")
	try:
		from scrappers.macae.portal_macae import fetch_contratos, normalizar_contratos
	except Exception:
		try:
			from portal_macae import fetch_contratos, normalizar_contratos
		except Exception as exc:
			log.warning("Falha ao importar portal_macae: %s", exc)
			return None

	try:
		df_raw = fetch_contratos()
		if df_raw is None or df_raw.empty:
			log.warning("Estratégia B retornou DataFrame vazio na coleta de contratos")
			return None

		df_portal = normalizar_contratos(df_raw)
		if df_portal.empty:
			log.warning("Estratégia B retornou DataFrame vazio após normalizar_contratos")
			return None

		log.info("Estratégia B sucesso com %s registros", len(df_portal))
		return df_portal

	except Exception as exc:
		log.warning("Estratégia B falhou: %s", exc)
		return None


# ── Estratégia C — TCE-RJ recente ─────────────────────────────────────────────

def _extrair_registros_tce(payload: object) -> list[dict]:
	if isinstance(payload, list):
		return [item for item in payload if isinstance(item, dict)]

	if isinstance(payload, dict):
		chaves = [
			"Contratos",
			"contratos",
			"data",
			"results",
			"content",
			"items",
		]
		for chave in chaves:
			itens = payload.get(chave)
			if isinstance(itens, list):
				return [item for item in itens if isinstance(item, dict)]

	return []


def _paginar_tce_contratos(ano: int) -> list[dict]:
	registros: list[dict] = []
	inicio = 0
	base_url = "https://dados.tcerj.tc.br/api/v1/contratos"

	while True:
		params = {
			"ano": ano,
			"inicio": inicio,
			"limite": TCE_PAGE_LIMIT,
			"csv": "false",
			"jsonfull": "false",
			"municipio": "MACAE",
		}

		try:
			resp = _get(base_url, params=params)
		except RuntimeError as exc:
			log.warning("Erro paginando TCE-RJ (ano=%s, inicio=%s): %s", ano, inicio, exc)
			break

		try:
			payload = resp.json()
		except ValueError:
			log.warning("Payload não-JSON em TCE-RJ (ano=%s, inicio=%s)", ano, inicio)
			break

		itens = _extrair_registros_tce(payload)
		if not itens:
			break

		registros.extend(itens)
		if len(itens) < TCE_PAGE_LIMIT:
			break

		inicio += TCE_PAGE_LIMIT
		time.sleep(DELAY_PAGINAS)

	return registros


def _tentar_tce_rj_recente() -> Optional[pd.DataFrame]:
	"""
	Busca contratos dos últimos 2 anos via TCE-RJ e filtra os ativos.
	"""
	log.info("Estratégia C — tentando API TCE-RJ com contratos recentes")
	anos_recentes = [datetime.now().year, datetime.now().year - 1]
	todos: list[dict] = []

	for ano in anos_recentes:
		registros = _paginar_tce_contratos(ano)
		log.info("TCE-RJ ano=%s: %s registros", ano, len(registros))
		todos.extend(registros)

	if not todos:
		log.warning("Estratégia C sem dados")
		return None

	df = pd.DataFrame(todos)
	if df.empty:
		return None

	c_municipio = _coluna_por_nome(df, "municipio", "ente")
	if c_municipio:
		filtro = df[c_municipio].fillna("").astype(str).str.lower().str.contains("maca")
		df = df[filtro].copy()

	if df.empty:
		log.warning("Estratégia C sem registros de Macaé após filtro por município")
		return None

	log.info("Estratégia C sucesso com %s registros", len(df))
	return df


# ── Filtro de obras ativas ────────────────────────────────────────────────────

def _coluna_por_nome(df: pd.DataFrame, *candidatos: str) -> Optional[str]:
	for candidato in candidatos:
		candidato_norm = _normalizar_texto(candidato)
		for col in df.columns:
			if candidato_norm in _normalizar_texto(col):
				return col
	return None


def _parse_data_utc(valor: object) -> Optional[datetime]:
	if valor is None:
		return None

	texto = str(valor).strip()
	if texto in ("", "nan", "None", "NaT"):
		return None

	formatos = [
		"%d/%m/%Y",
		"%Y-%m-%d",
		"%Y-%m-%d %H:%M:%S",
		"%d/%m/%Y %H:%M:%S",
		"%Y-%m-%dT%H:%M:%S",
	]
	for fmt in formatos:
		try:
			return datetime.strptime(texto, fmt).replace(tzinfo=timezone.utc)
		except ValueError:
			continue

	try:
		dt = datetime.fromisoformat(texto.replace("Z", "+00:00"))
		if dt.tzinfo is None:
			dt = dt.replace(tzinfo=timezone.utc)
		return dt.astimezone(timezone.utc)
	except ValueError:
		return None


def _filtrar_obras_ativas(df: pd.DataFrame) -> pd.DataFrame:
	"""
	Mantém apenas obras consideradas 'ativas':
	- situacao contém: "andamento", "execução", "iniciada", "vigente"
	- OU data_prevista_fim >= hoje - JANELA_DIAS
	- OU data_inicio >= hoje - JANELA_DIAS
	Remove obras com situacao contendo: "cancelada", "rescindida", "concluída"
	"""
	if df is None or df.empty:
		return pd.DataFrame() if df is None else df

	c_situacao = _coluna_por_nome(df, "situacao", "situação", "status")
	c_data_fim = _coluna_por_nome(df, "data_prevista_fim", "data final prevista", "data fim", "fim", "final", "vencimento", "vigencia", "vigência")
	c_data_inicio = _coluna_por_nome(df, "data_inicio", "data inicial prevista", "inicio", "início", "inicial", "assinatura", "cadastro")

	limite = datetime.now(timezone.utc) - timedelta(days=JANELA_DIAS)
	mascara_ativos_situacao = pd.Series(False, index=df.index)
	mascara_inativos_situacao = pd.Series(False, index=df.index)
	mascara_data_fim = pd.Series(False, index=df.index)
	mascara_data_inicio = pd.Series(False, index=df.index)

	if c_situacao:
		situacao = df[c_situacao].fillna("").astype(str).str.lower()
		mascara_ativos_situacao = situacao.str.contains("andamento|execu|iniciada|vigente", regex=True)
		mascara_inativos_situacao = situacao.str.contains("cancelada|rescindida|conclu[ií]da", regex=True)

	if c_data_fim:
		data_fim = df[c_data_fim].apply(_parse_data_utc)
		mascara_data_fim = data_fim.apply(lambda dt: dt is not None and dt >= limite)

	if c_data_inicio:
		data_inicio = df[c_data_inicio].apply(_parse_data_utc)
		mascara_data_inicio = data_inicio.apply(lambda dt: dt is not None and dt >= limite)

	mascara_final = (mascara_ativos_situacao | mascara_data_fim | mascara_data_inicio) & (~mascara_inativos_situacao)
	filtrado = df[mascara_final].copy()
	log.info("Filtro de obras ativas: %s -> %s", len(df), len(filtrado))
	return filtrado


def _filtrar_municipio_macae(df: pd.DataFrame) -> pd.DataFrame:
	"""Mantém somente linhas do município de Macaé quando a coluna existir."""
	if df is None or df.empty:
		return pd.DataFrame() if df is None else df

	c_municipio = _coluna_por_nome(df, "municipio")
	if not c_municipio:
		return df

	mascara = df[c_municipio].apply(lambda v: "macae" in _normalizar_texto(v))
	filtrado = df[mascara].copy()
	log.info("Filtro município Macaé: %s -> %s", len(df), len(filtrado))
	return filtrado


# ── Normalização ──────────────────────────────────────────────────────────────

def _valor(row: pd.Series, coluna: Optional[str]) -> Optional[str]:
	if coluna and coluna in row.index:
		val = row[coluna]
		if pd.isna(val):
			return None
		texto = str(val).strip()
		return None if texto in ("", "nan", "None", "NaT") else texto
	return None


def _float_monetario(valor: object) -> Optional[float]:
	if valor is None:
		return None

	texto = str(valor).strip()
	if texto in ("", "nan", "None", "NaT"):
		return None

	texto = texto.replace("R$", "").replace(" ", "")
	if "," in texto and "." in texto:
		if texto.rfind(",") > texto.rfind("."):
			texto = texto.replace(".", "").replace(",", ".")
		else:
			texto = texto.replace(",", "")
	elif "," in texto:
		texto = texto.replace(",", ".")

	try:
		return float(texto)
	except ValueError:
		return None


def _data_iso_utc(valor: object) -> Optional[str]:
	dt = _parse_data_utc(valor)
	return dt.isoformat() if dt else None


def normalizar(df: pd.DataFrame) -> pd.DataFrame:
	"""
	Normaliza DataFrame bruto para schema padronizado de obras atuais.
	"""
	obrigatorias = [
		"id_obra",
		"nome_obra",
		"situacao",
		"percentual_executado",
		"valor_contrato",
		"data_inicio",
		"data_prevista_fim",
		"secretaria",
		"bairro",
		"fonte",
		"coletado_em",
		"payload_bruto",
	]
	opcionais = [
		"latitude",
		"longitude",
		"cnpj_executora",
		"nome_executora",
		"valor_aditivos",
		"num_contrato",
		"num_licitacao",
	]

	if df is None or df.empty:
		return pd.DataFrame(columns=obrigatorias + opcionais)

	c_id = _coluna_por_nome(df, "id_obra", "identificador unico", "identificador", "id_contrato", "numero contrato", "número", "id")
	c_nome = _coluna_por_nome(df, "nome_obra", "nome da obra", "objeto", "descricao", "descrição")
	c_situacao = _coluna_por_nome(df, "situacao", "situação", "status")
	c_percentual = _coluna_por_nome(df, "percentual_executado", "percentual", "execucao", "execução")
	c_valor = _coluna_por_nome(df, "valor_contrato", "investimento previsto", "valor", "montante")
	c_data_inicio = _coluna_por_nome(df, "data_inicio", "data inicial prevista", "inicio", "início", "inicial", "assinatura", "cadastro")
	c_data_fim = _coluna_por_nome(df, "data_prevista_fim", "data final prevista", "previsao", "previsão", "vigencia", "vigência", "fim", "final", "vencimento")
	c_secretaria = _coluna_por_nome(df, "secretaria", "orgao", "órgão", "unidade", "repassador")
	c_bairro = _coluna_por_nome(df, "bairro", "local", "distrito", "endereco", "endereço")

	c_latitude = _coluna_por_nome(df, "latitude", "lat")
	c_longitude = _coluna_por_nome(df, "longitude", "lon", "lng")
	c_cnpj = _coluna_por_nome(df, "cnpj_executora", "cnpj_fornecedor", "cnpjcpfcontratado", "cnpj")
	c_exec = _coluna_por_nome(df, "nome_executora", "executor da obra", "fornecedor", "contratado", "empresa", "nome_fornecedor")
	c_aditivos = _coluna_por_nome(df, "valor_aditivos", "aditivo")
	c_num_contrato = _coluna_por_nome(df, "num_contrato", "numero contrato", "numerocontrato")
	c_num_licitacao = _coluna_por_nome(df, "num_licitacao", "processo licitatorio", "licitacao", "edital")

	linhas = []
	coletado_em = datetime.now(timezone.utc).isoformat()

	for idx, row in df.iterrows():
		payload = row.to_dict()
		id_obra = _valor(row, c_id)
		if not id_obra:
			id_obra = f"obra_{idx + 1}"

		linhas.append(
			{
				"id_obra": id_obra,
				"nome_obra": _valor(row, c_nome),
				"situacao": _valor(row, c_situacao),
				"percentual_executado": _float_monetario(_valor(row, c_percentual)),
				"valor_contrato": _float_monetario(_valor(row, c_valor)),
				"data_inicio": _data_iso_utc(_valor(row, c_data_inicio)),
				"data_prevista_fim": _data_iso_utc(_valor(row, c_data_fim)),
				"secretaria": _valor(row, c_secretaria),
				"bairro": _valor(row, c_bairro),
				"fonte": "painel_obras_atual_macae",
				"coletado_em": coletado_em,
				"payload_bruto": json.dumps(payload, ensure_ascii=False, default=str),
				"latitude": _float_monetario(_valor(row, c_latitude)),
				"longitude": _float_monetario(_valor(row, c_longitude)),
				"cnpj_executora": _valor(row, c_cnpj),
				"nome_executora": _valor(row, c_exec),
				"valor_aditivos": _float_monetario(_valor(row, c_aditivos)),
				"num_contrato": _valor(row, c_num_contrato),
				"num_licitacao": _valor(row, c_num_licitacao),
			}
		)

	resultado = pd.DataFrame(linhas, columns=obrigatorias + opcionais)
	log.info("Normalização concluída: %s registros", len(resultado))
	return resultado


# ── Cache ─────────────────────────────────────────────────────────────────────

def _salvar_cache(df: pd.DataFrame) -> None:
	"""Salva DataFrame em JSON orientado por registros."""
	if df is None or df.empty:
		return
	CACHE_DIR.mkdir(parents=True, exist_ok=True)
	df.to_json(CACHE_PATH, orient="records", force_ascii=False, indent=2)
	log.info("Cache salvo: %s (%s registros)", CACHE_PATH.name, len(df))


def _carregar_cache() -> Optional[pd.DataFrame]:
	"""Carrega cache. Retorna None se arquivo não existir."""
	if not CACHE_PATH.exists():
		return None
	try:
		df = pd.read_json(CACHE_PATH, orient="records")
		log.warning("Cache carregado: %s (%s registros)", CACHE_PATH.name, len(df))
		return df
	except Exception as exc:
		log.error("Erro ao carregar cache %s: %s", CACHE_PATH, exc)
		return None


# ── Pipeline principal ────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
	"""
	Pipeline completo de coleta do Painel de Obras/Intervenções de Macaé.

	Tenta as estratégias em cascata:
		A → CSV/XLSX direto (URLs candidatas)
		B → Portal de Transparência de Macaé (POST)
		C → TCE-RJ contratos recentes (API)
		D → Cache local (último recurso)

	Para a Estratégia A (Obrasgov), retorna todas as intervenções de Macaé
	exportadas na "Lista das Intervenções".
	Para estratégias de fallback, mantém filtro de obras ativas.
	Salva cache após sucesso.

	Returns:
		pd.DataFrame com intervenções/obras normalizadas.
		DataFrame vazio se todas as estratégias falharem e cache inexistente.
	"""
	estrategias = [
		("A", _tentar_csv_direto),
		("B", _tentar_portal_transparencia),
		("C", _tentar_tce_rj_recente),
	]

	for nome, func in estrategias:
		log.info("Iniciando estratégia %s", nome)
		try:
			bruto = func()
		except Exception as exc:
			log.warning("Estratégia %s falhou com exceção: %s", nome, exc)
			continue

		if bruto is None or bruto.empty:
			log.warning("Estratégia %s sem dados", nome)
			continue

		bruto = _filtrar_municipio_macae(bruto)
		if bruto.empty:
			log.warning("Estratégia %s sem dados de Macaé após filtro de município", nome)
			continue

		normalizado = normalizar(bruto)
		if normalizado.empty:
			log.warning("Estratégia %s retornou dados vazios após normalização", nome)
			continue

		if nome == "A":
			resultado = normalizado
			log.info("Estratégia A retornou %s intervenções de Macaé (sem filtro de ativos)", len(resultado))
		else:
			resultado = _filtrar_obras_ativas(normalizado)
			if resultado.empty:
				log.warning("Estratégia %s retornou 0 obras ativas após filtro", nome)
				continue

		_salvar_cache(resultado)
		log.info("Total final de registros retornados: %s", len(resultado))
		return resultado

	log.error("Todas as estratégias falharam. Tentando cache local.")
	cache_df = _carregar_cache()
	if cache_df is not None and not cache_df.empty:
		log.warning("Retornando dados do cache local")
		log.info("Total final de registros retornados: %s", len(cache_df))
		return cache_df

	log.error("Cache não existe ou está vazio. Retornando DataFrame vazio.")
	return pd.DataFrame()


if __name__ == "__main__":
	df = run()
	if df.empty:
		print("Nenhuma intervenção coletada para Macaé.")
	else:
		print(f"\n── Painel Atual · {len(df)} intervenções em Macaé ──\n")
		cols = [c for c in [
			"id_obra", "nome_obra", "situacao",
			"percentual_executado", "valor_contrato",
			"data_prevista_fim", "secretaria",
		] if c in df.columns]
		print(df[cols].head(20).to_string(index=False))
