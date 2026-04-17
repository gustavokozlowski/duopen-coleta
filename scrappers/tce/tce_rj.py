"""
scrappers/tce/tce_rj.py
Scraper da API de Dados Abertos do TCE-RJ
duopen-coleta · DUOPEN 2026

Coleta dados do municipio de Macae em tres frentes:
	1. Contratos (endpoint /contratos_municipio)
	2. Aditivos (via campos de aditivo em /convenios_municipio)
	3. Obras paralisadas (endpoint /obras_paralisadas)

Observacao sobre autenticacao:
	A API e publica e funciona sem token. Ainda assim, o modulo aceita
	TCE_RJ_TOKEN para compatibilidade futura (header Authorization opcional).

Documentacao:
	https://dados.tcerj.tc.br/api/v1/docs#/
"""

import json
import logging
import os
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# ── Configuracao ─────────────────────────────────────────────────────────────

logging.basicConfig(
	level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
	format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
	datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper.tce_rj")

BASE_URL = os.getenv("TCE_RJ_BASE_URL", "https://dados.tcerj.tc.br/api/v1")
MUNICIPIO = os.getenv("TCE_RJ_MUNICIPIO", "macae")
TOKEN = os.getenv("TCE_RJ_TOKEN", "").strip()

PAGE_SIZE = int(os.getenv("TCE_RJ_PAGE_SIZE", "1000"))
MAX_PAGES = int(os.getenv("TCE_RJ_MAX_PAGES", "100"))
RETRY_ATTEMPTS = int(os.getenv("TCE_RJ_RETRY_ATTEMPTS", "3"))
RETRY_BACKOFF = float(os.getenv("TCE_RJ_RETRY_BACKOFF", "2.0"))
REQUEST_TIMEOUT = int(os.getenv("TCE_RJ_REQUEST_TIMEOUT", "30"))

CACHE_DIR = Path(__file__).parent.parent.parent / "cache"


# ── Utilitarios ──────────────────────────────────────────────────────────────

def _headers() -> dict[str, str]:
	"""Monta headers HTTP com Authorization opcional."""
	headers = {
		"Accept": "application/json",
		"User-Agent": "duopen-coleta/1.0",
	}
	if TOKEN:
		headers["Authorization"] = f"Bearer {TOKEN}"
	return headers


def _build_url(endpoint: str) -> str:
	"""Normaliza endpoint para URL absoluta."""
	return f"{BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"


def _normalize_text(value: Optional[str]) -> str:
	"""Normaliza texto para comparacoes case-insensitive e sem acento."""
	if value is None:
		return ""
	normalized = unicodedata.normalize("NFKD", str(value))
	without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
	return without_accents.strip().lower()


def _municipio_match(value: Optional[str]) -> bool:
	"""Retorna True se o campo informado representa Macae."""
	return _normalize_text(value) == _normalize_text(MUNICIPIO)


def _to_float(value: Any) -> Optional[float]:
	"""Converte valores numericos para float com tolerancia a formato BR."""
	if value is None:
		return None
	if isinstance(value, (int, float)):
		return float(value)

	text = str(value).strip()
	if not text or text.lower() in {"nan", "none", "null"}:
		return None

	text = text.replace("R$", "").replace(" ", "")
	if "," in text and "." in text:
		text = text.replace(".", "").replace(",", ".")
	else:
		text = text.replace(",", ".")

	try:
		return float(text)
	except ValueError:
		return None


def _to_iso_datetime(value: Any) -> Optional[str]:
	"""Converte datas em string/epoch para ISO 8601 em UTC."""
	if value is None:
		return None

	if isinstance(value, str):
		text = value.strip()
		if not text:
			return None

		if text.isdigit():
			value = int(text)
		else:
			for fmt in (
				"%Y-%m-%d",
				"%d/%m/%Y",
				"%Y-%m-%dT%H:%M:%S",
				"%Y-%m-%dT%H:%M:%S.%f",
				"%Y-%m-%dT%H:%M:%S%z",
				"%Y-%m-%dT%H:%M:%S.%f%z",
			):
				try:
					dt = datetime.strptime(text, fmt)
					if dt.tzinfo is None:
						dt = dt.replace(tzinfo=timezone.utc)
					else:
						dt = dt.astimezone(timezone.utc)
					return dt.isoformat()
				except ValueError:
					continue

			try:
				dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
				if dt.tzinfo is None:
					dt = dt.replace(tzinfo=timezone.utc)
				else:
					dt = dt.astimezone(timezone.utc)
				return dt.isoformat()
			except ValueError:
				return text

	if isinstance(value, (int, float)):
		try:
			ts = float(value)
			# Epoch em ms e mais comum na API do TCE-RJ.
			if ts > 1e11:
				ts = ts / 1000.0
			dt = datetime.fromtimestamp(ts, tz=timezone.utc)
			return dt.isoformat()
		except (ValueError, OSError, OverflowError):
			return str(value)

	return str(value)


def _extract_records(data: Any, preferred_keys: tuple[str, ...] = ()) -> list[dict[str, Any]]:
	"""Extrai lista de registros de payloads em formato lista ou objeto."""
	if isinstance(data, list):
		return [item for item in data if isinstance(item, dict)]

	if not isinstance(data, dict):
		return []

	for key in preferred_keys:
		value = data.get(key)
		if isinstance(value, list):
			return [item for item in value if isinstance(item, dict)]

	for key in ("data", "content", "items", "Contratos", "Obras", "Convenios"):
		value = data.get(key)
		if isinstance(value, list):
			return [item for item in value if isinstance(item, dict)]

	for value in data.values():
		if isinstance(value, list):
			return [item for item in value if isinstance(item, dict)]

	return []


def _get(endpoint: str, params: Optional[dict[str, Any]] = None) -> Any:
	"""Executa GET com retry e backoff exponencial."""
	url = _build_url(endpoint)
	params = params or {}

	for attempt in range(1, RETRY_ATTEMPTS + 1):
		try:
			response = requests.get(
				url,
				headers=_headers(),
				params=params,
				timeout=REQUEST_TIMEOUT,
			)

			if response.status_code in (429, 500, 502, 503, 504):
				if attempt == RETRY_ATTEMPTS:
					response.raise_for_status()
				wait = RETRY_BACKOFF * (2 ** (attempt - 1))
				log.warning(
					"HTTP %s em %s (tentativa %s/%s). Aguardando %.1fs.",
					response.status_code,
					endpoint,
					attempt,
					RETRY_ATTEMPTS,
					wait,
				)
				time.sleep(wait)
				continue

			response.raise_for_status()
			return response.json()

		except requests.exceptions.HTTPError:
			raise
		except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
			if attempt == RETRY_ATTEMPTS:
				raise RuntimeError(
					f"Falha apos {RETRY_ATTEMPTS} tentativas em {endpoint}"
				) from exc

			wait = RETRY_BACKOFF * (2 ** (attempt - 1))
			log.warning(
				"Erro de conexao em %s (tentativa %s/%s): %s. Aguardando %.1fs.",
				endpoint,
				attempt,
				RETRY_ATTEMPTS,
				exc,
				wait,
			)
			time.sleep(wait)


def _fetch_paginated(
	endpoint: str,
	params_base: dict[str, Any],
	data_keys: tuple[str, ...],
) -> list[dict[str, Any]]:
	"""Pagina endpoints com parametros inicio/limite."""
	records: list[dict[str, Any]] = []

	for page in range(MAX_PAGES):
		params = {
			**params_base,
			"inicio": page * PAGE_SIZE,
			"limite": PAGE_SIZE,
		}
		payload = _get(endpoint, params)
		items = _extract_records(payload, preferred_keys=data_keys)

		if not items:
			break

		records.extend(items)
		if len(items) < PAGE_SIZE:
			break

		time.sleep(0.2)

	return records


def _tem_aditivo(registro: dict[str, Any]) -> bool:
	"""Identifica se o convenio possui informacao de aditivo."""
	qtd = _to_float(registro.get("QuantidadeAditivos"))
	valor = _to_float(registro.get("ValorAditivos"))
	ultima = registro.get("UltimaDataAditivo")
	flag = _normalize_text(registro.get("ComvenioAditivo") or registro.get("TemAditivos"))

	if qtd is not None and qtd > 0:
		return True
	if valor is not None and valor > 0:
		return True
	if ultima not in (None, "", "null"):
		return True
	return "aditiv" in flag


# ── Coleta por endpoint ──────────────────────────────────────────────────────

def fetch_contratos() -> list[dict[str, Any]]:
	"""Coleta contratos do municipio de Macae com paginação."""
	log.info("Coletando contratos do TCE-RJ para municipio=%s", MUNICIPIO)
	registros = _fetch_paginated(
		endpoint="contratos_municipio",
		params_base={"municipio": MUNICIPIO},
		data_keys=("Contratos",),
	)
	filtrados = [r for r in registros if _municipio_match(r.get("Ente"))]
	log.info("Contratos coletados: %s", len(filtrados))
	return filtrados


def fetch_aditivos() -> list[dict[str, Any]]:
	"""Coleta aditivos a partir do endpoint de convenios municipais."""
	log.info("Coletando aditivos do TCE-RJ para municipio=%s", MUNICIPIO)
	payload = _get("convenios_municipio", {"jsonfull": False})
	registros = _extract_records(payload, preferred_keys=("Convenios",))
	if not registros and isinstance(payload, list):
		registros = [r for r in payload if isinstance(r, dict)]

	filtrados = [
		r
		for r in registros
		if _municipio_match(r.get("Ente")) and _tem_aditivo(r)
	]
	log.info("Aditivos coletados: %s", len(filtrados))
	return filtrados


def fetch_obras() -> list[dict[str, Any]]:
	"""Coleta obras paralisadas e filtra registros do municipio de Macae."""
	log.info("Coletando obras do TCE-RJ para municipio=%s", MUNICIPIO)
	payload = _get("obras_paralisadas", {"jsonfull": False})
	registros = _extract_records(payload, preferred_keys=("Obras",))
	if not registros and isinstance(payload, list):
		registros = [r for r in payload if isinstance(r, dict)]

	filtrados = [r for r in registros if _municipio_match(r.get("Ente"))]
	log.info("Obras coletadas: %s", len(filtrados))
	return filtrados


# ── Normalizacao ─────────────────────────────────────────────────────────────

def normalizar_contratos(registros: list[dict[str, Any]]) -> pd.DataFrame:
	"""Normaliza contratos em DataFrame padrao do projeto."""
	if not registros:
		return pd.DataFrame()

	rows = []
	for registro in registros:
		rows.append(
			{
				"id_contrato": registro.get("NumeroContrato") or registro.get("ProcessoLicitatorio"),
				"fonte": "tce_rj_contratos_municipio",
				"municipio": registro.get("Ente"),
				"unidade_gestora": registro.get("UnidadeGestora"),
				"objeto": registro.get("Objeto"),
				"modalidade": registro.get("Modalidade"),
				"tipo_contrato": registro.get("TipoContrato"),
				"processo_licitatorio": registro.get("ProcessoLicitatorio"),
				"cnpj_cpf_contratado": registro.get("CNPJCPFContratado"),
				"contratado": registro.get("Contratado"),
				"valor_contrato": _to_float(registro.get("ValorContrato")),
				"valor_empenhado": _to_float(registro.get("ValorEmpenhado")),
				"valor_liquidado": _to_float(registro.get("ValorLiquidado")),
				"valor_pago": _to_float(registro.get("ValorPago")),
				"data_assinatura": _to_iso_datetime(registro.get("DataAssinaturaContrato")),
				"data_vencimento": _to_iso_datetime(registro.get("DataVencimentoContrato")),
				"coletado_em": datetime.now(timezone.utc).isoformat(),
				"payload_bruto": json.dumps(registro, ensure_ascii=False),
			}
		)

	return pd.DataFrame(rows)


def normalizar_aditivos(registros: list[dict[str, Any]]) -> pd.DataFrame:
	"""Normaliza aditivos (derivados de convenios) para DataFrame."""
	if not registros:
		return pd.DataFrame()

	rows = []
	for registro in registros:
		numero = registro.get("NumeroConvenio")
		ano = registro.get("AnoConvenio")
		rows.append(
			{
				"id_aditivo": f"{numero}-{ano}",
				"fonte": "tce_rj_convenios_municipio",
				"municipio": registro.get("Ente"),
				"unidade_gestora": registro.get("UnidadeGestora"),
				"numero_convenio": numero,
				"ano_convenio": registro.get("AnoConvenio"),
				"mes_convenio": registro.get("MesConvenio"),
				"tipo_registro": registro.get("ComvenioAditivo"),
				"objeto": registro.get("Objeto"),
				"quantidade_aditivos": _to_float(registro.get("QuantidadeAditivos")),
				"valor_aditivos": _to_float(registro.get("ValorAditivos")),
				"valor_convenio": _to_float(registro.get("Valor")),
				"ultima_data_aditivo": _to_iso_datetime(registro.get("UltimaDataAditivo")),
				"data_assinatura": _to_iso_datetime(registro.get("DataAssinatura")),
				"data_publicacao": _to_iso_datetime(registro.get("DataPublicacao")),
				"coletado_em": datetime.now(timezone.utc).isoformat(),
				"payload_bruto": json.dumps(registro, ensure_ascii=False),
			}
		)

	return pd.DataFrame(rows)


def normalizar_obras(registros: list[dict[str, Any]]) -> pd.DataFrame:
	"""Normaliza obras paralisadas para DataFrame."""
	if not registros:
		return pd.DataFrame()

	rows = []
	for registro in registros:
		numero_contrato = registro.get("NumeroContrato")
		data_paralisacao = _to_iso_datetime(registro.get("DataParalisacao"))
		rows.append(
			{
				"id_obra": f"{numero_contrato}-{data_paralisacao}",
				"fonte": "tce_rj_obras_paralisadas",
				"municipio": registro.get("Ente"),
				"nome_obra": registro.get("Nome"),
				"funcao_governo": registro.get("FuncaoGoverno"),
				"numero_contrato": numero_contrato,
				"cnpj_contratada": registro.get("CNPJContratada"),
				"nome_contratada": registro.get("NomeContratada"),
				"tipo_unidade": registro.get("TipoUnidade"),
				"status_contrato": registro.get("StatusContrato"),
				"classificacao_obra": registro.get("ClassificacaoObra"),
				"tipo_paralisacao": registro.get("TipoParalisacao"),
				"motivo_paralisacao": registro.get("MotivoParalisacao"),
				"tempo_paralisacao": registro.get("TempoParalizacao"),
				"valor_total_contrato": _to_float(registro.get("ValorTotalContrato")),
				"valor_pago_obra": _to_float(registro.get("ValorPagoObra")),
				"data_inicio_obra": _to_iso_datetime(registro.get("DataInicioObra")),
				"data_paralisacao": data_paralisacao,
				"data_ultima_atualizacao": _to_iso_datetime(registro.get("DataUltimaAtualizacao")),
				"coletado_em": datetime.now(timezone.utc).isoformat(),
				"payload_bruto": json.dumps(registro, ensure_ascii=False),
			}
		)

	return pd.DataFrame(rows)


# ── Cache ────────────────────────────────────────────────────────────────────

def _salvar_cache(datasets: dict[str, pd.DataFrame]) -> None:
	"""Salva datasets normalizados no diretorio cache/ em JSON."""
	CACHE_DIR.mkdir(parents=True, exist_ok=True)

	mapping = {
		"contratos": "tce_rj_contratos.json",
		"aditivos": "tce_rj_aditivos.json",
		"obras": "tce_rj_obras.json",
	}

	for key, filename in mapping.items():
		df = datasets.get(key, pd.DataFrame())
		if df.empty:
			continue
		path = CACHE_DIR / filename
		df.to_json(path, orient="records", force_ascii=False, indent=2)
		log.info("Cache salvo: %s (%s registros)", path, len(df))


def _carregar_cache() -> dict[str, pd.DataFrame]:
	"""Carrega caches locais para fallback em caso de falha da API."""
	mapping = {
		"contratos": "tce_rj_contratos.json",
		"aditivos": "tce_rj_aditivos.json",
		"obras": "tce_rj_obras.json",
	}

	resultado: dict[str, pd.DataFrame] = {}
	for key, filename in mapping.items():
		path = CACHE_DIR / filename
		if path.exists():
			resultado[key] = pd.read_json(path, orient="records")
			log.warning("Cache carregado: %s (%s registros)", path.name, len(resultado[key]))
		else:
			resultado[key] = pd.DataFrame()
	return resultado


# ── Pipeline principal ───────────────────────────────────────────────────────

def run() -> dict[str, pd.DataFrame]:
	"""
	Executa pipeline completo do TCE-RJ:
		1. Coleta contratos, aditivos e obras
		2. Normaliza para DataFrames
		3. Salva cache JSON

	Em caso de falha, retorna ultimo cache disponivel.
	"""
	log.info("=" * 60)
	log.info("TCE-RJ - inicio da coleta (municipio=%s)", MUNICIPIO)
	log.info("=" * 60)

	try:
		contratos_raw = fetch_contratos()
		aditivos_raw = fetch_aditivos()
		obras_raw = fetch_obras()

		datasets = {
			"contratos": normalizar_contratos(contratos_raw),
			"aditivos": normalizar_aditivos(aditivos_raw),
			"obras": normalizar_obras(obras_raw),
		}

		if any(not df.empty for df in datasets.values()):
			_salvar_cache(datasets)

	except Exception as exc:
		log.error("Falha na coleta do TCE-RJ: %s. Tentando fallback de cache...", exc)
		datasets = _carregar_cache()

	log.info("=" * 60)
	log.info("Coleta finalizada")
	log.info("Contratos: %s", len(datasets["contratos"]))
	log.info("Aditivos: %s", len(datasets["aditivos"]))
	log.info("Obras: %s", len(datasets["obras"]))
	log.info("=" * 60)
	return datasets


if __name__ == "__main__":
	dados = run()
	for nome, df in dados.items():
		if df.empty:
			print(f"\n{nome}: nenhum dado coletado.")
			continue

		print(f"\n-- {nome.upper()} ({len(df)} registros) --")
		print(df.head(10).to_string(index=False))
