"""
scrapers/macae/egim.py
Scraper EGIM — Obras e Projetos Georreferenciados da Prefeitura de Macaé
duopen-coleta · DUOPEN 2026

Coleta obras georreferenciadas monitoradas pelo Escritório de Gestão,
Indicadores e Metas (EGIM) da Prefeitura de Macaé via Google My Maps.

Fonte:
    Google My Maps público do EGIM:
    https://www.google.com/maps/d/viewer?mid=1tu5J6jl4n2xcg2A_uMpThe7H2E07nYc

    O Google My Maps expõe um endpoint de download KML para mapas públicos:
    https://www.google.com/maps/d/kml?mid={MAP_ID}&forcekml=1

    O KML contém todas as camadas (layers) do mapa, cada uma com placemarks
    que representam obras/projetos com: nome, descrição, coordenadas (lat/lng),
    ícone/cor (que indica status: laranja = concluída, azul = em andamento).

Fluxo:
    1. Baixar KML completo do Google My Maps
    2. Parsear camadas e placemarks via xml.etree.ElementTree
    3. Extrair coordenadas, nome, descrição e status de cada obra
    4. Normalizar em DataFrame pronto para o ETL

Variáveis de ambiente (.env):
    EGIM_MAP_ID   ID do mapa Google My Maps do EGIM
                  (padrão: 1tu5J6jl4n2xcg2A_uMpThe7H2E07nYc)
    LOG_LEVEL     nível de log (padrão: INFO)
"""

import os
import json
import time
import logging
import zipfile
import re
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET

import requests
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ── Configuração ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper.egim")

MAP_ID          = os.getenv("EGIM_MAP_ID", "1tu5J6jl4n2xcg2A_uMpThe7H2E07nYc")
KML_URL         = f"https://www.google.com/maps/d/kml?mid={MAP_ID}&forcekml=1"
KMZ_URL         = f"https://www.google.com/maps/d/kml?mid={MAP_ID}&forcekml=0"
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS  = 3
RETRY_BACKOFF   = 2.0
CACHE_DIR       = Path(__file__).parent.parent.parent / "cache"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; duopen-coleta/1.0; "
        "hackathon DUOPEN 2026)"
    ),
    "Accept": "application/vnd.google-earth.kml+xml, application/octet-stream, */*",
}

# Namespaces do KML
NS = {
    "kml":   "http://www.opengis.net/kml/2.2",
    "gx":    "http://www.google.com/kml/ext/2.2",
    "atom":  "http://www.w3.org/2005/Atom",
}

# Mapeamento de cores de ícone para status da obra
# Google My Maps usa URLs de ícones com cores embutidas no nome
COR_STATUS = {
    "orange":  "concluída",
    "laranja": "concluída",
    "yellow":  "concluída",
    "blue":    "em andamento",
    "azul":    "em andamento",
    "ltblue":  "em andamento",
    "green":   "em andamento",
    "red":     "paralisada",
    "vermelho": "paralisada",
    "purple":  "planejada",
    "gray":    "indefinido",
    "grey":    "indefinido",
}


# ── Cliente HTTP ──────────────────────────────────────────────────────────────

def _get(url: str) -> requests.Response:
    """GET com retry e backoff exponencial."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            log.error(f"HTTP {resp.status_code} em {url}: {e}")
            raise
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            log.warning(f"Tentativa {attempt}/{RETRY_ATTEMPTS} falhou: {e}")
            if attempt == RETRY_ATTEMPTS:
                raise RuntimeError(
                    f"Falha após {RETRY_ATTEMPTS} tentativas: {url}"
                ) from e
            wait = RETRY_BACKOFF * (2 ** (attempt - 1))
            log.info(f"Aguardando {wait}s...")
            time.sleep(wait)


# ── Download e extração do KML ────────────────────────────────────────────────

def _extrair_kml_de_kmz(conteudo: bytes) -> bytes:
    """
    Extrai o arquivo .kml de dentro de um .kmz (que é um ZIP).
    Retorna os bytes do KML.
    """
    with zipfile.ZipFile(BytesIO(conteudo)) as z:
        for nome in z.namelist():
            if nome.endswith(".kml"):
                log.info(f"KML encontrado dentro do KMZ: {nome}")
                return z.read(nome)
    raise ValueError("Nenhum arquivo .kml encontrado dentro do KMZ.")


def download_kml() -> bytes:
    """
    Baixa o KML do Google My Maps do EGIM.

    Tenta primeiro o endpoint KML direto (forcekml=1),
    depois o KMZ (forcekml=0) e extrai o KML de dentro.

    Retorna bytes do KML.
    """
    # Tentativa 1: KML direto
    log.info(f"Baixando KML do EGIM: {KML_URL}")
    try:
        resp = _get(KML_URL)
        content_type = resp.headers.get("Content-Type", "")
        conteudo = resp.content

        if b"<kml" in conteudo[:500]:
            log.info(f"KML obtido: {len(conteudo):,} bytes")
            return conteudo

        # Pode ter vindo como KMZ mesmo com forcekml=1
        if b"PK" == conteudo[:2]:
            log.info("Resposta é KMZ — extraindo KML interno...")
            return _extrair_kml_de_kmz(conteudo)

    except Exception as e:
        log.warning(f"KML direto falhou: {e}")

    # Tentativa 2: KMZ
    log.info(f"Tentando KMZ: {KMZ_URL}")
    resp = _get(KMZ_URL)
    conteudo = resp.content

    if b"PK" == conteudo[:2]:
        log.info(f"KMZ obtido: {len(conteudo):,} bytes — extraindo KML...")
        return _extrair_kml_de_kmz(conteudo)

    if b"<kml" in conteudo[:500]:
        return conteudo

    raise RuntimeError("Não foi possível obter KML válido do Google My Maps.")


# ── Parser KML ────────────────────────────────────────────────────────────────

def _tag(elemento: ET.Element, *caminhos: str) -> Optional[str]:
    """
    Busca texto de um sub-elemento por múltiplos caminhos (fallback).
    Tenta com e sem namespace kml:.
    """
    for caminho in caminhos:
        # Com namespace
        partes_ns = "/".join(f"kml:{p}" for p in caminho.split("/"))
        el = elemento.find(partes_ns, NS)
        if el is not None and el.text:
            return el.text.strip()
        # Sem namespace (KML mal-formado)
        el = elemento.find(caminho)
        if el is not None and el.text:
            return el.text.strip()
    return None


def _inferir_status_por_icone(placemark: ET.Element) -> str:
    """
    Tenta inferir o status da obra pela cor do ícone definida no Style.
    No Google My Maps, as cores dos marcadores são codificadas na URL do ícone.
    """
    # Buscar href do ícone no Style inline ou via StyleUrl
    href = _tag(placemark, "Style/IconStyle/Icon/href")
    if not href:
        # No KML exportado do My Maps a tag costuma ser styleUrl (camelCase).
        style_url = _tag(placemark, "styleUrl", "StyleUrl")
        if style_url:
            href = style_url

    if href:
        href_lower = href.lower()
        for cor, status in COR_STATUS.items():
            if cor in href_lower:
                return status

    return "indefinido"

def _inferir_status_por_descricao(campos: dict) -> str:
    """
    Infere o status a partir dos campos extraídos da descrição.
    Prioridade:
      1. Campo "obra" (mais específico e confiável)
      2. Campos genéricos: 'status', 'situação'
    """
    if not campos:
        return "indefinido"
    
    # Prioridade 1: Campo "obra" (exato)
    valor_obra = campos.get("obra", "").lower()
    if valor_obra:
        if any(t in valor_obra for t in ("conclu", "finaliz", "entreg")):
            return "concluída"
        if any(t in valor_obra for t in ("andamento", "execu")):
            return "em andamento"
        if any(t in valor_obra for t in ("paralis", "suspens")):
            return "paralisada"
        if any(t in valor_obra for t in ("planeja", "projeto", "previs")):
            return "planejada"
    
    # Fallback: Outros campos genéricos
    status_textos = []
    for chave in ("status", "situação"):
        for k, v in campos.items():
            if chave in k.lower():
                status_textos.append(v.lower())
    if not status_textos:
        return "indefinido"
    texto = " ".join(status_textos)
    if any(t in texto for t in ("conclu", "finaliz", "entreg")):
        return "concluída"
    if any(t in texto for t in ("andamento", "execu", "obra")):
        return "em andamento"
    if any(t in texto for t in ("paralis", "suspens")):
        return "paralisada"
    if any(t in texto for t in ("planeja", "projeto", "previs")):
        return "planejada"
    return "indefinido"


def _inferir_status_por_camada(nome_camada: str) -> str:
    """Fallback de status com base no nome da camada do My Maps."""
    if not nome_camada:
        return "indefinido"

    camada = nome_camada.lower()
    if any(token in camada for token in ("conclu", "finaliz", "entreg")):
        return "concluída"
    if any(token in camada for token in ("andamento", "execu", "obra")):
        return "em andamento"
    if any(token in camada for token in ("paralis", "suspens")):
        return "paralisada"
    if any(token in camada for token in ("planeja", "projeto", "previs")):
        return "planejada"

    return "indefinido"


def _parsear_coordenadas(texto: Optional[str]) -> tuple[Optional[float], Optional[float]]:
    """
    Extrai latitude e longitude de uma string de coordenadas KML.
    Formato KML: "longitude,latitude,altitude"
    """
    if not texto:
        return None, None
    texto = texto.strip()
    partes = texto.split(",")
    try:
        lon = float(partes[0])
        lat = float(partes[1])
        return lat, lon
    except (IndexError, ValueError):
        return None, None


def _limpar_html(texto: Optional[str]) -> Optional[str]:
    """Remove tags HTML da descrição do placemark."""
    if not texto:
        return None
    return BeautifulSoup(texto, "lxml").get_text(separator=" ", strip=True)


def _extrair_campos_descricao(descricao: str) -> dict:
    """
    Extrai campos estruturados da descrição HTML do placemark.
    O Google My Maps armazena atributos personalizados como tabela HTML
    ou como pares chave: valor na descrição.

    Retorna dict com campos encontrados.
    """
    campos = {}
    if not descricao:
        return campos

    # Tentar extrair via tabela HTML
    soup = BeautifulSoup(descricao, "lxml")
    tabela = soup.find("table")
    if tabela:
        for linha in tabela.find_all("tr"):
            colunas = linha.find_all(["td", "th"])
            if len(colunas) >= 2:
                chave = colunas[0].get_text(strip=True).lower()
                valor = colunas[1].get_text(strip=True)
                if chave and valor:
                    campos[chave] = valor

    # Tentar padrão "Chave: Valor" em linhas de texto
    if not campos:
        texto_limpo = soup.get_text(separator="\n", strip=True)
        if texto_limpo:
            for linha in texto_limpo.split("\n"):
                if ":" in linha:
                    partes = linha.split(":", 1)
                    chave = partes[0].strip().lower()
                    valor = partes[1].strip()
                    if chave and valor:
                        campos[chave] = valor

    return campos


def parsear_kml(kml_bytes: bytes) -> list[dict]:
    """
    Parseia o KML do Google My Maps e extrai todos os placemarks.

    Cada placemark representa uma obra ou projeto monitorado pelo EGIM.
    Retorna lista de dicts com os dados brutos de cada placemark.
    """
    log.info("Parseando KML...")

    try:
        root = ET.fromstring(kml_bytes)
    except ET.ParseError as e:
        # Tentar remover namespace problemático e retentar
        kml_str = kml_bytes.decode("utf-8", errors="replace")
        kml_str = re.sub(r' xmlns[^"]*"[^"]*"', "", kml_str)
        root = ET.fromstring(kml_str.encode("utf-8"))

    placemarks = []

    # Iterar sobre todas as camadas (Folders) e placemarks
    for folder in root.iter():
        if folder.tag.endswith("Folder") or folder.tag.endswith("Document"):
            # Nome da camada (layer)
            nome_camada = _tag(folder, "name") or "sem_camada"

            for placemark in folder:
                if not placemark.tag.endswith("Placemark"):
                    continue

                nome = _tag(placemark, "name") or ""
                descricao_raw = _tag(placemark, "description") or ""
                coords_texto = _tag(
                    placemark,
                    "Point/coordinates",
                    "MultiGeometry/Point/coordinates",
                )

                lat, lon = _parsear_coordenadas(coords_texto)

                campos_extras = _extrair_campos_descricao(descricao_raw)
                # Nova lógica: prioridade status por descrição > ícone > camada
                status = _inferir_status_por_descricao(campos_extras)
                if status == "indefinido":
                    status = _inferir_status_por_icone(placemark)
                if status == "indefinido":
                    status = _inferir_status_por_camada(nome_camada)

                placemarks.append({
                    "nome":           nome,
                    "camada":         nome_camada,
                    "descricao_raw":  descricao_raw,
                    "descricao":      _limpar_html(descricao_raw),
                    "latitude":       lat,
                    "longitude":      lon,
                    "status_icone":   status,
                    "campos_extras":  campos_extras,
                    "coords_kml":     coords_texto,
                })

    log.info(f"Placemarks extraídos: {len(placemarks)}")
    return placemarks


# ── Normalização ──────────────────────────────────────────────────────────────

def _campo(campos: dict, *chaves: str) -> Optional[str]:
    """Busca um valor em campos_extras por múltiplas chaves alternativas."""
    for chave in chaves:
        for k, v in campos.items():
            if chave.lower() in k.lower():
                return v
    return None


def normalizar(placemarks: list[dict]) -> pd.DataFrame:
    """
    Transforma os placemarks brutos do KML em DataFrame normalizado,
    pronto para o ETL e para o PostGIS.
    """
    if not placemarks:
        log.warning("Nenhum placemark para normalizar.")
        return pd.DataFrame()

    rows = []
    for p in placemarks:
        extras = p.get("campos_extras", {})
        rows.append({
            # Identificação
            "nome_obra":        p.get("nome"),
            "camada_mapa":      p.get("camada"),
            "fonte":            "egim_google_mymaps",
            "map_id":           MAP_ID,

            # Localização (pronto para PostGIS)
            "latitude":         p.get("latitude"),
            "longitude":        p.get("longitude"),

            # Status inferido pela cor do ícone
            "status":           p.get("status_icone"),

            # Campos extraídos da descrição (variam por obra)
            "secretaria":       _campo(extras, "secretaria", "órgão", "responsavel"),
            "valor":            _campo(extras, "valor", "investimento", "custo"),
            "previsao_termino": _campo(extras, "previsão", "termino", "conclusão", "prazo"),
            "percentual":       _campo(extras, "percentual", "execução", "%"),
            "programa":         _campo(extras, "programa", "fonte", "recurso"),
            "bairro":           _campo(extras, "bairro", "localidade", "região"),
            "endereco":         _campo(extras, "endereço", "logradouro", "local"),

            # Descrição completa
            "descricao":        p.get("descricao"),

            # Auditoria
            "coletado_em":      datetime.now(timezone.utc).isoformat(),
            "payload_bruto":    json.dumps(p, ensure_ascii=False),
        })

    df = pd.DataFrame(rows)

    # Remover placemarks sem coordenadas — sem coordenadas não servem para o PostGIS
    sem_coords = df["latitude"].isna() | df["longitude"].isna()
    if sem_coords.any():
        log.warning(f"{sem_coords.sum()} placemarks sem coordenadas — descartados")
        df = df[~sem_coords].copy()

    log.info(f"Normalização concluída: {len(df)} obras georreferenciadas")
    return df


# ── Cache ─────────────────────────────────────────────────────────────────────

def _salvar_cache(kml_bytes: bytes, df: pd.DataFrame) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # KML bruto
    path_kml = CACHE_DIR / "egim.kml"
    path_kml.write_bytes(kml_bytes)
    # DataFrame normalizado em JSON
    path_json = CACHE_DIR / "egim.json"
    df.to_json(path_json, orient="records", force_ascii=False, indent=2)
    log.info(f"Cache salvo: {path_kml} e {path_json}")


def _carregar_cache() -> Optional[pd.DataFrame]:
    path_json = CACHE_DIR / "egim.json"
    if not path_json.exists():
        log.warning("Cache EGIM não encontrado.")
        return None
    df = pd.read_json(path_json, orient="records")
    log.warning(f"Usando cache local EGIM ({len(df)} registros)")
    return df


# ── Pipeline principal ────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
    """
    Executa o pipeline completo de coleta do EGIM:

        1. Baixa KML do Google My Maps público do EGIM
        2. Parseia camadas e placemarks
        3. Normaliza em DataFrame com coordenadas prontas para PostGIS

    Em caso de falha, serve o cache da última execução bem-sucedida.

    Retorna DataFrame com colunas:
        nome_obra, camada_mapa, latitude, longitude, status,
        secretaria, valor, previsao_termino, percentual,
        programa, bairro, endereco, descricao, coletado_em, payload_bruto
    """
    log.info("=" * 55)
    log.info("EGIM — Obras Georreferenciadas da Prefeitura de Macaé")
    log.info(f"Mapa: https://www.google.com/maps/d/viewer?mid={MAP_ID}")
    log.info("=" * 55)

    try:
        # Etapa 1 — baixar KML
        kml_bytes = download_kml()

        # Etapa 2 — parsear
        placemarks = parsear_kml(kml_bytes)
        if not placemarks:
            raise RuntimeError("KML parseado sem placemarks.")

        # Etapa 3 — normalizar
        df = normalizar(placemarks)

        # Salvar cache após sucesso
        _salvar_cache(kml_bytes, df)

    except Exception as e:
        log.error(f"Falha na coleta EGIM: {e}. Tentando cache local...")
        df = _carregar_cache()
        if df is None or df.empty:
            log.error("Cache vazio. Retornando DataFrame vazio.")
            return pd.DataFrame()

    log.info("=" * 55)
    log.info(f"EGIM finalizado: {len(df)} obras georreferenciadas")
    if not df.empty:
        for status, qtd in df["status"].value_counts().items():
            log.info(f"  {status}: {qtd} obras")
    log.info("=" * 55)

    return df


if __name__ == "__main__":
    df = run()

    if df.empty:
        print("\nNenhuma obra coletada do EGIM.")
    else:
        print(f"\n── EGIM · {len(df)} obras georreferenciadas ──\n")
        print(df[[
            "nome_obra", "camada_mapa", "status",
            "latitude", "longitude", "secretaria",
        ]].to_string(index=False))