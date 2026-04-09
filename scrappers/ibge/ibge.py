"""
scrapers/ibge/ibge.py
Scraper do IBGE — duopen-coleta · DUOPEN 2026

Coleta dados geográficos e socioeconômicos de Macaé (RJ) via APIs públicas do IBGE.

APIs utilizadas (todas públicas, sem autenticação):

    1. Localidades
       GET /api/v1/localidades/municipios/{id}
       Metadados do município: nome, UF, mesorregião, microrregião, região imediata

    2. Malhas geográficas
       GET /api/v3/malhas/municipios/{id}?formato=application/vnd.geo+json
       GeoJSON do perímetro de Macaé — pronto para gravar no PostGIS

    3. SIDRA — Agregados (tabelas de dados estatísticos)
       GET /api/v3/agregados/{agregado}/periodos/{periodo}/variaveis/{variavel}
            ?localidades=N6[{ibge_id}]&view=flat

       Tabelas utilizadas:
         9514  — População residente (Censo 2022)          variável 93
         6579  — Estimativa populacional (mais recente)    variável 9324
         4714  — Área territorial e densidade demográfica  variáveis 614,616
         5938  — PIB per capita municipal                  variável 37
         7735  — IDHM                                      variável 30255

Variáveis de ambiente (.env):
    IBGE_MUNICIPIO_CODE   código IBGE de Macaé — 7 dígitos (padrão: 3302403)
    LOG_LEVEL             nível de log (padrão: INFO)
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Configuração ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper.ibge")

MUNICIPIO_ID     = os.getenv("IBGE_MUNICIPIO_CODE", "3302403")  # Macaé
BASE_LOCALIDADES = "https://servicodados.ibge.gov.br/api/v1/localidades"
BASE_MALHAS      = "https://servicodados.ibge.gov.br/api/v3/malhas"
BASE_AGREGADOS   = "https://servicodados.ibge.gov.br/api/v3/agregados"
REQUEST_TIMEOUT  = 30
RETRY_ATTEMPTS   = 3
RETRY_BACKOFF    = 2.0
CACHE_DIR        = Path(__file__).parent.parent.parent / "cache"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "duopen-coleta/1.0 (hackathon DUOPEN 2026)",
}

# Tabelas SIDRA com suas variáveis e descrições
# Formato: { agregado_id: { "descricao": str, "variaveis": [int], "periodo": str } }
TABELAS_SIDRA = {
    "9514": {
        "descricao":  "População residente — Censo 2022",
        "variaveis":  [93],
        "periodo":    "2022",
    },
    "6579": {
        "descricao":  "Estimativa populacional (mais recente)",
        "variaveis":  [9324],
        "periodo":    "last",
    },
    "4714": {
        "descricao":  "Área territorial e densidade demográfica",
        "variaveis":  [614, 616],
        "periodo":    "last",
    },
    "5938": {
        "descricao":  "PIB per capita municipal",
        "variaveis":  [37],
        "periodo":    "last",
    },
    "7735": {
        "descricao":  "IDHM",
        "variaveis":  [30255],
        "periodo":    "last",
    },
}


# ── Cliente HTTP ──────────────────────────────────────────────────────────────

def _get(url: str, params: dict = None, accept: str = "application/json") -> requests.Response:
    """
    GET com retry e backoff exponencial.
    Lança RuntimeError após esgotar as tentativas.
    """
    headers = {**HEADERS, "Accept": accept}
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(
                url,
                headers=headers,
                params=params or {},
                timeout=REQUEST_TIMEOUT,
            )
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


# ── 1. Localidades ────────────────────────────────────────────────────────────

def fetch_localidade() -> dict:
    """
    Busca metadados do município de Macaé via API de Localidades.

    Retorna dict com: nome, UF, mesorregião, microrregião, região imediata,
    região intermediária e região geográfica.
    """
    log.info(f"Buscando metadados do município {MUNICIPIO_ID}...")
    url = f"{BASE_LOCALIDADES}/municipios/{MUNICIPIO_ID}"
    resp = _get(url)
    data = resp.json()

    resultado = {
        "municipio_id":           data.get("id"),
        "municipio_nome":         data.get("nome"),
        "uf_id":                  data.get("microrregiao", {}).get("mesorregiao", {}).get("UF", {}).get("id"),
        "uf_sigla":               data.get("microrregiao", {}).get("mesorregiao", {}).get("UF", {}).get("sigla"),
        "uf_nome":                data.get("microrregiao", {}).get("mesorregiao", {}).get("UF", {}).get("nome"),
        "mesorregiao_id":         data.get("microrregiao", {}).get("mesorregiao", {}).get("id"),
        "mesorregiao_nome":       data.get("microrregiao", {}).get("mesorregiao", {}).get("nome"),
        "microrregiao_id":        data.get("microrregiao", {}).get("id"),
        "microrregiao_nome":      data.get("microrregiao", {}).get("nome"),
        "regiao_imediata_id":     data.get("regiao-imediata", {}).get("id"),
        "regiao_imediata_nome":   data.get("regiao-imediata", {}).get("nome"),
        "regiao_intermediaria_id":   data.get("regiao-imediata", {}).get("regiao-intermediaria", {}).get("id"),
        "regiao_intermediaria_nome": data.get("regiao-imediata", {}).get("regiao-intermediaria", {}).get("nome"),
        "payload_bruto":          json.dumps(data, ensure_ascii=False),
    }

    log.info(f"Localidade obtida: {resultado['municipio_nome']} / {resultado['uf_sigla']}")
    return resultado


# ── 2. GeoJSON do perímetro ───────────────────────────────────────────────────

def fetch_geojson() -> dict:
    """
    Busca o GeoJSON do perímetro de Macaé via API de Malhas Geográficas.
    Retorna o GeoJSON completo pronto para gravar no PostGIS (geography POLYGON).

    Endpoint:
        GET /api/v3/malhas/municipios/{id}?formato=application/vnd.geo+json
    """
    log.info(f"Buscando GeoJSON do perímetro de Macaé (IBGE {MUNICIPIO_ID})...")
    url = f"{BASE_MALHAS}/municipios/{MUNICIPIO_ID}"
    resp = _get(url, params={"formato": "application/vnd.geo+json"})
    geojson = resp.json()

    # Extrair coordenadas da primeira feature para log
    try:
        coords = geojson["features"][0]["geometry"]["coordinates"]
        n_pontos = sum(len(anel) for poligono in coords for anel in poligono) \
            if geojson["features"][0]["geometry"]["type"] == "MultiPolygon" \
            else sum(len(anel) for anel in coords)
        log.info(f"GeoJSON obtido: {n_pontos} pontos no perímetro")
    except (KeyError, IndexError, TypeError):
        log.info("GeoJSON obtido (estrutura não padrão)")

    return geojson


# ── 3. SIDRA — dados estatísticos ────────────────────────────────────────────

def _extrair_valor_sidra(data: list, variavel_id: int) -> Optional[str]:
    """
    Extrai o valor de uma variável específica da resposta flat do SIDRA.
    Retorna string com o valor ou None se não encontrado.
    """
    for item in data:
        if str(item.get("D3C")) == str(variavel_id) or \
           str(item.get("VC")) == str(variavel_id):
            return item.get("V")
    # fallback: primeira linha com valor
    if data:
        return data[0].get("V")
    return None


def fetch_sidra(agregado_id: str, variaveis: list[int], periodo: str) -> dict:
    """
    Consulta uma tabela SIDRA para o município de Macaé.

    Parâmetros:
        agregado_id  — código da tabela SIDRA (ex: "9514")
        variaveis    — lista de IDs de variáveis a consultar
        periodo      — período de referência (ex: "2022", "last")

    Retorna dict { variavel_id: valor_str }.
    """
    variaveis_str = "|".join(str(v) for v in variaveis)
    url = (
        f"{BASE_AGREGADOS}/{agregado_id}"
        f"/periodos/{periodo}"
        f"/variaveis/{variaveis_str}"
    )
    params = {
        "localidades": f"N6[{MUNICIPIO_ID}]",
        "view":        "flat",
    }

    log.info(f"SIDRA tabela {agregado_id} — variáveis {variaveis_str} — período {periodo}")

    try:
        resp = _get(url, params=params)
        data = resp.json()

        resultado = {}
        for variavel_id in variaveis:
            valor = _extrair_valor_sidra(data, variavel_id)
            resultado[variavel_id] = valor
            log.debug(f"  variável {variavel_id} = {valor}")

        return resultado

    except Exception as e:
        log.error(f"Falha ao consultar SIDRA tabela {agregado_id}: {e}")
        return {v: None for v in variaveis}


def fetch_todos_sidra() -> dict:
    """
    Consulta todas as tabelas SIDRA definidas em TABELAS_SIDRA.
    Retorna dict consolidado com todos os indicadores de Macaé.
    """
    log.info("Iniciando coleta SIDRA...")
    consolidado = {}

    for agregado_id, config in TABELAS_SIDRA.items():
        log.info(f"Tabela {agregado_id}: {config['descricao']}")
        valores = fetch_sidra(
            agregado_id=agregado_id,
            variaveis=config["variaveis"],
            periodo=config["periodo"],
        )
        consolidado[agregado_id] = {
            "descricao": config["descricao"],
            "valores":   valores,
        }
        time.sleep(0.3)  # delay entre chamadas

    return consolidado


# ── Normalização ──────────────────────────────────────────────────────────────

def _to_float(val) -> Optional[float]:
    if val is None or val == "...":
        return None
    try:
        return float(str(val).replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


def _to_int(val) -> Optional[int]:
    f = _to_float(val)
    return int(f) if f is not None else None


def normalizar(
    localidade: dict,
    geojson: dict,
    sidra: dict,
) -> dict:
    """
    Consolida localidade + GeoJSON + SIDRA em um único dict normalizado,
    pronto para gravar nas tabelas do Supabase.

    Retorna:
        {
          "metadados":   dict com dados do município
          "geojson":     dict GeoJSON do perímetro (para PostGIS)
          "indicadores": DataFrame com série histórica de indicadores
        }
    """
    # Extrair valores SIDRA por tabela/variável
    pop_censo_2022   = _to_int(sidra.get("9514", {}).get("valores", {}).get(93))
    pop_estimada     = _to_int(sidra.get("6579", {}).get("valores", {}).get(9324))
    area_km2         = _to_float(sidra.get("4714", {}).get("valores", {}).get(614))
    densidade_hab_km2 = _to_float(sidra.get("4714", {}).get("valores", {}).get(616))
    pib_per_capita   = _to_float(sidra.get("5938", {}).get("valores", {}).get(37))
    idhm             = _to_float(sidra.get("7735", {}).get("valores", {}).get(30255))

    metadados = {
        # Identificação
        "municipio_id":              localidade.get("municipio_id"),
        "municipio_nome":            localidade.get("municipio_nome"),
        "uf_sigla":                  localidade.get("uf_sigla"),
        "uf_nome":                   localidade.get("uf_nome"),
        "mesorregiao_nome":          localidade.get("mesorregiao_nome"),
        "microrregiao_nome":         localidade.get("microrregiao_nome"),
        "regiao_imediata_nome":      localidade.get("regiao_imediata_nome"),
        "regiao_intermediaria_nome": localidade.get("regiao_intermediaria_nome"),

        # Indicadores populacionais
        "populacao_censo_2022":      pop_censo_2022,
        "populacao_estimada":        pop_estimada,

        # Indicadores geográficos
        "area_territorial_km2":      area_km2,
        "densidade_demografica":     densidade_hab_km2,

        # Indicadores econômicos e sociais
        "pib_per_capita":            pib_per_capita,
        "idhm":                      idhm,

        # Contexto para ML (features de enriquecimento das obras)
        "fonte":                     "ibge",
        "coletado_em":               datetime.now(timezone.utc).isoformat(),
        "payload_sidra_bruto":       json.dumps(sidra, ensure_ascii=False),
        "payload_localidade_bruto":  localidade.get("payload_bruto"),
    }

    log.info(
        f"Normalização concluída:\n"
        f"  Município:    {metadados['municipio_nome']} / {metadados['uf_sigla']}\n"
        f"  População:    {metadados['populacao_censo_2022']:,} hab (Censo 2022)\n"
        f"  Área:         {metadados['area_territorial_km2']} km²\n"
        f"  Densidade:    {metadados['densidade_demografica']} hab/km²\n"
        f"  PIB per cap:  R$ {metadados['pib_per_capita']:,.2f}\n"
        f"  IDHM:         {metadados['idhm']}"
        if all([
            metadados["populacao_censo_2022"],
            metadados["area_territorial_km2"],
            metadados["pib_per_capita"],
            metadados["idhm"],
        ]) else "Normalização concluída (alguns indicadores indisponíveis)"
    )

    return {
        "metadados": metadados,
        "geojson":   geojson,
    }


# ── Cache ─────────────────────────────────────────────────────────────────────

def _salvar_cache(resultado: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # Salvar metadados
    path_meta = CACHE_DIR / "ibge_metadados.json"
    with open(path_meta, "w", encoding="utf-8") as f:
        json.dump(resultado["metadados"], f, ensure_ascii=False, indent=2)
    # Salvar GeoJSON separado
    path_geo = CACHE_DIR / "ibge_macae.geojson"
    with open(path_geo, "w", encoding="utf-8") as f:
        json.dump(resultado["geojson"], f, ensure_ascii=False)
    log.info(f"Cache salvo: {path_meta} e {path_geo}")


def _carregar_cache() -> Optional[dict]:
    path_meta = CACHE_DIR / "ibge_metadados.json"
    path_geo  = CACHE_DIR / "ibge_macae.geojson"
    if not path_meta.exists() or not path_geo.exists():
        log.warning("Cache IBGE não encontrado.")
        return None
    with open(path_meta, encoding="utf-8") as f:
        metadados = json.load(f)
    with open(path_geo, encoding="utf-8") as f:
        geojson = json.load(f)
    log.warning(f"Usando cache local IBGE ({path_meta.name} + {path_geo.name})")
    return {"metadados": metadados, "geojson": geojson}


# ── Pipeline principal ────────────────────────────────────────────────────────

def run() -> dict:
    """
    Executa o pipeline completo de coleta do IBGE:

        1. Metadados do município (API de Localidades)
        2. GeoJSON do perímetro (API de Malhas)
        3. Indicadores socioeconômicos (SIDRA)
        4. Normalização e consolidação

    Retorna dict com:
        "metadados"  — dict com todos os indicadores normalizados
        "geojson"    — GeoJSON do perímetro (para PostGIS)

    Em caso de falha, serve o cache da última execução bem-sucedida.
    """
    log.info("=" * 55)
    log.info("IBGE — início da coleta")
    log.info(f"Município: Macaé (IBGE {MUNICIPIO_ID})")
    log.info("=" * 55)

    try:
        # Etapa 1 — metadados do município
        localidade = fetch_localidade()

        # Etapa 2 — GeoJSON do perímetro
        geojson = fetch_geojson()

        # Etapa 3 — indicadores SIDRA
        sidra = fetch_todos_sidra()

        # Etapa 4 — normalizar
        resultado = normalizar(localidade, geojson, sidra)

        # Salvar cache após sucesso
        _salvar_cache(resultado)

    except Exception as e:
        log.error(f"Falha na coleta IBGE: {e}. Tentando cache local...")
        resultado = _carregar_cache()
        if not resultado:
            log.error("Cache vazio. Retornando dict vazio.")
            return {}

    log.info("=" * 55)
    log.info("Coleta IBGE finalizada com sucesso.")
    log.info("=" * 55)
    return resultado


if __name__ == "__main__":
    resultado = run()

    if not resultado:
        print("\nNenhum dado coletado do IBGE.")
    else:
        meta = resultado["metadados"]
        geo  = resultado["geojson"]

        print(f"\n── IBGE · {meta.get('municipio_nome')} / {meta.get('uf_sigla')} ──\n")
        print(f"  Mesorregião:      {meta.get('mesorregiao_nome')}")
        print(f"  Microrregião:     {meta.get('microrregiao_nome')}")
        print(f"  Região imediata:  {meta.get('regiao_imediata_nome')}")
        print(f"  População 2022:   {meta.get('populacao_censo_2022'):,} hab")
        print(f"  Pop. estimada:    {meta.get('populacao_estimada'):,} hab")
        print(f"  Área territorial: {meta.get('area_territorial_km2')} km²")
        print(f"  Densidade:        {meta.get('densidade_demografica')} hab/km²")
        print(f"  PIB per capita:   R$ {meta.get('pib_per_capita'):,.2f}")
        print(f"  IDHM:             {meta.get('idhm')}")
        print(f"\n  GeoJSON: {geo.get('type')} com "
              f"{len(geo.get('features', []))} feature(s) — pronto para PostGIS")