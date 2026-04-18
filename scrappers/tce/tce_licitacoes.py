"""
scrapers/tce/tce_licitantes.py
Scraper do Portal de Dados Abertos do TCE-RJ — Licitantes e Fornecedores
duopen-coleta · DUOPEN 2026

Coleta dados de licitações e contratos do município de Macaé via API pública
do Portal de Dados Abertos do TCE-RJ (dados.tcerj.tc.br).

─────────────────────────────────────────────────────────────
CONTEXTO
─────────────────────────────────────────────────────────────
O TCE-RJ recebe dados de todos os 91 municípios fluminenses via sistema
SIGFIS (Sistema Integrado de Gestão Fiscal), conforme Deliberação nº 281/2017.

Portal:      https://dados.tcerj.tc.br
Docs:        https://dados.tcerj.tc.br/api/v1/docs
Base URL:    https://dados.tcerj.tc.br/api/v1

Endpoints utilizados:
    GET /licitacoes          → licitações do portal TCE-RJ
    GET /contratos           → contratos por município e ano
    GET /compras_diretas     → dispensas e inexigibilidades
    GET /obras_paralisadas   → obras paralisadas (bônus — útil para ML)

Coleta de licitações:
    Endpoint base:  /licitacoes?inicio=0&limite=1000&csv=false&jsonfull=false
    Filtro local:   registros com Ente = MACAE (comparação sem acento)

Parâmetros comuns (SIGFIS):
    municipio   nome do município (ex: "Macaé")
    ano         exercício fiscal (ex: 2023)
    limit       tamanho da página
    offset      paginação por offset

─────────────────────────────────────────────────────────────
VALOR PARA O PROJETO
─────────────────────────────────────────────────────────────
Os dados de licitantes/fornecedores do TCE-RJ são a principal fonte para:
  - Feature engineering do ML: recorrência de fornecedor, histórico
    de atrasos, percentual de obras com aditivos por CNPJ
  - Detecção de concentração: fornecedores que ganham muitas licitações
  - Cruzamento com SISMOB e portal_macae para validar consistência

Variáveis de ambiente (.env):
    TCE_RJ_MUNICIPIO    nome do município (padrão: Macaé)
    TCE_RJ_ANO_INICIO   primeiro ano a coletar (padrão: 2017)
    LOG_LEVEL           nível de log (padrão: INFO)
"""

import os
import json
import time
import logging
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def _optional_int(name: str) -> Optional[int]:
    """Lê um inteiro opcional do ambiente e retorna None quando ausente."""
    value = os.getenv(name, "").strip()
    return int(value) if value else None

# ── Configuração ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper.tce_licitantes")

BASE_URL      = "https://dados.tcerj.tc.br/api/v1"
MUNICIPIO     = os.getenv("TCE_RJ_MUNICIPIO", "MACAE").strip()
ANO_INICIO    = _optional_int("TCE_RJ_ANO_INICIO")
ANO_FIM       = _optional_int("TCE_RJ_ANO_FIM") or datetime.now().year
PAGE_LIMIT    = 100         # registros por página
LICITACOES_PAGE_LIMIT = 1000
DELAY_PAGINAS = 0.3         # segundos entre páginas
DELAY_ANOS    = 0.5         # segundos entre anos
RETRY_ATTEMPTS = 3
RETRY_BACKOFF  = 2.0
REQUEST_TIMEOUT = 30
CACHE_DIR     = Path(__file__).parent.parent.parent / "cache"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "duopen-coleta/1.0 (hackathon DUOPEN 2026)",
}


def _normalize_text(value: Optional[str]) -> str:
    """Normaliza texto para comparacoes case-insensitive e sem acento."""
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return without_accents.strip().lower()


def _municipio_match(value: Optional[str]) -> bool:
    """Retorna True quando o registro pertence a Macaé."""
    return _normalize_text(value) == _normalize_text(MUNICIPIO)


def _anos_consulta(
    ano_inicio: Optional[int] = None,
    ano_fim: Optional[int] = None,
) -> list[int]:
    """Retorna os anos a consultar ou lista vazia quando o filtro está desativado."""
    inicio = ANO_INICIO if ano_inicio is None else ano_inicio
    fim = ANO_FIM if ano_fim is None else ano_fim

    if inicio is None or fim is None or inicio <= 0 or fim <= 0 or fim < inicio:
        return []
    return list(range(inicio, fim + 1))

# Endpoints disponíveis na API do TCE-RJ
ENDPOINTS = {
    "licitacoes":       "/licitacoes",
    "contratos":        "/contratos",
    "compras_diretas":  "/compras_diretas",
    "obras_paralisadas": "/obras_paralisadas",
}

CACHE_FILES = {
    "licitacoes": "tce_licitacoes.json",
    "contratos": "tce_contratos.json",
    "compras_diretas": "tce_compras_diretas.json",
    "obras_paralisadas": "tce_obras_paralisadas.json",
    "perfil_fornecedores": "tce_perfil_fornecedores.json",
}


# ── Cliente HTTP ──────────────────────────────────────────────────────────────

def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    """
    GET com retry e backoff exponencial.
    Lança RuntimeError após esgotar as tentativas.
    """
    url = f"{BASE_URL}{path}"
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        resp = None
        try:
            resp = requests.get(
                url,
                headers=HEADERS,
                params=params or {},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.HTTPError as e:
            status = resp.status_code if resp is not None else "?"
            # 404 pode significar que não há dados para aquele ano/município
            if status == 404:
                log.debug(f"404 em {url} — sem dados para esses parâmetros")
                return {}
            log.error(f"HTTP {status} em {url}: {e}")
            raise

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            log.warning(f"Tentativa {attempt}/{RETRY_ATTEMPTS} falhou: {e}")
            if attempt == RETRY_ATTEMPTS:
                raise RuntimeError(
                    f"Falha após {RETRY_ATTEMPTS} tentativas: {url}"
                ) from e
            wait = RETRY_BACKOFF * (2 ** (attempt - 1))
            log.info(f"Aguardando {wait}s antes da próxima tentativa...")
            time.sleep(wait)


# ── Paginação genérica ────────────────────────────────────────────────────────

def _paginar(endpoint: str, params_base: dict) -> list[dict]:
    """
    Pagina um endpoint do TCE-RJ via parâmetros limit/offset.
    Para automaticamente quando a página retorna vazia ou menos
    registros que o limite (última página).

    Retorna lista completa de registros do endpoint.
    """
    registros = []
    offset = 0

    while True:
        params = {**params_base, "limit": PAGE_LIMIT, "offset": offset}
        log.debug(f"GET {endpoint} offset={offset}...")

        try:
            data = _get(endpoint, params)
        except RuntimeError as e:
            log.error(f"Abandonando paginação em {endpoint}: {e}")
            break

        # A API pode retornar lista direta ou dict com chave de dados
        if isinstance(data, list):
            itens = data
        elif isinstance(data, dict):
            # Tenta chaves comuns de paginação
            itens = (
                data.get("data") or
                data.get("results") or
                data.get("content") or
                data.get("items") or
                []
            )
        else:
            itens = []

        if not itens:
            break

        registros.extend(itens)
        log.debug(f"  +{len(itens)} registros (total: {len(registros)})")

        # Última página: menos registros que o limite
        if len(itens) < PAGE_LIMIT:
            break

        offset += PAGE_LIMIT
        time.sleep(DELAY_PAGINAS)

    return registros


def _paginar_licitacoes(params_base: dict = None) -> list[dict]:
    """
    Pagina o endpoint de licitações usando inicio/limite.

    A API retorna o payload dentro da chave ``Licitacoes``.
    """
    registros = []
    inicio = 0

    while True:
        params = {
            **(params_base or {}),
            "inicio": inicio,
            "limite": LICITACOES_PAGE_LIMIT,
            "csv": "false",
            "jsonfull": "false",
        }
        log.debug(f"GET /licitacoes inicio={inicio}...")

        try:
            data = _get("/licitacoes", params)
        except RuntimeError as e:
            log.error(f"Abandonando paginação de licitações: {e}")
            break

        if isinstance(data, list):
            itens = [item for item in data if isinstance(item, dict)]
        elif isinstance(data, dict):
            itens_brutos = (
                data.get("Licitacoes") or
                data.get("data") or
                data.get("results") or
                data.get("content") or
                data.get("items") or
                []
            )
            itens = [item for item in itens_brutos if isinstance(item, dict)]
        else:
            itens = []

        if not itens:
            break

        registros.extend(itens)
        log.debug(f"  +{len(itens)} registros (total: {len(registros)})")

        if len(itens) < LICITACOES_PAGE_LIMIT:
            break

        inicio += LICITACOES_PAGE_LIMIT
        time.sleep(DELAY_PAGINAS)

    return registros


# ── Coleta por endpoint e ano ─────────────────────────────────────────────────

def _coletar_endpoint_por_anos(
    endpoint_key: str,
    params_extras: dict[str, Any] | None = None,
) -> list[dict]:
    """
    Coleta todos os registros de um endpoint para o município de Macaé,
    iterando por cada ano desde ANO_INICIO até o ano atual.

    Estratégia por ano: a API do SIGFIS/TCE-RJ organiza dados por exercício
    fiscal. Coletar por ano evita timeouts em consultas muito grandes.
    """
    endpoint = ENDPOINTS[endpoint_key]
    todos = []
    anos = _anos_consulta()

    if not anos:
        log.info(f"[{endpoint_key}] filtro por ano desativado; consultando sem ano")
        params = {
            "municipio": MUNICIPIO,
            **(params_extras or {}),
        }
        registros = _paginar(endpoint, params)

        if registros:
            todos.extend(registros)
            log.info(f"  sem ano: {len(registros)} registros")
        else:
            log.debug("  sem dados")

        log.info(f"[{endpoint_key}] Total: {len(todos)} registros (sem filtro por ano)")
        return todos

    for ano in anos:
        params = {
            "municipio": MUNICIPIO,
            "ano":       ano,
            **(params_extras or {}),
        }

        log.info(f"[{endpoint_key}] ano={ano}...")

        registros_ano = _paginar(endpoint, params)

        if registros_ano:
            # Enriquecer cada registro com o ano de referência
            for r in registros_ano:
                r.setdefault("_ano_coleta", ano)
            todos.extend(registros_ano)
            log.info(f"  {ano}: {len(registros_ano)} registros")
        else:
            log.debug(f"  {ano}: sem dados")

        time.sleep(DELAY_ANOS)

    log.info(f"[{endpoint_key}] Total: {len(todos)} registros ({ANO_INICIO}–{ANO_FIM})")
    return todos


# ── Coleta de cada fonte ──────────────────────────────────────────────────────

def fetch_licitacoes() -> list[dict]:
    """
    Coleta licitações do TCE-RJ e filtra somente registros de Macaé.

    O endpoint público /licitacoes não recebe filtro direto por município,
    então a coleta pagina o resultado inteiro e filtra pelo campo Ente.
    """
    log.info("Coletando licitações via TCE-RJ para município=%s...", MUNICIPIO)
    registros = _paginar_licitacoes()
    filtrados = [
        registro for registro in registros
        if _municipio_match(registro.get("Ente") or registro.get("Municipio"))
    ]
    log.info("Licitações coletadas: %s", len(filtrados))
    return filtrados


def fetch_contratos() -> list[dict]:
    """
    Coleta contratos firmados por Macaé registrados no SIGFIS/TCE-RJ.
    Fonte principal para dados de fornecedores (CNPJ, razão social, valores).
    """
    log.info("Coletando contratos via TCE-RJ...")
    return _coletar_endpoint_por_anos("contratos")


def fetch_compras_diretas() -> list[dict]:
    """
    Coleta dispensas de licitação e inexigibilidades de Macaé.
    Inclui contratações que não passaram por licitação — relevante para
    detectar padrões de concentração em fornecedores específicos.
    """
    log.info("Coletando compras diretas via TCE-RJ...")
    return _coletar_endpoint_por_anos("compras_diretas")


def fetch_obras_paralisadas() -> list[dict]:
    """
    Coleta obras paralisadas de Macaé registradas no e-TCERJ.
    Feature valiosa para o ML: obras que paralisaram são preditoras
    de risco de estouro de prazo em obras similares.
    """
    log.info("Coletando obras paralisadas via TCE-RJ...")
    try:
        return _paginar(ENDPOINTS["obras_paralisadas"], {"municipio": MUNICIPIO})
    except Exception as e:
        log.warning(f"Obras paralisadas: {e}")
        return []


# ── Normalização ──────────────────────────────────────────────────────────────

def _str(val) -> Optional[str]:
    if val is None or str(val).strip() in ("", "nan", "None", "NaT"):
        return None
    return str(val).strip()


def _float(val) -> Optional[float]:
    if isinstance(val, (int, float)):
        return float(val)

    v = _str(val)
    if not v:
        return None

    v = v.replace("R$", "").replace(" ", "").strip()
    if "," in v and "." in v:
        if v.rfind(",") > v.rfind("."):
            v = v.replace(".", "").replace(",", ".")
        else:
            v = v.replace(",", "")
    elif "," in v:
        v = v.replace(",", ".")

    try:
        return float(v)
    except ValueError:
        return None


def _data(val) -> Optional[str]:
    v = _str(val)
    if not v:
        return None
    formatos = [
        "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y",
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
    ]
    for fmt in formatos:
        try:
            dt = datetime.strptime(v, fmt).replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue
    return v


def _get_field(r: dict, *chaves: str) -> Optional[str]:
    """Busca um campo no dict por múltiplas chaves alternativas."""
    for chave in chaves:
        # Busca exata
        if chave in r:
            return _str(r[chave])
        # Busca case-insensitive
        for k, v in r.items():
            if k.lower() == chave.lower():
                return _str(v)
    return None


def normalizar_licitacoes(registros: list[dict]) -> pd.DataFrame:
    """Normaliza registros brutos de licitações do TCE-RJ."""
    if not registros:
        return pd.DataFrame()

    rows = []
    for r in registros:
        rows.append({
            # Identificação
            "id_licitacao":      _get_field(r, "ProcessoLicitatorio", "processo_licitatorio", "NumeroEdital", "numero_edital"),
            "numero":            _get_field(r, "NumeroEdital", "numero_edital"),
            "processo_licitatorio": _get_field(r, "ProcessoLicitatorio", "processo_licitatorio"),
            "ano":               _get_field(r, "Ano", "ano", "exercicio", "_ano_coleta"),
            "mes":               _get_field(r, "Mes", "mes"),
            "municipio":         _get_field(r, "Ente", "municipio", "nomeMunicipio", "nome_municipio"),
            "unidade":           _get_field(r, "Unidade", "unidade"),
            "fonte":             "tce_rj_licitacoes",

            # Licitação
            "objeto":            _get_field(r, "Objeto", "objeto", "descricao", "objetoLicitacao"),
            "modalidade":        _get_field(r, "Modalidade", "modalidade", "tipoLicitacao", "tipo_licitacao"),
            "situacao":          _get_field(r, "Parecer", "situacao", "status", "situacaoLicitacao"),
            "tipo":              _get_field(r, "Tipo", "tipo", "tipoObjeto"),
            "publicacao_oficial": _get_field(r, "PublicacaoOficial", "publicacao_oficial"),
            "adiado_sine_die":    _get_field(r, "AdiadoSineDie", "adiado_sine_die"),
            "orcamento_sigiloso": _get_field(r, "OrcamentoSigiloso", "orcamento_sigiloso"),

            # Valores
            "valor_estimado":    _float(_get_field(r, "ValorEstimado", "valorEstimado", "valor_estimado", "valorTotal")),
            "valor_homologado":  _float(_get_field(r, "valorHomologado", "valor_homologado")),
            "percentual_recursos_uniao": _float(_get_field(r, "PercentualRecursosUniao", "percentual_recursos_uniao")),

            # Datas
            "data_publicacao_edital": _data(_get_field(r, "DataPublicacaoEdital", "dataPublicacaoEdital", "data_publicacao_edital")),
            "data_publicacao_oficial": _data(_get_field(r, "DataPublicacaoOficial", "dataPublicacaoOficial", "data_publicacao_oficial")),
            "data_homologacao":  _data(_get_field(r, "DataHomologacao", "dataHomologacao", "data_homologacao")),

            # Órgão
            "orgao":             _get_field(r, "Unidade", "orgao", "nomeOrgao", "unidadeGestora"),
            "secretaria":        _get_field(r, "secretaria", "nomeSecretaria"),

            # Licitante vencedor
            "cnpj_vencedor":     _get_field(r, "cnpjVencedor", "cnpj_vencedor", "cnpjContratado", "cnpj"),
            "nome_vencedor":     _get_field(r, "nomeVencedor", "nome_vencedor", "nomeContratado", "fornecedor"),

            # Auditoria
            "coletado_em":       datetime.now(timezone.utc).isoformat(),
            "payload_bruto":     json.dumps(r, ensure_ascii=False),
        })

    df = pd.DataFrame(rows)
    log.info(f"Licitações normalizadas: {len(df)} registros")
    return df


def normalizar_contratos(registros: list[dict]) -> pd.DataFrame:
    """Normaliza registros brutos de contratos do TCE-RJ."""
    if not registros:
        return pd.DataFrame()

    rows = []
    for r in registros:
        rows.append({
            # Identificação
            "id_contrato":       _get_field(r, "id", "codigo", "numeroContrato", "numero"),
            "ano":               _get_field(r, "ano", "exercicio", "_ano_coleta"),
            "municipio":         _get_field(r, "municipio", "nomeMunicipio"),
            "fonte":             "tce_rj_contratos",

            # Contrato
            "objeto":            _get_field(r, "objeto", "descricao", "objetoContrato"),
            "modalidade":        _get_field(r, "modalidade", "tipoLicitacao"),
            "situacao":          _get_field(r, "situacao", "status"),
            "num_licitacao":     _get_field(r, "numeroLicitacao", "num_licitacao", "licitacao"),

            # Fornecedor — chave para feature engineering do ML
            "cnpj_fornecedor":   _get_field(r, "cnpj", "cnpjFornecedor", "cnpjContratado"),
            "nome_fornecedor":   _get_field(r, "fornecedor", "nomeFornecedor", "nomeContratado", "razaoSocial"),

            # Valores
            "valor_contrato":    _float(_get_field(r, "valorContrato", "valor", "valorTotal")),
            "valor_aditivos":    _float(_get_field(r, "valorAditivos", "valor_aditivos")),
            "valor_final":       _float(_get_field(r, "valorFinal", "valor_final")),

            # Datas
            "data_assinatura":   _data(_get_field(r, "dataAssinatura", "data_assinatura", "dataContrato")),
            "data_inicio":       _data(_get_field(r, "dataInicio", "data_inicio", "dataVigenciaInicio")),
            "data_fim":          _data(_get_field(r, "dataFim", "data_fim", "dataVigenciaFim")),
            "data_rescisao":     _data(_get_field(r, "dataRescisao", "data_rescisao")),

            # Órgão
            "orgao":             _get_field(r, "orgao", "nomeOrgao", "unidadeGestora"),

            # Aditivos — feature importante para ML
            "qtd_aditivos":      _get_field(r, "quantidadeAditivos", "qtd_aditivos", "numAditivos"),
            "possui_aditivo":    _get_field(r, "possuiAditivo", "temAditivo"),

            # Auditoria
            "coletado_em":       datetime.now(timezone.utc).isoformat(),
            "payload_bruto":     json.dumps(r, ensure_ascii=False),
        })

    df = pd.DataFrame(rows)
    log.info(f"Contratos normalizados: {len(df)} registros")
    return df


def normalizar_compras_diretas(registros: list[dict]) -> pd.DataFrame:
    """Normaliza dispensas de licitação e inexigibilidades do TCE-RJ."""
    if not registros:
        return pd.DataFrame()

    rows = []
    for r in registros:
        rows.append({
            "id_compra":         _get_field(r, "id", "codigo", "numero"),
            "ano":               _get_field(r, "ano", "exercicio", "_ano_coleta"),
            "municipio":         _get_field(r, "municipio", "nomeMunicipio"),
            "fonte":             "tce_rj_compras_diretas",
            "tipo":              _get_field(r, "tipo", "tipoDispensa", "modalidade"),
            "objeto":            _get_field(r, "objeto", "descricao"),
            "cnpj_fornecedor":   _get_field(r, "cnpj", "cnpjFornecedor"),
            "nome_fornecedor":   _get_field(r, "fornecedor", "nomeFornecedor"),
            "valor":             _float(_get_field(r, "valor", "valorContrato")),
            "data_assinatura":   _data(_get_field(r, "dataAssinatura", "data")),
            "fundamento_legal":  _get_field(r, "fundamentoLegal", "fundamento", "inciso"),
            "coletado_em":       datetime.now(timezone.utc).isoformat(),
            "payload_bruto":     json.dumps(r, ensure_ascii=False),
        })

    df = pd.DataFrame(rows)
    log.info(f"Compras diretas normalizadas: {len(df)} registros")
    return df


def normalizar_obras_paralisadas(registros: list[dict]) -> pd.DataFrame:
    """Normaliza obras paralisadas registradas no e-TCERJ."""
    if not registros:
        return pd.DataFrame()

    rows = []
    for r in registros:
        rows.append({
            "id_obra":           _get_field(r, "id", "codigo"),
            "municipio":         _get_field(r, "municipio", "nomeMunicipio"),
            "fonte":             "tce_rj_obras_paralisadas",
            "nome_obra":         _get_field(r, "nome", "descricao", "objeto"),
            "tipo_obra":         _get_field(r, "tipo", "tipoObra"),
            "situacao":          _get_field(r, "situacao", "status"),
            "motivo_paralisacao": _get_field(r, "motivoParalisacao", "motivo", "justificativa"),
            "cnpj_executora":    _get_field(r, "cnpjExecutora", "cnpj"),
            "nome_executora":    _get_field(r, "nomeExecutora", "empresa"),
            "valor_contrato":    _float(_get_field(r, "valorContrato", "valor")),
            "percentual_executado": _float(_get_field(r, "percentualExecutado", "percentual")),
            "data_inicio":       _data(_get_field(r, "dataInicio", "dataOrdemServico")),
            "data_paralisacao":  _data(_get_field(r, "dataParalisacao", "dataOcorrencia")),
            "orgao":             _get_field(r, "orgao", "nomeOrgao"),
            "coletado_em":       datetime.now(timezone.utc).isoformat(),
            "payload_bruto":     json.dumps(r, ensure_ascii=False),
        })

    df = pd.DataFrame(rows)
    log.info(f"Obras paralisadas normalizadas: {len(df)} registros")
    return df


DatasetFetcher = Callable[[], list[dict[str, Any]]]
DatasetNormalizer = Callable[[list[dict[str, Any]]], pd.DataFrame]
DatasetStep = tuple[str, DatasetFetcher, DatasetNormalizer]


def _base_datasets() -> tuple[DatasetStep, ...]:
    """Define as etapas-base do pipeline sem congelar referências para testes."""
    return (
        ("licitacoes", fetch_licitacoes, normalizar_licitacoes),
        ("contratos", fetch_contratos, normalizar_contratos),
        ("compras_diretas", fetch_compras_diretas, normalizar_compras_diretas),
        ("obras_paralisadas", fetch_obras_paralisadas, normalizar_obras_paralisadas),
    )


# ── Análise de licitantes (feature engineering) ───────────────────────────────

def calcular_perfil_fornecedores(
    df_contratos: pd.DataFrame,
    df_compras: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calcula o perfil histórico de cada fornecedor com base nos contratos
    e compras diretas do TCE-RJ. Esses dados alimentam diretamente o
    feature engineering do modelo XGBoost de previsão de risco.

    Métricas calculadas por CNPJ:
        - total de contratos
        - valor total contratado
        - média de valor por contrato
        - total de aditivos
        - taxa de contratos com aditivo
        - anos de atuação em Macaé
    """
    frames = []
    if not df_contratos.empty and "cnpj_fornecedor" in df_contratos.columns:
        frames.append(df_contratos[["cnpj_fornecedor", "nome_fornecedor",
                                     "valor_contrato", "qtd_aditivos",
                                     "possui_aditivo", "ano"]].copy())
    if not df_compras.empty and "cnpj_fornecedor" in df_compras.columns:
        frames.append(df_compras[["cnpj_fornecedor", "nome_fornecedor",
                                   "valor", "ano"]].rename(
            columns={"valor": "valor_contrato"}
        ))

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = df[df["cnpj_fornecedor"].notna() & (df["cnpj_fornecedor"] != "")]

    perfil = df.groupby("cnpj_fornecedor").agg(
        nome_fornecedor=("nome_fornecedor", "first"),
        total_contratos=("valor_contrato", "count"),
        valor_total=("valor_contrato", "sum"),
        valor_medio=("valor_contrato", "mean"),
        anos_atuacao=("ano", "nunique"),
        primeiro_ano=("ano", "min"),
        ultimo_ano=("ano", "max"),
    ).reset_index()

    # Taxa de aditivos (se disponível)
    if "possui_aditivo" in df.columns:
        taxa_aditivo = (
            df[df["possui_aditivo"].notna()]
            .groupby("cnpj_fornecedor")["possui_aditivo"]
            .apply(lambda x: (x.str.lower().isin(["sim", "true", "1", "s"])).mean())
            .reset_index()
            .rename(columns={"possui_aditivo": "taxa_aditivo"})
        )
        perfil = perfil.merge(taxa_aditivo, on="cnpj_fornecedor", how="left")

    perfil["fonte"] = "tce_rj_perfil_fornecedores"
    perfil["coletado_em"] = datetime.now(timezone.utc).isoformat()

    log.info(f"Perfil de fornecedores calculado: {len(perfil)} CNPJs únicos")
    return perfil


# ── Cache ─────────────────────────────────────────────────────────────────────

def _cache_path(nome: str) -> Path:
    return CACHE_DIR / CACHE_FILES[nome]


def _carregar_cache_dataset(nome: str) -> pd.DataFrame:
    path = _cache_path(nome)
    if path.exists():
        df = pd.read_json(path, orient="records")
        log.warning(f"Cache carregado: {path.name} ({len(df)} registros)")
        return df
    return pd.DataFrame()

def _salvar_cache(datasets: dict[str, pd.DataFrame]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    for nome, df in datasets.items():
        if not df.empty:
            path = _cache_path(nome)
            df.to_json(path, orient="records", force_ascii=False, indent=2)
            log.info(f"Cache salvo: {path.name} ({len(df)} registros)")


def _carregar_cache() -> dict[str, pd.DataFrame]:
    return {nome: _carregar_cache_dataset(nome) for nome in CACHE_FILES}


def _executar_etapa(
    nome: str,
    fetcher: DatasetFetcher,
    normalizador: DatasetNormalizer,
) -> pd.DataFrame:
    """Executa uma etapa de coleta e usa o cache do próprio dataset em caso de falha."""
    try:
        registros = fetcher()
        return normalizador(registros)
    except Exception as exc:
        log.error(f"Falha na etapa {nome}: {exc}. Tentando cache local...")
        return _carregar_cache_dataset(nome)


# ── Pipeline principal ────────────────────────────────────────────────────────

def run() -> dict[str, pd.DataFrame]:
    """
    Executa o pipeline completo de coleta do TCE-RJ:

        1. Licitações (paginação direta via inicio/limite)
        2. Contratos (por ano quando o filtro estiver configurado)
        3. Compras diretas (dispensas e inexigibilidades)
        4. Obras paralisadas
        5. Perfil de fornecedores (feature engineering)

    Retorna dict com DataFrames normalizados:
        "licitacoes"          → licitações do município
        "contratos"           → contratos com fornecedores
        "compras_diretas"     → dispensas e inexigibilidades
        "obras_paralisadas"   → obras que paralisaram
        "perfil_fornecedores" → métricas por CNPJ para o ML

    Em caso de falha, serve o cache da última execução bem-sucedida.
    """
    log.info("=" * 55)
    log.info("TCE-RJ — início da coleta")
    anos = _anos_consulta()
    if anos:
        log.info(f"Município: {MUNICIPIO} | Anos: {anos[0]}–{anos[-1]}")
    else:
        log.info(f"Município: {MUNICIPIO} | Filtro por ano: desativado")
    log.info("=" * 55)

    try:
        datasets = {
            nome: _executar_etapa(nome, fetcher, normalizador)
            for nome, fetcher, normalizador in _base_datasets()
        }

        # Etapa 5 — perfil de fornecedores
        try:
            datasets["perfil_fornecedores"] = calcular_perfil_fornecedores(
                datasets["contratos"],
                datasets["compras_diretas"],
            )
        except Exception as exc:
            log.error(f"Falha ao calcular perfil de fornecedores: {exc}. Tentando cache local...")
            datasets["perfil_fornecedores"] = _carregar_cache_dataset("perfil_fornecedores")

        # Salvar cache se pelo menos uma fonte retornou dados
        if any(not df.empty for df in datasets.values()):
            _salvar_cache(datasets)

    except Exception as e:
        log.error(f"Falha na coleta TCE-RJ: {e}. Tentando cache local...")
        datasets = _carregar_cache()

    log.info("=" * 55)
    log.info("TCE-RJ — coleta finalizada:")
    for nome, df in datasets.items():
        log.info(f"  {nome}: {len(df)} registros")
    log.info("=" * 55)

    return datasets


if __name__ == "__main__":
    resultado = run()

    for nome, df in resultado.items():
        if df.empty:
            print(f"\n{nome}: sem dados coletados.")
            continue

        print(f"\n── {nome.upper()} ({len(df)} registros) ──")

        # Preview das colunas mais relevantes por dataset
        preview_cols = {
            "licitacoes":          ["numero", "processo_licitatorio", "ano", "objeto", "modalidade", "valor_estimado", "municipio"],
            "contratos":           ["id_contrato", "ano", "objeto", "cnpj_fornecedor", "nome_fornecedor", "valor_contrato"],
            "compras_diretas":     ["id_compra", "ano", "tipo", "cnpj_fornecedor", "valor"],
            "obras_paralisadas":   ["id_obra", "nome_obra", "situacao", "motivo_paralisacao", "percentual_executado"],
            "perfil_fornecedores": ["cnpj_fornecedor", "nome_fornecedor", "total_contratos", "valor_total", "taxa_aditivo"],
        }

        cols = [c for c in preview_cols.get(nome, df.columns[:5]) if c in df.columns]
        print(df[cols].head(10).to_string(index=False))
