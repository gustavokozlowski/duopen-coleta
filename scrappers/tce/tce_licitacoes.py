"""
scrapers/tce/tce_licitantes.py
Scraper do Portal de Dados Abertos do TCE-RJ — Licitações e Contratos
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
    GET /contratos_municipio → contratos por município (ano=0)

Coleta de licitações:
    Endpoint base:  /licitacoes?inicio=0&limite=1000&csv=false&jsonfull=false
    Filtro local:   registros com Ente = MACAE (comparação sem acento)

Coleta de contratos:
    Endpoint base:  /contratos_municipio?ano=0&inicio=0&limite=1000
                    &municipio=MACAE&csv=false&jsonfull=false
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

# ── Configuração ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper.tce_licitantes")

BASE_URL      = "https://dados.tcerj.tc.br/api/v1"
MUNICIPIO     = os.getenv("TCE_RJ_MUNICIPIO", "MACAE").strip()
LICITACOES_PAGE_LIMIT = 1000
CONTRATOS_PAGE_LIMIT = 1000
DELAY_PAGINAS = 0.3         # segundos entre páginas
RETRY_ATTEMPTS = 3
RETRY_BACKOFF  = 2.0
REQUEST_TIMEOUT = 30
CACHE_DIR     = Path(__file__).parent.parent.parent / "cache"
CONTRATOS_ENDPOINT = "/contratos_municipio"

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

CACHE_FILES = {
    "licitacoes": "tce_licitacoes.json",
    "contratos": "tce_contratos.json",
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

def _paginar_contratos_municipio(params_base: dict[str, Any] | None = None) -> list[dict]:
    """
    Pagina o endpoint de contratos do município usando inicio/limite.

    A API retorna o payload dentro da chave ``Contratos``.
    """
    registros = []
    inicio = 0

    while True:
        params = {
            **(params_base or {}),
            "ano": 0,
            "inicio": inicio,
            "limite": CONTRATOS_PAGE_LIMIT,
            "municipio": MUNICIPIO,
            "csv": "false",
            "jsonfull": "false",
        }
        log.debug(f"GET {CONTRATOS_ENDPOINT} inicio={inicio}...")

        try:
            data = _get(CONTRATOS_ENDPOINT, params)
        except RuntimeError as e:
            log.error(f"Abandonando paginação de contratos: {e}")
            break

        if isinstance(data, list):
            itens = [item for item in data if isinstance(item, dict)]
        elif isinstance(data, dict):
            itens_brutos = (
                data.get("Contratos") or
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

        if len(itens) < CONTRATOS_PAGE_LIMIT:
            break

        inicio += CONTRATOS_PAGE_LIMIT
        time.sleep(DELAY_PAGINAS)

    return registros


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

    Usa o endpoint /contratos_municipio com ano=0 porque a API filtra melhor
    por município do que por exercício isolado.
    """
    log.info("Coletando contratos via TCE-RJ para município=%s...", MUNICIPIO)
    registros = _paginar_contratos_municipio()
    filtrados = [
        registro for registro in registros
        if _municipio_match(registro.get("Ente") or registro.get("Municipio"))
    ]
    log.info("Contratos coletados: %s", len(filtrados))
    return filtrados


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
            "id_contrato":       _get_field(r, "NumeroContrato", "numeroContrato", "id", "codigo", "numero"),
            "ano":               _get_field(r, "Ano", "ano", "exercicio", "AnoContrato", "_ano_coleta"),
            "municipio":         _get_field(r, "Ente", "municipio", "nomeMunicipio"),
            "fonte":             "tce_rj_contratos_municipio",

            # Contrato
            "objeto":            _get_field(r, "Objeto", "objeto", "descricao", "objetoContrato"),
            "modalidade":        _get_field(r, "Modalidade", "modalidade", "tipoLicitacao"),
            "situacao":          _get_field(r, "Situacao", "situacao", "status"),
            "num_licitacao":     _get_field(r, "ProcessoLicitatorio", "numeroLicitacao", "num_licitacao", "licitacao"),

            # Fornecedor — chave para feature engineering do ML
            "cnpj_fornecedor":   _get_field(r, "CNPJCPFContratado", "cnpj", "cnpjFornecedor", "cnpjContratado"),
            "nome_fornecedor":   _get_field(r, "Contratado", "fornecedor", "nomeFornecedor", "nomeContratado", "razaoSocial"),

            # Valores
            "valor_contrato":    _float(_get_field(r, "ValorContrato", "valorContrato", "valor", "valorTotal")),
            "valor_aditivos":    _float(_get_field(r, "ValorAditivos", "valorAditivos", "valor_aditivos")),
            "valor_final":       _float(_get_field(r, "ValorFinal", "valorFinal", "valor_final")),

            # Datas
            "data_assinatura":   _data(_get_field(r, "DataAssinaturaContrato", "dataAssinatura", "data_assinatura", "dataContrato")),
            "data_inicio":       _data(_get_field(r, "DataInicio", "dataInicio", "data_inicio", "dataVigenciaInicio")),
            "data_fim":          _data(_get_field(r, "DataVencimentoContrato", "DataFim", "dataFim", "data_fim", "dataVigenciaFim")),
            "data_rescisao":     _data(_get_field(r, "DataRescisao", "dataRescisao", "data_rescisao")),

            # Órgão
            "orgao":             _get_field(r, "UnidadeGestora", "orgao", "nomeOrgao", "unidadeGestora"),

            # Aditivos — feature importante para ML
            "qtd_aditivos":      _get_field(r, "QuantidadeAditivos", "quantidadeAditivos", "qtd_aditivos", "numAditivos"),
            "possui_aditivo":    _get_field(r, "PossuiAditivo", "possuiAditivo", "temAditivo"),

            # Auditoria
            "coletado_em":       datetime.now(timezone.utc).isoformat(),
            "payload_bruto":     json.dumps(r, ensure_ascii=False),
        })

    df = pd.DataFrame(rows)
    log.info(f"Contratos normalizados: {len(df)} registros")
    return df


DatasetFetcher = Callable[[], list[dict[str, Any]]]
DatasetNormalizer = Callable[[list[dict[str, Any]]], pd.DataFrame]
DatasetStep = tuple[str, DatasetFetcher, DatasetNormalizer]


def _base_datasets() -> tuple[DatasetStep, ...]:
    """Define as etapas-base do pipeline sem congelar referências para testes."""
    return (
        ("licitacoes", fetch_licitacoes, normalizar_licitacoes),
        ("contratos", fetch_contratos, normalizar_contratos),
    )


# ── Análise de licitantes (feature engineering) ───────────────────────────────

def calcular_perfil_fornecedores(df_contratos: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o perfil histórico de cada fornecedor com base nos contratos
    do TCE-RJ. Esses dados alimentam diretamente o feature engineering do
    modelo XGBoost de previsão de risco.

    Métricas calculadas por CNPJ:
        - total de contratos
        - valor total contratado
        - média de valor por contrato
        - total de aditivos
        - taxa de contratos com aditivo
        - anos de atuação em Macaé
    """
    if df_contratos.empty or "cnpj_fornecedor" not in df_contratos.columns:
        return pd.DataFrame()

    df = df_contratos.copy()
    for coluna in ["nome_fornecedor", "valor_contrato", "qtd_aditivos", "possui_aditivo", "ano"]:
        if coluna not in df.columns:
            df[coluna] = pd.NA

    df = df[["cnpj_fornecedor", "nome_fornecedor", "valor_contrato",
             "qtd_aditivos", "possui_aditivo", "ano"]]
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
        2. Contratos (consulta direta via /contratos_municipio)
        3. Perfil de fornecedores (feature engineering baseado em contratos)

    Retorna dict com DataFrames normalizados:
        "licitacoes"          → licitações do município
        "contratos"           → contratos com fornecedores
        "perfil_fornecedores" → métricas por CNPJ para o ML

    Em caso de falha, serve o cache da última execução bem-sucedida.
    """
    log.info("=" * 55)
    log.info("TCE-RJ — início da coleta")
    log.info(f"Município: {MUNICIPIO}")
    log.info("=" * 55)

    try:
        datasets = {
            nome: _executar_etapa(nome, fetcher, normalizador)
            for nome, fetcher, normalizador in _base_datasets()
        }

        # Perfil de fornecedores — derivado apenas dos contratos
        try:
            datasets["perfil_fornecedores"] = calcular_perfil_fornecedores(datasets["contratos"])
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
            "perfil_fornecedores": ["cnpj_fornecedor", "nome_fornecedor", "total_contratos", "valor_total", "taxa_aditivo"],
        }

        cols = [c for c in preview_cols.get(nome, df.columns[:5]) if c in df.columns]
        print(df[cols].head(10).to_string(index=False))
