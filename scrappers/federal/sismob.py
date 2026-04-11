"""
scrapers/federal/sismob.py
Scraper do SISMOB Cidadão — Ministério da Saúde / FNS
duopen-coleta · DUOPEN 2026

Coleta obras de infraestrutura de saúde financiadas pelo Ministério da Saúde
no município de Macaé (RJ) via API pública do SISMOB Cidadão.

Fluxo:
    1. Paginar endpoint de listagem para coletar todos os propostaIds
    2. Para cada propostaId, buscar detalhes completos da obra
    3. Normalizar e retornar DataFrame pronto para o ETL

Endpoints:
    Listagem:  GET /api/public/obras?size=&page=&ufIbge=&municipioIbge=
    Detalhe:   GET /api/public/obras/:propostaId

Variáveis de ambiente (.env):
    IBGE_MUNICIPIO_CODE   código IBGE de Macaé — 6 dígitos (padrão: 330240)
    IBGE_UF_CODE          código IBGE da UF — 2 dígitos (padrão: 33 = RJ)
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
log = logging.getLogger("scraper.sismob")

BASE_URL          = "https://sismobcidadao.saude.gov.br/api/public"
MUNICIPIO_SISMOB    = os.getenv("SISMOB_MUNICIPIO_CODE", "330240")   # 6 dígitos
UF_IBGE           = os.getenv("IBGE_UF_CODE", "33")              # RJ
PAGE_SIZE         = 50       # máximo recomendado por página
DELAY_ENTRE_OBRAS = 0.2      # segundos entre chamadas de detalhe
RETRY_ATTEMPTS    = 3
RETRY_BACKOFF     = 2.0      # segundos base para backoff exponencial
REQUEST_TIMEOUT   = 30       # segundos
CACHE_DIR         = Path(__file__).parent.parent.parent / "cache"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "duopen-coleta/1.0 (hackathon DUOPEN 2026)",
}


# ── Cliente HTTP ──────────────────────────────────────────────────────────────

def _get(path: str, params: dict = None) -> dict:
    """
    GET com retry e backoff exponencial.
    Lança RuntimeError após esgotar as tentativas.
    """
    url = f"{BASE_URL}{path}"
    for attempt in range(1, RETRY_ATTEMPTS + 1):
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
            # Erros 4xx não adianta retentativa
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


# ── Listagem paginada ─────────────────────────────────────────────────────────

def listar_obras() -> list[dict]:
    """
    Pagina o endpoint de listagem e retorna todos os registros resumidos
    de obras de Macaé.

    Retorna lista com campos básicos de cada obra, incluindo propostaId
    que será usado para buscar os detalhes.
    """
    log.info("Iniciando listagem paginada de obras...")

    todas = []
    pagina = 0

    while True:
        params = {
            "size":          PAGE_SIZE,
            "page":          pagina,
            "ufIbge":        UF_IBGE,
            "municipioIbge": MUNICIPIO_SISMOB,
            "sgUf":          "",
            "noMunicipio":   "",
        }

        log.info(f"Buscando página {pagina}...")
        data = _get("/obras", params)

        itens = data.get("content", [])
        todas.extend(itens)

        total_paginas = data.get("totalPages", 1)
        total_registros = data.get("totalElements", len(todas))

        log.info(
            f"Página {pagina + 1}/{total_paginas} — "
            f"{len(itens)} obras — total acumulado: {len(todas)}"
        )

        # Última página
        if data.get("last", True) or pagina + 1 >= total_paginas:
            break

        pagina += 1
        time.sleep(0.3)  # delay entre páginas

    log.info(f"Listagem concluída: {len(todas)} obras de {total_registros} esperadas")
    return todas


# ── Detalhe por obra ──────────────────────────────────────────────────────────

def buscar_detalhe(proposta_id: int) -> Optional[dict]:
    """
    Busca os detalhes completos de uma obra pelo propostaId.

    Retorna dict com todos os campos do endpoint de detalhe,
    ou None se a obra não for encontrada (404).
    """
    try:
        return _get(f"/obras/{proposta_id}")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            log.warning(f"Obra {proposta_id} não encontrada (404) — ignorando.")
            return None
        raise
    except RuntimeError as e:
        log.error(f"Falha ao buscar detalhe da obra {proposta_id}: {e}")
        return None


def buscar_todos_detalhes(obras: list[dict]) -> list[dict]:
    """
    Itera a lista de obras e busca os detalhes de cada uma pelo propostaId.
    Preserva os campos do resumo em obras cujo detalhe falhar.

    Retorna lista de dicts mesclando resumo + detalhe.
    """
    log.info(f"Buscando detalhes de {len(obras)} obras...")
    resultados = []

    for i, obra in enumerate(obras, start=1):
        proposta_id = obra.get("propostaId")
        if not proposta_id:
            log.warning(f"Obra sem propostaId — ignorando: {obra}")
            continue

        log.info(f"  [{i}/{len(obras)}] propostaId={proposta_id}")
        detalhe = buscar_detalhe(proposta_id)

        if detalhe:
            # Mescla resumo + detalhe (detalhe sobrescreve campos duplicados)
            registro = {**obra, **detalhe}
        else:
            # Fallback: usa apenas o resumo da listagem
            registro = obra

        resultados.append(registro)
        time.sleep(DELAY_ENTRE_OBRAS)

    log.info(f"Detalhes coletados: {len(resultados)} obras")
    return resultados


# ── Normalização ──────────────────────────────────────────────────────────────

def _data(val) -> Optional[str]:
    """Normaliza datas para ISO 8601 UTC."""
    if not val:
        return None
    formatos = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f+0000",
        "%Y-%m-%dT%H:%M:%S+0000",
        "%d/%m/%Y",
    ]
    s = str(val).strip()
    for fmt in formatos:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue
    return s  # retorna string original se nenhum formato bater


def _float(val) -> Optional[float]:
    """Converte valor para float ou None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def normalizar(registros: list[dict]) -> pd.DataFrame:
    """
    Transforma os registros brutos (resumo + detalhe mesclados)
    em DataFrame normalizado e pronto para o ETL.

    Campos mapeados dos dois endpoints:
        - Listagem: propostaId, situacaoObra, tipoObra, programa, coordenadas...
        - Detalhe:  datas, valores, parcelas, empresas, fases do projeto...
    """
    if not registros:
        log.warning("Nenhum registro para normalizar.")
        return pd.DataFrame()

    rows = []
    for r in registros:
        rows.append({
            # ── Identificação ────────────────────────────────────────
            "proposta_id":                   r.get("propostaId") or r.get("coSeqProposta"),
            "numero_proposta":               r.get("numeroProposta") or r.get("nuProposta"),
            "fonte":                         "sismob_cidadao",
            "municipio_ibge":                MUNICIPIO_SISMOB,
            "uf":                            r.get("uf") or r.get("sgUf"),
            "municipio":                     r.get("municipio") or r.get("noMunicipioAcentuado"),
            "cnpj_fundo":                    r.get("nuCnpj"),
            "entidade":                      r.get("noPadronizadoEntidade"),
            "esfera_administrativa":         r.get("dsEsferaAdministrativa"),

            # ── Obra ─────────────────────────────────────────────────
            "situacao_obra":                 r.get("situacaoObra") or r.get("dsSituacaoObra"),
            "co_situacao_obra":              r.get("coSituacaoObra"),
            "tipo_obra":                     r.get("tipoObra") or r.get("dsTipoObra"),
            "co_tipo_obra":                  r.get("coTipoObra"),
            "tipo_recurso":                  r.get("tipoRecurso") or r.get("dsTipoRecurso"),
            "programa":                      r.get("programa") or r.get("dsPrograma"),
            "rede_programa":                 r.get("redePrograma") or r.get("dsRedePrograma"),
            "fase_projeto":                  r.get("dsFaseProjeto"),
            "etapa_proposta":                r.get("dsEtapaProposta"),
            "portaria":                      r.get("nuPortaria"),
            "dt_portaria":                   _data(r.get("dtPortaria")),
            "ano_referencia":                r.get("nuAnoReferencia"),

            # ── Estabelecimento de saúde ──────────────────────────────
            "nome_estabelecimento":          r.get("nomeEstabelecimento") or r.get("noEstabelecimentoCnes"),
            "nome_estabelecimento_proposta": r.get("noEstabelecimentoProposta"),
            "cnes":                          r.get("cnes") or r.get("coCnes"),
            "co_unidade":                    r.get("coUnidade"),

            # ── Localização ───────────────────────────────────────────
            "bairro":                        r.get("novoBairro") or r.get("bairro") or r.get("noBairro"),
            "logradouro":                    r.get("dsLogradouro"),
            "numero":                        r.get("nuEndereco"),
            "complemento":                   r.get("dsComplemento"),
            "cep":                           r.get("nuCep"),
            "latitude":                      _float(r.get("nuLatitude")),
            "longitude":                     _float(r.get("nuLongitude")),

            # ── Financeiro ────────────────────────────────────────────
            "valor_proposta":                _float(r.get("vlProposta")),
            "valor_total_contrato":          _float(r.get("vlTotalContrato")),
            "percentual_executado":          _float(r.get("vlPercentualExecutado")),
            "valor_1a_parcela":              _float(r.get("vlPrimeraParcela")),
            "valor_2a_parcela":              _float(r.get("vlSegundaParcela")),
            "valor_3a_parcela":              _float(r.get("vlTerceiraParcela")),
            "valor_4a_parcela":              _float(r.get("vlQuartaParcela")),

            # ── Datas ─────────────────────────────────────────────────
            "dt_cadastro":                   _data(r.get("dtCadastro")),
            "dt_inicio_projeto":             _data(r.get("dtInicioProjeto")),
            "dt_prevista_inicio":            _data(r.get("dtPrevistaInicioProjeto")),
            "dt_prevista_conclusao":         _data(r.get("dtPrevistaConclusaoProjeto")),
            "dt_conclusao_projeto":          _data(r.get("dtConclusaoProjeto")),
            "dt_ordem_servico":              _data(r.get("dtOrdemServico")),
            "dt_inicio_obra":                _data(r.get("dtInicioObra")),
            "dt_execucao":                   _data(r.get("dtExecucao")),
            "dt_conclusao_final":            _data(r.get("dtConclusaoFinal")),
            "dt_prevista_conclusao_final":   _data(r.get("dtProvavelConclusaoFinal")),
            "dt_1a_parcela":                 _data(r.get("dtPrimeiraParcela")),
            "dt_2a_parcela":                 _data(r.get("dtSegundaParcela")),
            "dt_3a_parcela":                 _data(r.get("dtTerceiraParcela")),
            "dt_4a_parcela":                 _data(r.get("dtQuartaParcela")),
            "dt_atualizacao":                _data(r.get("dtAtualizacao") or r.get("dtMudancaSituacao")),
            "dt_inauguracao":                _data(r.get("dtInauguracao")),
            "dt_inicio_funcionamento":       _data(r.get("dtInicioFuncionamento")),

            # ── Metadados adicionais ───────────────────────────────────
            "possui_aditivo_contratual":     r.get("stAditivoContratual"),
            "qtd_fotos":                     sum(
                                                 len(g.get("fotos", []))
                                                 for g in r.get("gruposFotografias", [])
                                             ),
            "qtd_empresas":                  len(r.get("empresas", [])),
            "justificativa":                 r.get("dsJustificativa"),

            # ── Auditoria ─────────────────────────────────────────────
            "coletado_em":                   datetime.now(timezone.utc).isoformat(),
            "payload_bruto":                 json.dumps(r, ensure_ascii=False),
        })

    df = pd.DataFrame(rows)
    log.info(f"Normalização concluída: {len(df)} registros")
    return df


# ── Cache ─────────────────────────────────────────────────────────────────────

def _salvar_cache(registros: list[dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / "sismob.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, indent=2)
    log.info(f"Cache salvo: {path} ({len(registros)} registros)")


def _carregar_cache() -> list[dict]:
    path = CACHE_DIR / "sismob.json"
    if not path.exists():
        log.warning("Cache SISMOB não encontrado.")
        return []
    with open(path, encoding="utf-8") as f:
        dados = json.load(f)
    log.warning(f"Usando cache local SISMOB ({len(dados)} registros)")
    return dados


# ── Pipeline principal ────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
    """
    Executa o pipeline completo de coleta do SISMOB Cidadão:

        1. Lista todas as obras de Macaé (paginado)
        2. Busca detalhes de cada obra pelo propostaId
        3. Normaliza e retorna DataFrame pronto para o ETL

    Em caso de falha, serve o cache da última execução bem-sucedida.
    """
    log.info("=" * 55)
    log.info("SISMOB Cidadão — início da coleta")
    log.info(f"Município: Macaé · municipioIbge={MUNICIPIO_SISMOB} · ufIbge={UF_IBGE}")
    log.info("=" * 55)

    try:
        # Etapa 1 — listar obras
        obras_resumo = listar_obras()
        if not obras_resumo:
            raise RuntimeError("Listagem retornou vazia.")

        # Etapa 2 — buscar detalhes
        obras_detalhadas = buscar_todos_detalhes(obras_resumo)

        # Salvar cache após sucesso
        _salvar_cache(obras_detalhadas)

    except Exception as e:
        log.error(f"Falha na coleta: {e}. Tentando cache local...")
        obras_detalhadas = _carregar_cache()
        if not obras_detalhadas:
            log.error("Cache vazio. Retornando DataFrame vazio.")
            return pd.DataFrame()

    # Etapa 3 — normalizar
    df = normalizar(obras_detalhadas)

    log.info("=" * 55)
    log.info(f"Coleta finalizada: {len(df)} obras de saúde em Macaé")
    log.info("=" * 55)
    return df


if __name__ == "__main__":
    df = run()

    if df.empty:
        print("\nNenhum dado coletado.")
    else:
        print(f"\n── SISMOB · {len(df)} obras de saúde em Macaé ──\n")
        print(df[[
            "proposta_id", "nome_estabelecimento",
            "situacao_obra", "tipo_obra",
            "valor_proposta", "percentual_executado",
            "latitude", "longitude",
        ]].to_string(index=False))