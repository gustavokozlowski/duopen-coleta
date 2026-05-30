"""
scrappers/federal/transparencia_convenios.py
Convênios federais de Macaé — API do Portal da Transparência
duopen-coleta · DUOPEN 2026

Traz a DATA DE CONCLUSÃO REAL dos convênios federais do município (além de
situação, valores e CNPJ do proponente). Diferente do dump SICONV (fim de
vigência = proxy), aqui vem `dataConclusao` de fato — usado para enriquecer
`obras.data_conclusao` do grupo legado (rótulo de atraso do duopen-ml).

Fonte: GET /api-de-dados/convenios?codigoIBGE=<ibge>&pagina=N
    header: chave-api-dados: <TRANSPARENCIA_API_KEY>

Chave de junção: `nr_convenio` (= dimConvenio.codigo) casa com
`raw_obras_legado.num_licitacao` / `obras.num_licitacao`.

> O CNPJ aqui é do CONVENENTE (proponente = Município), não da empresa
> executora — não serve para features de fornecedor.

Variáveis de ambiente (.env):
    TRANSPARENCIA_API_KEY   chave da API (obrigatória)
    TRANSPARENCIA_BASE_URL  base (padrão: https://api.transparencia.gov.br/api-de-dados)
    IBGE_MUNICIPIO_CODE     código IBGE do município (padrão: 3302403 = Macaé)
"""

import os
import re
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
log = logging.getLogger("scraper.transparencia_convenios")

# strip de aspas/espaços: secret colado como `"chave"` (copiado do .env) gera 401
API_KEY         = os.getenv("TRANSPARENCIA_API_KEY", "").strip().strip('"').strip("'").strip()
# Host verificado da API de dados do Portal da Transparência. Var dedicada para não
# colidir com TRANSPARENCIA_BASE_URL (uso legado, aponta para host que pode não resolver).
BASE_URL        = os.getenv("TRANSPARENCIA_CONVENIOS_URL", "https://api.portaldatransparencia.gov.br/api-de-dados").rstrip("/")
MUNICIPIO_IBGE  = os.getenv("IBGE_MUNICIPIO_CODE", "3302403")
RETRY_ATTEMPTS  = 3
RETRY_BACKOFF   = 2.0
REQUEST_TIMEOUT = 30
DELAY_PAGINA    = 0.3
MAX_PAGINAS     = 30          # guarda contra loop; Macaé tem ~118 convênios
CACHE_DIR       = Path(__file__).parent.parent.parent / "cache"

HEADERS = {
    "chave-api-dados": API_KEY,
    "Accept": "application/json",
    "User-Agent": "duopen-coleta/1.0 (hackathon DUOPEN 2026)",
}


# ── Cliente HTTP ────────────────────────────────────────────────────────────────

def _get(path: str, params: dict) -> Optional[list]:
    """GET com retry/backoff. Retorna lista (página) ou None em falha."""
    url = f"{BASE_URL}{path}"
    for tentativa in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            log.error("HTTP %s em %s: %s", getattr(resp, "status_code", "?"), url, e)
            return None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            log.warning("Tentativa %s/%s falhou: %s", tentativa, RETRY_ATTEMPTS, e)
            if tentativa == RETRY_ATTEMPTS:
                return None
            time.sleep(RETRY_BACKOFF * (2 ** (tentativa - 1)))
    return None


# ── Helpers ─────────────────────────────────────────────────────────────────────

def _so_digitos(val) -> Optional[str]:
    if val is None:
        return None
    d = re.sub(r"\D", "", str(val))
    return d or None


def _float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ── Coleta paginada ─────────────────────────────────────────────────────────────

def listar_convenios() -> list[dict]:
    """Pagina todos os convênios federais do município."""
    todos: list[dict] = []
    for pagina in range(1, MAX_PAGINAS + 1):
        dados = _get("/convenios", {"codigoIBGE": MUNICIPIO_IBGE, "pagina": pagina})
        if not dados:
            break
        todos.extend(dados)
        log.info("Página %s: %s convênios (acumulado: %s)", pagina, len(dados), len(todos))
        if len(dados) < 15:   # página incompleta = última
            break
        time.sleep(DELAY_PAGINA)
    log.info("Convênios coletados: %s", len(todos))
    return todos


# ── Normalização ────────────────────────────────────────────────────────────────

def normalizar(brutos: list[dict]) -> pd.DataFrame:
    """Uma linha por convênio, pronta para raw_convenios_federais."""
    if not brutos:
        log.warning("Nenhum convênio para normalizar.")
        return pd.DataFrame()

    coletado_em = datetime.now(timezone.utc).isoformat()
    rows = []
    for c in brutos:
        dim = c.get("dimConvenio") or {}
        conv = c.get("convenente") or {}
        orgao = c.get("orgao") or {}
        rows.append({
            "nr_convenio":          str(dim.get("codigo") or "").strip() or None,
            "numero":               dim.get("numero"),
            "objeto":               dim.get("objeto"),
            "situacao":             c.get("situacao"),
            "data_conclusao":       c.get("dataConclusao"),
            "data_inicio_vigencia": c.get("dataInicioVigencia"),
            "data_fim_vigencia":    c.get("dataFinalVigencia"),
            "data_publicacao":      c.get("dataPublicacao"),
            "valor":                _float(c.get("valor")),
            "valor_liberado":       _float(c.get("valorLiberado")),
            "valor_contrapartida":  _float(c.get("valorContrapartida")),
            "cnpj_proponente":      _so_digitos(conv.get("cnpjFormatado")),
            "nome_proponente":      conv.get("nome"),
            "orgao":                orgao.get("nome") if isinstance(orgao, dict) else None,
            "municipio_ibge":       MUNICIPIO_IBGE,
            "coletado_em":          coletado_em,
            "payload_bruto":        json.dumps(c, ensure_ascii=False),
        })
    df = pd.DataFrame(rows)
    df = df[df["nr_convenio"].notna()].drop_duplicates("nr_convenio")
    log.info("Normalização concluída: %s convênios", len(df))
    return df


# ── Cache ─────────────────────────────────────────────────────────────────────

def _salvar_cache(registros: list[dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / "transparencia_convenios.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, indent=2)
    log.info("Cache salvo: %s (%s registros)", path, len(registros))


def _carregar_cache() -> list[dict]:
    path = CACHE_DIR / "transparencia_convenios.json"
    if not path.exists():
        return []
    log.warning("Usando cache local do Portal da Transparência.")
    return json.loads(path.read_text(encoding="utf-8"))


# ── Pipeline principal ──────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
    """Coleta convênios federais do município. Degrada para cache em falha."""
    log.info("=" * 55)
    log.info("Portal da Transparência — convênios de Macaé (IBGE %s)", MUNICIPIO_IBGE)
    log.info("=" * 55)

    if not API_KEY:
        log.error("TRANSPARENCIA_API_KEY ausente — retornando vazio.")
        return pd.DataFrame()

    brutos = listar_convenios()
    if brutos:
        df = normalizar(brutos)
        # cache guarda os registros NORMALIZADOS (flat, schema da raw) — é o que o
        # pipeline ingere. Salvar os brutos da API quebraria o loader (campo `id`
        # numérico do convênio iria para a coluna UUID).
        _salvar_cache(df.to_dict(orient="records"))
        return df

    log.error("Coleta vazia. Tentando cache local...")
    cache = _carregar_cache()
    return pd.DataFrame(cache) if cache else pd.DataFrame()


if __name__ == "__main__":
    df = run()
    if df.empty:
        print("\nNenhum convênio coletado.")
    else:
        com_concl = df["data_conclusao"].notna().sum()
        print(f"\n── Portal Transparência · {len(df)} convênios ({com_concl} com data de conclusão) ──\n")
        print(df[["nr_convenio", "situacao", "data_conclusao", "valor"]].head(12).to_string(index=False))
