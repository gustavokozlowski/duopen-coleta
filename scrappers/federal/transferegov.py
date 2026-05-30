"""
scrappers/federal/transferegov.py
Aditivos + CNPJ por convênio federal — TransfereGov / SICONV
duopen-coleta · DUOPEN 2026  ·  PROTÓTIPO

Resolve o bloqueio do duopen-ml no modelo de estouro / features de fornecedor do
grupo de TREINO (legado): busca, na fonte federal, os termos aditivos e o CNPJ do
proponente, chaveados pelo número do convênio que já coletamos
(`raw_obras_legado.num_licitacao` = `nr_convenio_obras`).

Fluxo:
    1. Lê a lista de convênios do legado (cache painel_legado_obras.json)
    2. Para cada convênio, consulta o TransfereGov (convênio + termos aditivos)
    3. Agrega aditivos (qtd, valor) + CNPJ proponente → uma linha por convênio
    4. Normaliza, grava cache/transferegov_aditivos.json e retorna DataFrame

⚠️ O endpoint exato do TransfereGov deve ser validado antes de produção
   (a base migrou SICONV → +Brasil → TransfereGov). Ver docs/INSTRUCOES_transferegov.md.
   Em falha, o scraper degrada para cache local ou DataFrame vazio.

Variáveis de ambiente (.env):
    TRANSFEREGOV_BASE_URL   base da API (padrão abaixo)
    LOG_LEVEL               nível de log (padrão: INFO)
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
log = logging.getLogger("scraper.transferegov")

# ⚠️ Verificar o caminho exato do recurso de convênios/aditivos no TransfereGov.
BASE_URL        = os.getenv("TRANSFEREGOV_BASE_URL", "https://api.transferegov.gestao.gov.br").rstrip("/")
RETRY_ATTEMPTS  = 3
RETRY_BACKOFF   = 2.0
REQUEST_TIMEOUT = 30
DELAY_ENTRE     = 0.2  # s entre convênios
CACHE_DIR       = Path(__file__).parent.parent.parent / "cache"
LEGADO_CACHE    = CACHE_DIR / "painel_legado_obras.json"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "duopen-coleta/1.0 (hackathon DUOPEN 2026)",
}


# ── Cliente HTTP ────────────────────────────────────────────────────────────────

def _get(path: str, params: dict | None = None) -> Optional[list | dict]:
    """GET com retry/backoff. Retorna None em falha (não derruba o pipeline)."""
    url = f"{BASE_URL}{path}"
    for tentativa in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params or {}, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            log.warning(f"HTTP {getattr(resp, 'status_code', '?')} em {url}: {e}")
            return None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            log.warning(f"Tentativa {tentativa}/{RETRY_ATTEMPTS} falhou: {e}")
            if tentativa == RETRY_ATTEMPTS:
                return None
            time.sleep(RETRY_BACKOFF * (2 ** (tentativa - 1)))
    return None


# ── Helpers ─────────────────────────────────────────────────────────────────────

def _float(val) -> Optional[float]:
    """Converte número ou string (BR '1.234,56' ou US '1234.56') para float."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).replace("R$", "").strip()
    if not s:
        return None
    # Vírgula presente → formato BR: '.' é milhar, ',' é decimal.
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def _so_digitos(val) -> Optional[str]:
    if val is None:
        return None
    d = re.sub(r"\D", "", str(val))
    return d or None


def convenios_do_legado() -> list[str]:
    """Lê os números de convênio (num_licitacao) do cache do painel legado."""
    if not LEGADO_CACHE.exists():
        log.warning("Cache do legado não encontrado: %s", LEGADO_CACHE)
        return []
    dados = json.loads(LEGADO_CACHE.read_text(encoding="utf-8"))
    convenios = {
        str(r.get("num_licitacao")).strip()
        for r in dados
        if r.get("num_licitacao") not in (None, "", "None")
    }
    log.info("Convênios do legado para consultar: %d", len(convenios))
    return sorted(convenios)


# ── Consulta por convênio ─────────────────────────────────────────────────────

def buscar_convenio(nr_convenio: str) -> Optional[dict]:
    """
    Consulta convênio + termos aditivos no TransfereGov e agrega.
    Retorna dict bruto (convênio + lista de aditivos) ou None.

    ⚠️ Os paths são placeholders a confirmar no recurso real do TransfereGov.
    """
    convenio = _get("/v1/convenios", {"nr_convenio": f"eq.{nr_convenio}"})
    if not convenio:
        return None
    registro = convenio[0] if isinstance(convenio, list) else convenio
    aditivos = _get("/v1/termos_aditivos", {"nr_convenio": f"eq.{nr_convenio}"}) or []
    registro["_aditivos"] = aditivos if isinstance(aditivos, list) else []
    return registro


# ── Normalização ────────────────────────────────────────────────────────────────

def normalizar(brutos: list[dict]) -> pd.DataFrame:
    """Uma linha por convênio: CNPJ + agregação de aditivos."""
    if not brutos:
        log.warning("Nenhum convênio coletado para normalizar.")
        return pd.DataFrame()

    coletado_em = datetime.now(timezone.utc).isoformat()
    rows = []
    for r in brutos:
        aditivos = r.get("_aditivos", []) or []
        valor_aditivos = sum(
            _float(a.get("vl_global_ta") or a.get("valor") or a.get("vl_ta")) or 0.0
            for a in aditivos
        )
        rows.append({
            "nr_convenio":     str(r.get("nr_convenio") or r.get("NR_CONVENIO") or "").strip(),
            "id_proposta":     str(r.get("id_proposta") or r.get("ID_PROPOSTA") or "").strip() or None,
            "cnpj_proponente": _so_digitos(r.get("identif_proponente") or r.get("IDENTIF_PROPONENTE")),
            "nome_proponente": r.get("nm_proponente") or r.get("NM_PROPONENTE"),
            "valor_global":    _float(r.get("vl_global_conv") or r.get("VL_GLOBAL_CONV")),
            "valor_aditivos":  round(valor_aditivos, 2) if aditivos else None,
            "qtd_aditivos":    len(aditivos),
            "situacao":        r.get("sit_convenio") or r.get("SIT_CONVENIO"),
            "coletado_em":     coletado_em,
            "payload_bruto":   json.dumps(r, ensure_ascii=False, default=str),
        })
    df = pd.DataFrame(rows)
    log.info("Normalização concluída: %d convênios", len(df))
    return df


# ── Cache ─────────────────────────────────────────────────────────────────────

def _salvar_cache(registros: list[dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / "transferegov_aditivos.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, indent=2)
    log.info("Cache salvo: %s (%d registros)", path, len(registros))


def _carregar_cache() -> list[dict]:
    path = CACHE_DIR / "transferegov_aditivos.json"
    if not path.exists():
        return []
    log.warning("Usando cache local do TransfereGov.")
    return json.loads(path.read_text(encoding="utf-8"))


# ── Pipeline principal ──────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
    """
    Coleta aditivos + CNPJ por convênio federal do grupo legado.
    Em falha de rede, degrada para cache local ou DataFrame vazio.
    """
    log.info("=" * 55)
    log.info("TransfereGov — aditivos + CNPJ por convênio (legado)")
    log.info("=" * 55)

    convenios = convenios_do_legado()
    if not convenios:
        log.error("Sem convênios para consultar — retornando vazio.")
        return pd.DataFrame()

    brutos: list[dict] = []
    for i, nr in enumerate(convenios, 1):
        log.info("  [%d/%d] convênio %s", i, len(convenios), nr)
        reg = buscar_convenio(nr)
        if reg:
            brutos.append(reg)
        time.sleep(DELAY_ENTRE)

    if not brutos:
        log.error("Coleta vazia (endpoint indisponível?). Tentando cache local...")
        cache = _carregar_cache()
        return normalizar(cache) if cache else pd.DataFrame()

    df = normalizar(brutos)
    _salvar_cache(df.to_dict(orient="records"))
    log.info("Coleta finalizada: %d convênios com dados federais", len(df))
    return df


if __name__ == "__main__":
    df = run()
    if df.empty:
        print("\nNenhum dado coletado (verificar TRANSFEREGOV_BASE_URL / endpoint).")
    else:
        print(f"\n── TransfereGov · {len(df)} convênios ──\n")
        print(df[["nr_convenio", "cnpj_proponente", "qtd_aditivos", "valor_aditivos"]].to_string(index=False))
