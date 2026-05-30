"""
scrappers/federal/transferegov.py
Aditivos por convênio federal — dados abertos SICONV / TransfereGov
duopen-coleta · DUOPEN 2026

Resolve o bloqueio do duopen-ml no modelo de estouro / features de fornecedor do
grupo de TREINO (legado): traz, da fonte federal, os termos aditivos por convênio,
chaveados pelo número que já coletamos (`raw_obras_legado.num_licitacao` =
`nr_convenio_obras`). O join municipal (`raw_contratos`) dava 0% — legado são
convênios federais, não contratos municipais.

Fonte (verificada): dump CSV de dados abertos do SICONV
    https://repositorio.dados.gov.br/seges/detru/siconv_convenio.csv.zip
    https://repositorio.dados.gov.br/seges/detru/siconv_termo_aditivo.csv.zip
Arquivos nacionais (`;`, latin-1) — baixados e filtrados pelos ~35 convênios do
legado (não há varredura por registro).

Saída: uma linha por convênio com valor global, situação, qtd de aditivos e soma
do valor dos aditivos (muitos são "Alteração de Vigência" → valor 0, o que é um
verdadeiro-negativo de estouro, não dado faltante).

> CNPJ do proponente não vem destes 2 arquivos (exigiria siconv_proposta.csv,
> ~199 MB); o legado já traz `cnpj_executora` direto. Ver docs/INSTRUCOES_transferegov.md.

Variáveis de ambiente (.env):
    TRANSFEREGOV_REPO_URL   base do repositório de dados abertos (padrão abaixo)
    LOG_LEVEL               nível de log (padrão: INFO)
"""

import os
import io
import csv
import json
import zipfile
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

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

REPO_URL        = os.getenv("TRANSFEREGOV_REPO_URL", "https://repositorio.dados.gov.br/seges/detru").rstrip("/")
ARQ_CONVENIO    = "siconv_convenio.csv.zip"
ARQ_ADITIVO     = "siconv_termo_aditivo.csv.zip"
REQUEST_TIMEOUT = 240   # arquivos nacionais (16–57 MB)
CACHE_DIR       = Path(__file__).parent.parent.parent / "cache"
LEGADO_CACHE    = CACHE_DIR / "painel_legado_obras.json"

HEADERS = {"User-Agent": "duopen-coleta/1.0 (hackathon DUOPEN 2026)"}


# ── Leitura dos convênios-alvo (do legado) ──────────────────────────────────────

def convenios_do_legado() -> set[str]:
    """Conjunto de números de convênio (num_licitacao) do cache do painel legado."""
    if not LEGADO_CACHE.exists():
        log.warning("Cache do legado não encontrado: %s", LEGADO_CACHE)
        return set()
    dados = json.loads(LEGADO_CACHE.read_text(encoding="utf-8"))
    convenios = {
        str(r.get("num_licitacao")).strip()
        for r in dados
        if r.get("num_licitacao") not in (None, "", "None")
    }
    log.info("Convênios do legado para consultar: %d", len(convenios))
    return convenios


# ── Download + leitura streaming do CSV (zip nacional) ──────────────────────────

def _baixar_zip_csv(arquivo: str) -> Iterator[dict]:
    """
    Baixa um zip de dados abertos SICONV e itera as linhas do CSV interno.
    CSV é `;`-separado, UTF-8 com BOM (utf-8-sig remove o BOM e decodifica certo).
    """
    url = f"{REPO_URL}/{arquivo}"
    log.info("Baixando %s ...", url)
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(resp.content))
    nome_interno = z.namelist()[0]
    with z.open(nome_interno) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"), delimiter=";")
        for row in reader:
            yield row


def _chave_nr_convenio(row: dict) -> str:
    """Lê NR_CONVENIO (utf-8-sig remove o BOM, mas mantém tolerante por segurança)."""
    for k in row:
        if k.endswith("NR_CONVENIO"):
            return (row.get(k) or "").strip()
    return ""


def _valor(texto) -> float:
    """Converte valor SICONV ('356400' ou '324023,45') para float; '' → 0.0."""
    if texto is None:
        return 0.0
    s = str(texto).strip()
    if not s:
        return 0.0
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


# ── Coleta ──────────────────────────────────────────────────────────────────────

def coletar(convenios: set[str]) -> dict[str, dict]:
    """Retorna {nr_convenio: {dados do convênio + agregado de aditivos}} filtrado."""
    dados: dict[str, dict] = {}

    # 1) Convênio: valor global, situação, id_proposta, qtd de aditivos (QTD_TA)
    for row in _baixar_zip_csv(ARQ_CONVENIO):
        nr = _chave_nr_convenio(row)
        if nr in convenios:
            dados[nr] = {
                "nr_convenio":  nr,
                "id_proposta":  (row.get("ID_PROPOSTA") or "").strip() or None,
                "valor_global": _valor(row.get("VL_GLOBAL_CONV")),
                "situacao":     row.get("SIT_CONVENIO"),
                "qtd_aditivos": int(row.get("QTD_TA") or 0) if (row.get("QTD_TA") or "").strip().isdigit() else 0,
                "valor_aditivos": 0.0,
                "_tem_aditivo": False,
            }
    log.info("Convênios encontrados no SICONV: %d/%d", len(dados), len(convenios))

    if not dados:
        return dados

    # 2) Termos aditivos: soma do valor por convênio (muitos = vigência → 0)
    for row in _baixar_zip_csv(ARQ_ADITIVO):
        nr = _chave_nr_convenio(row)
        if nr in dados:
            dados[nr]["valor_aditivos"] += _valor(row.get("VL_GLOBAL_TA"))
            dados[nr]["_tem_aditivo"] = True

    return dados


# ── Normalização ────────────────────────────────────────────────────────────────

def normalizar(dados: dict[str, dict]) -> pd.DataFrame:
    """Uma linha por convênio, pronta para raw_aditivos_federais."""
    if not dados:
        log.warning("Nenhum convênio para normalizar.")
        return pd.DataFrame()

    coletado_em = datetime.now(timezone.utc).isoformat()
    rows = []
    for d in dados.values():
        rows.append({
            "nr_convenio":     d["nr_convenio"],
            "id_proposta":     d.get("id_proposta"),
            "cnpj_proponente": None,  # exige siconv_proposta (~199 MB); legado já tem cnpj_executora
            "nome_proponente": None,
            "valor_global":    round(d["valor_global"], 2),
            "valor_aditivos":  round(d["valor_aditivos"], 2) if d["_tem_aditivo"] else None,
            "qtd_aditivos":    d["qtd_aditivos"],
            "situacao":        d.get("situacao"),
            "coletado_em":     coletado_em,
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
    Coleta aditivos por convênio federal (legado) dos dados abertos SICONV.
    Em falha de rede, degrada para cache local ou DataFrame vazio.
    """
    log.info("=" * 55)
    log.info("TransfereGov/SICONV — aditivos por convênio (legado)")
    log.info("=" * 55)

    convenios = convenios_do_legado()
    if not convenios:
        log.error("Sem convênios para consultar — retornando vazio.")
        return pd.DataFrame()

    try:
        dados = coletar(convenios)
    except Exception as exc:  # rede/zip/parsing
        log.error("Falha na coleta SICONV: %s. Tentando cache local...", exc)
        cache = _carregar_cache()
        return normalizar({d["nr_convenio"]: {**d, "_tem_aditivo": d.get("valor_aditivos") is not None}
                           for d in cache}) if cache else pd.DataFrame()

    df = normalizar(dados)
    if not df.empty:
        _salvar_cache(df.to_dict(orient="records"))
    log.info("Coleta finalizada: %d convênios com dados federais", len(df))
    return df


if __name__ == "__main__":
    df = run()
    if df.empty:
        print("\nNenhum convênio do legado encontrado no SICONV.")
    else:
        print(f"\n── TransfereGov/SICONV · {len(df)} convênios ──\n")
        print(df[["nr_convenio", "qtd_aditivos", "valor_aditivos", "valor_global", "situacao"]].to_string(index=False))
