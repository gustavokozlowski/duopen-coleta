"""Pipeline ETL: lê caches produzidos pelos scrapers e carrega no Supabase.

Fluxo esperado no CI:
  python scrappers/macae/portal_macae.py   → salva cache/
  python scrappers/tce/tce_rj.py           → salva cache/
  ...outros scrapers...
  python pipeline.py                       → lê cache/ → ETL → Supabase
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")

_CACHE_DIR = Path(os.getenv("CACHE_DIR", "cache"))


def _ler_json(caminho: Path) -> pd.DataFrame | None:
    """Lê um cache JSON no formato {metadata, dados} e retorna DataFrame."""
    try:
        payload = json.loads(caminho.read_text(encoding="utf-8"))
        dados = payload.get("dados")
        if not isinstance(dados, list) or not dados:
            return None
        return pd.DataFrame(dados)
    except Exception as exc:
        log.error("Falha ao ler %s: %s", caminho.name, exc)
        return None


def _ler_csv(caminho: Path) -> pd.DataFrame:
    """Lê CSV do cache (usado pelo portal_macae que exporta CSV diretamente)."""
    try:
        # dtype=str preserva valores brutos para o cleaner decidir a conversão
        df = pd.read_csv(caminho, dtype=str)
        log.info("CSV: %s (%s registros)", caminho.name, len(df))
        return df
    except Exception as exc:
        log.error("Falha ao ler %s: %s", caminho.name, exc)
        return pd.DataFrame()


def _descobrir_datasets() -> list[tuple[str, pd.DataFrame]]:
    """Descobre todos os datasets disponíveis no diretório de cache."""
    if not _CACHE_DIR.exists():
        log.warning("Diretório de cache não encontrado: %s", _CACHE_DIR)
        return []

    datasets: list[tuple[str, pd.DataFrame]] = []

    for caminho in sorted(_CACHE_DIR.glob("*.json")):
        df = _ler_json(caminho)
        if df is not None and not df.empty:
            log.info("Cache JSON: %s (%s registros)", caminho.stem, len(df))
            datasets.append((caminho.stem, df))

    for caminho in sorted(_CACHE_DIR.glob("*.csv")):
        df = _ler_csv(caminho)
        if not df.empty:
            datasets.append((caminho.stem, df))

    return datasets


def main() -> int:
    from etl import cleaner, compressor, loader

    datasets = _descobrir_datasets()
    if not datasets:
        log.warning("Nenhum dado em cache — nada a carregar no Supabase")
        return 0

    log.info("ETL iniciado: %s dataset(s) a processar", len(datasets))

    total_gravado = 0
    falhas: list[str] = []

    for nome, df in datasets:
        try:
            log.info("Processando: %s (%s registros)", nome, len(df))
            df_clean = cleaner.clean(df)
            df_comp = compressor.compress(df_clean)
            gravado = loader.load(df_comp)
            total_gravado += gravado
            log.info("  → %s registros gravados no Supabase", gravado)
        except Exception as exc:
            log.error("  → ETL falhou para '%s': %s", nome, exc)
            falhas.append(nome)

    log.info(
        "Pipeline concluído: %s registros | %s ok | %s falha(s)",
        total_gravado,
        len(datasets) - len(falhas),
        len(falhas),
    )
    if falhas:
        log.warning("Datasets com falha no ETL: %s", ", ".join(falhas))

    # Só retorna código de erro se TODOS os datasets falharam
    return 1 if falhas and len(falhas) == len(datasets) else 0


if __name__ == "__main__":
    sys.exit(main())
