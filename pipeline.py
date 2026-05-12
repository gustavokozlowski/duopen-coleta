"""Pipeline ETL: lê caches produzidos pelos scrapers e carrega no Supabase.

Cada arquivo de cache é roteado para a tabela Raw correspondente conforme
`etl/routing.py`. O loader executa upsert na tabela alvo usando a chave de
conflito definida no roteamento e registra o resultado em `ingestoes`.

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
import time
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
    """Lê cache JSON: lista, {metadata, dados} ou dict único (registro só)."""
    try:
        raw = json.loads(caminho.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            dados = raw
        elif isinstance(raw, dict):
            if isinstance(raw.get("dados"), list):
                dados = raw["dados"]
            else:
                # dict plano = um único registro (ex: ibge_metadados.json)
                dados = [raw]
        else:
            return None
        if not isinstance(dados, list) or not dados:
            return None
        return pd.DataFrame(dados)
    except Exception as exc:
        log.error("Falha ao ler %s: %s", caminho.name, exc)
        return None


def _ler_csv(caminho: Path) -> pd.DataFrame:
    """Lê CSV do cache (usado pelo portal_macae que exporta CSV diretamente)."""
    try:
        df = pd.read_csv(caminho, dtype=str)
        log.info("CSV: %s (%s registros)", caminho.name, len(df))
        return df
    except Exception as exc:
        log.error("Falha ao ler %s: %s", caminho.name, exc)
        return pd.DataFrame()


def _aplicar_rota(df: pd.DataFrame, fonte: str, rota: dict) -> pd.DataFrame:
    """Aplica rename/defaults/fonte do roteamento, preparando o DataFrame para o loader."""
    rename = rota.get("rename") or {}
    renames_validos = {
        origem: destino
        for origem, destino in rename.items()
        if origem in df.columns and destino not in df.columns
    }
    transformado = df.rename(columns=renames_validos).copy() if renames_validos else df.copy()

    defaults = rota.get("defaults") or {}
    for coluna, valor in defaults.items():
        if coluna not in transformado.columns:
            transformado[coluna] = valor
            continue
        serie = transformado[coluna]
        validos = serie.notna() & (serie.astype(str).str.strip() != "")
        transformado.loc[~validos, coluna] = valor

    return transformado.assign(fonte=fonte)


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
    from etl.routing import colunas_alvo, resolver_rota

    datasets = _descobrir_datasets()
    if not datasets:
        log.warning("Nenhum dado em cache — nada a carregar no Supabase")
        return 0

    log.info("ETL iniciado: %s dataset(s) a processar", len(datasets))

    client = loader.init_client()

    total_gravado = 0
    falhas: list[str] = []
    pulados: list[str] = []

    for nome, df in datasets:
        rota = resolver_rota(nome)
        if rota is None:
            log.warning("Sem rota definida para '%s' — dataset ignorado", nome)
            pulados.append(nome)
            continue

        tabela = rota["tabela"]
        fonte = rota["fonte"]
        conflict = rota["conflict"]
        colunas_validas = colunas_alvo(tabela)

        inicio = time.monotonic()
        gravado = 0
        status = "ok"
        mensagem: str | None = None

        try:
            log.info(
                "Processando: %s (%s registros) → %s [fonte=%s]",
                nome, len(df), tabela, fonte,
            )
            df_clean = cleaner.clean(df, required_columns=rota.get("required"))
            df_comp = compressor.compress(df_clean)
            df_pronto = _aplicar_rota(df_comp, fonte=fonte, rota=rota)
            gravado = loader.load(
                df_pronto,
                tabela=tabela,
                conflict_column=conflict,
                allowed_columns=colunas_validas or None,
                client=client,
            )
            total_gravado += gravado
            log.info("  → %s registros gravados em %s", gravado, tabela)
        except Exception as exc:
            status = "erro"
            mensagem = str(exc)
            log.error("  → ETL falhou para '%s': %s", nome, exc)
            falhas.append(nome)

        duracao = time.monotonic() - inicio
        loader.registrar_ingestao(
            client=client,
            fonte=fonte,
            status=status,
            qtd_registros=gravado,
            duracao_segundos=duracao,
            qtd_erros=0 if status == "ok" else max(len(df) - gravado, 1),
            mensagem=mensagem,
        )

    log.info(
        "Pipeline concluído: %s registros | %s ok | %s falha(s) | %s ignorado(s)",
        total_gravado,
        len(datasets) - len(falhas) - len(pulados),
        len(falhas),
        len(pulados),
    )
    if falhas:
        log.warning("Datasets com falha no ETL: %s", ", ".join(falhas))
    if pulados:
        log.info("Datasets sem rota: %s", ", ".join(pulados))

    processados = len(datasets) - len(pulados)
    if processados > 0 and len(falhas) == processados:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
