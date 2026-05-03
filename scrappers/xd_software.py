"""
scrappers/xd_software.py
Ingestao de exportacoes do XD Software (XLS/XLSX/ZIP/GZ).

Fluxo:
	- Detecta formato do arquivo
	- Le planilha direto ou a partir de arquivo compactado
	- Adiciona metadados da fonte e timestamp UTC
	- Salva cache para fallback
"""

from __future__ import annotations

import gzip
import logging
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple, Any
import io

import pandas as pd
import openpyxl  # noqa: F401
import xlrd  # noqa: F401
from dotenv import load_dotenv

from etl.fallback import carregar_cache, salvar_cache

load_dotenv()

# ── Configuracao ─────────────────────────────────────────────────────────────

logging.basicConfig(
	level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
	format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
	datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper.xd_software")

FONTE_NOME = "XD Software"
CACHE_NOME = "xd_software"

DATE_COLUMN_HINTS: tuple[str, ...] = (
	"data",
	"date",
	"dt_",
	"_dt",
	"assinatura",
	"vigencia",
	"vencimento",
	"inicio",
	"fim",
	"rescisao",
	"publicacao",
	"atualizacao",
	"coletado",
)


# ── Utilitarios internos ─────────────────────────────────────────────────────
def _engine_for_suffix(suffix: str) -> Optional[str]:
	normalized = suffix.lower()
	if normalized == ".xlsx":
		return "openpyxl"
	if normalized == ".xls":
		return "xlrd"
	return None


def _select_excel_name(names: list[str]) -> str:
	candidates = [
		name
		for name in names
		if name.lower().endswith((".xlsx", ".xls")) and not name.endswith("/")
	]
	if not candidates:
		raise ValueError("Nenhum arquivo Excel encontrado no arquivo compactado")
	candidates.sort(key=lambda n: 0 if n.lower().endswith(".xlsx") else 1)
	return candidates[0]


def _read_excel_from_bytes(blob: bytes, suffix: str) -> pd.DataFrame:
	engine = _engine_for_suffix(suffix)
	buffer = io.BytesIO(blob)
	return pd.read_excel(buffer, engine=engine)


def _read_excel_from_zip(path: Path) -> Tuple[pd.DataFrame, str]:
	with zipfile.ZipFile(path) as zf:
		inner_name = _select_excel_name(zf.namelist())
		with zf.open(inner_name) as handle:
			data = handle.read()
	df = _read_excel_from_bytes(data, Path(inner_name).suffix)
	return df, inner_name


def _read_excel_from_gz(path: Path) -> pd.DataFrame:
	with gzip.open(path, "rb") as handle:
		data = handle.read()
	suffixes = path.suffixes
	inner_suffix = suffixes[-2] if len(suffixes) >= 2 else ""
	return _read_excel_from_bytes(data, inner_suffix)


def _is_date_column(column_name: str) -> bool:
	normalized = column_name.lower()
	return any(hint in normalized for hint in DATE_COLUMN_HINTS)


def _decode_value(value: Any) -> Any:
	if isinstance(value, (bytes, bytearray, memoryview)):
		return bytes(value).decode("utf-8", errors="replace")
	return value


def _normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
	if df is None or df.empty:
		return pd.DataFrame() if df is None else df.copy()

	cleaned = df.copy()
	for column in cleaned.columns:
		if cleaned[column].dtype != object:
			continue
		cleaned[column] = cleaned[column].apply(_decode_value)
	return cleaned


def _normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
	if df is None or df.empty:
		return pd.DataFrame() if df is None else df.copy()

	cleaned = df.copy()
	for column in cleaned.columns:
		if not _is_date_column(column):
			continue
		cleaned[column] = pd.to_datetime(cleaned[column], utc=True, errors="coerce")
	return cleaned


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
	cleaned = _normalize_text_columns(df)
	return _normalize_dates(cleaned)


def _prepare_raw_df(df: pd.DataFrame, payload: Optional[list[dict]] = None) -> pd.DataFrame:
	if df is None:
		return pd.DataFrame()
	payload = payload or df.to_dict(orient="records")
	prepared = df.copy()
	prepared["payload_bruto"] = payload
	prepared["fonte"] = FONTE_NOME
	prepared["coletado_em"] = pd.Timestamp(datetime.now(timezone.utc))
	return prepared


# ── API publica ──────────────────────────────────────────────────────────────
def run(filepath: str) -> pd.DataFrame:
	"""Le arquivo do XD Software e devolve DataFrame bruto para o ETL."""
	path = Path(filepath) if filepath else None
	try:
		if path is None:
			raise ValueError("filepath nao informado")
		if not path.exists():
			raise FileNotFoundError(f"Arquivo nao encontrado: {path}")

		decompressed = False
		if path.suffix.lower() in {".zip"}:
			df_raw, inner = _read_excel_from_zip(path)
			decompressed = True
			log.info("Arquivo compactado lido: %s (entrada=%s)", path.name, inner)
		elif path.suffix.lower() in {".gz"}:
			try:
				df_raw, inner = _read_excel_from_zip(path)
				log.info("Arquivo compactado lido: %s (entrada=%s)", path.name, inner)
			except zipfile.BadZipFile:
				df_raw = _read_excel_from_gz(path)
				log.info("Arquivo compactado lido: %s (gzip)", path.name)
			decompressed = True
		else:
			engine = _engine_for_suffix(path.suffix)
			df_raw = pd.read_excel(path, engine=engine)

		if df_raw is None or df_raw.empty:
			log.warning("Planilha vazia: %s", path.name)
			raise ValueError("Planilha vazia")

		payload = df_raw.to_dict(orient="records")
		df_normalized = _normalize_dataframe(df_raw)
		df_prepared = _prepare_raw_df(df_normalized, payload)
		log.info(
			"Planilha carregada: %s registros (descompressao=%s)",
			len(df_prepared),
			"sim" if decompressed else "nao",
		)

		salvar_cache(CACHE_NOME, df_prepared)
		return df_prepared
	except Exception as exc:
		log.error("Falha ao ler exportacao XD: %s", exc)
		cached = carregar_cache(CACHE_NOME)
		if cached is None:
			return pd.DataFrame()
		return cached


if __name__ == "__main__":
	import sys

	if len(sys.argv) < 2:
		log.error("Uso: python scrappers/xd_software.py <arquivo>")
		raise SystemExit(1)

	run(sys.argv[1])
