"""ETL compressor: comprime campos texto pesados de DataFrames limpos.

Responsabilidade unica:
- comprimir campos texto alvo com zlib (nivel 6)
- ignorar payloads menores que 64 bytes
- manter estrutura do DataFrame inalterada
"""

from __future__ import annotations

import logging
import zlib
from typing import Any, Iterable, Sequence

import pandas as pd

log = logging.getLogger("etl.compressor")

COMPRESSION_LEVEL = 6
MIN_COMPRESS_BYTES = 64
TARGET_COLUMNS: tuple[str, ...] = (
	"objeto_contrato",
	"historico_obra",
	"razao_social_fornecedor",
	"descricao_item",
)


def compress(
	df: pd.DataFrame,
	target_columns: Sequence[str] = TARGET_COLUMNS,
	min_compress_bytes: int = MIN_COMPRESS_BYTES,
	compression_level: int = COMPRESSION_LEVEL,
) -> pd.DataFrame:
	"""Comprime campos texto alvo e retorna um novo DataFrame."""
	if df is None or df.empty:
		return pd.DataFrame() if df is None else df.copy()

	compressed = df.copy()
	available_columns = [col for col in target_columns if col in compressed.columns]

	for column in available_columns:
		compressed = compressed.assign(
			**{
				column: compressed[column].apply(
					lambda value: compress_field(
						value,
						min_compress_bytes=min_compress_bytes,
						compression_level=compression_level,
					)
				)
			}
		)

	ratio = benchmark_compression_ratio(
		df_before=df,
		df_after=compressed,
		target_columns=available_columns,
		min_compress_bytes=min_compress_bytes,
	)
	if ratio is not None:
		log.info("Taxa media de compressao nos campos alvo: %.2f%%", ratio)

	return compressed


def compress_field(
	value: Any,
	min_compress_bytes: int = MIN_COMPRESS_BYTES,
	compression_level: int = COMPRESSION_LEVEL,
) -> Any:
	"""Comprime um valor individual se for texto com tamanho suficiente."""
	if _is_missing(value):
		return value

	if _is_binary(value):
		# Evita recompressao acidental de payload ja binario.
		return _to_bytes(value)

	text = str(value)
	raw = text.encode("utf-8")
	if len(raw) < min_compress_bytes:
		return value

	return zlib.compress(raw, level=compression_level)


def decompress_field(value: Any) -> Any:
	"""Descomprime bytes zlib e retorna texto UTF-8."""
	if _is_missing(value):
		return value

	if not _is_binary(value):
		return value

	blob = _to_bytes(value)
	try:
		return zlib.decompress(blob).decode("utf-8")
	except zlib.error:
		# Se nao for zlib valido, preserva o valor original para nao quebrar leitura.
		return value


def benchmark_compression_ratio(
	df_before: pd.DataFrame,
	df_after: pd.DataFrame,
	target_columns: Iterable[str] = TARGET_COLUMNS,
	min_compress_bytes: int = MIN_COMPRESS_BYTES,
) -> float | None:
	"""Calcula taxa media de compressao percentual nos campos alvo.

	Retorna percentual entre 0 e 100. Se nao houver campos elegiveis, retorna None.
	"""
	if df_before is None or df_after is None or df_before.empty:
		return None

	columns = [col for col in target_columns if col in df_before.columns and col in df_after.columns]
	if not columns:
		return None

	total_before = 0
	total_after = 0

	for column in columns:
		for original, current in zip(df_before[column], df_after[column]):
			if _is_missing(original):
				continue

			original_bytes = _value_to_utf8_bytes(original)
			if original_bytes is None or len(original_bytes) < min_compress_bytes:
				continue

			current_bytes = _value_to_bytes_for_size(current)
			if current_bytes is None:
				continue

			total_before += len(original_bytes)
			total_after += len(current_bytes)

	if total_before == 0:
		return None

	saving_ratio = 1 - (total_after / total_before)
	return saving_ratio * 100


def _is_missing(value: Any) -> bool:
	if pd.isna(value):
		return True
	return isinstance(value, str) and value.strip() == ""


def _is_binary(value: Any) -> bool:
	return isinstance(value, (bytes, bytearray, memoryview))


def _to_bytes(value: bytes | bytearray | memoryview) -> bytes:
	if isinstance(value, bytes):
		return value
	if isinstance(value, bytearray):
		return bytes(value)
	return value.tobytes()


def _value_to_utf8_bytes(value: Any) -> bytes | None:
	if _is_missing(value):
		return None
	if _is_binary(value):
		return _to_bytes(value)
	return str(value).encode("utf-8")


def _value_to_bytes_for_size(value: Any) -> bytes | None:
	if _is_missing(value):
		return None
	if _is_binary(value):
		return _to_bytes(value)
	return str(value).encode("utf-8")


__all__ = [
	"COMPRESSION_LEVEL",
	"MIN_COMPRESS_BYTES",
	"TARGET_COLUMNS",
	"benchmark_compression_ratio",
	"compress",
	"compress_field",
	"decompress_field",
]
