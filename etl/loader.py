"""ETL loader: persiste DataFrames comprimidos no Supabase via upsert em lote.

Responsabilidade unica:
- conectar no Supabase
- enviar registros em lotes
- executar upsert por id_contrato
- registrar coletado_em em UTC
- isolar falhas por lote com retry em erros 5xx
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

log = logging.getLogger("etl.loader")

DEFAULT_TABLE = "raw_contratos"
CONFLICT_COLUMN = "id_contrato"
BATCH_SIZE = int(os.getenv("SUPABASE_BATCH_SIZE", "500"))
RETRY_ATTEMPTS = int(os.getenv("SUPABASE_RETRY_ATTEMPTS", "3"))
RETRY_BACKOFF = float(os.getenv("SUPABASE_RETRY_BACKOFF", "2.0"))


def init_client(supabase_url: str | None = None, supabase_key: str | None = None) -> Any:
	"""Inicializa cliente Supabase via URL/KEY do .env ou parametros explicitos."""
	url = (supabase_url or os.getenv("SUPABASE_URL", "")).strip()
	key = (supabase_key or os.getenv("SUPABASE_KEY", "")).strip()

	if not url or not key:
		raise ValueError("SUPABASE_URL e SUPABASE_KEY sao obrigatorios para o loader")

	if "anon" in key.lower():
		raise ValueError("SUPABASE_KEY anon nao e permitida. Use service_role para escrita no Raw")

	return create_client(url, key)


def load(
	df: pd.DataFrame,
	tabela: str = DEFAULT_TABLE,
	batch_size: int = BATCH_SIZE,
	conflict_column: str = CONFLICT_COLUMN,
	retry_attempts: int = RETRY_ATTEMPTS,
	retry_backoff: float = RETRY_BACKOFF,
	client: Any | None = None,
) -> int:
	"""Grava DataFrame no Supabase via upsert em lote e retorna total gravado."""
	if df is None or df.empty:
		return 0

	if batch_size <= 0:
		raise ValueError("batch_size deve ser maior que zero")

	client = client or init_client()
	records = _prepare_records(df)
	total_gravado = 0

	for batch_index, batch in enumerate(_chunked(records, batch_size), start=1):
		try:
			_upsert_batch_with_retry(
				client=client,
				tabela=tabela,
				batch=batch,
				conflict_column=conflict_column,
				retry_attempts=retry_attempts,
				retry_backoff=retry_backoff,
			)
			total_gravado += len(batch)
			log.info(
				"Batch %s gravado com sucesso (%s registros)",
				batch_index,
				len(batch),
			)
		except Exception as exc:
			ids_falha = _extract_ids(batch, conflict_column)
			log.error(
				"Falha no batch %s (%s registros). IDs com problema: %s. Erro: %s",
				batch_index,
				len(batch),
				ids_falha,
				exc,
			)

	return total_gravado


def _prepare_records(df: pd.DataFrame) -> list[dict[str, Any]]:
	collected_at = _utc_now_iso()
	prepared = df.copy().assign(coletado_em=collected_at)
	raw_records = prepared.to_dict(orient="records")
	return [_normalize_record(record) for record in raw_records]


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
	return {key: _to_supabase_value(value) for key, value in record.items()}


def _to_supabase_value(value: Any) -> Any:
	if _is_missing(value):
		return None

	if isinstance(value, pd.Timestamp):
		return _to_iso_utc_datetime(value.to_pydatetime())

	if isinstance(value, datetime):
		return _to_iso_utc_datetime(value)

	if isinstance(value, (bytes, bytearray, memoryview)):
		blob = _to_bytes(value)
		# Formato aceito pelo PostgreSQL para bytea via texto hex.
		return "\\x" + blob.hex()

	if hasattr(value, "item") and not isinstance(value, (str, dict, list, tuple)):
		try:
			return _to_supabase_value(value.item())
		except Exception:
			pass

	if isinstance(value, (str, int, float, bool, dict, list)):
		return value

	return str(value)


def _upsert_batch_with_retry(
	client: Any,
	tabela: str,
	batch: Sequence[dict[str, Any]],
	conflict_column: str,
	retry_attempts: int,
	retry_backoff: float,
) -> None:
	last_error: Exception | None = None

	for attempt in range(1, retry_attempts + 1):
		try:
			client.table(tabela).upsert(batch, on_conflict=conflict_column).execute()
			return
		except Exception as exc:
			last_error = exc
			if not _is_retryable_error(exc) or attempt >= retry_attempts:
				raise

			wait_seconds = retry_backoff * (2 ** (attempt - 1))
			log.warning(
				"Erro temporario no upsert (tentativa %s/%s): %s. Retry em %.1fs.",
				attempt,
				retry_attempts,
				exc,
				wait_seconds,
			)
			time.sleep(wait_seconds)

	if last_error is not None:
		raise last_error


def _is_retryable_error(exc: Exception) -> bool:
	status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
	if isinstance(status, int) and 500 <= status < 600:
		return True

	response = getattr(exc, "response", None)
	response_status = getattr(response, "status_code", None)
	if isinstance(response_status, int) and 500 <= response_status < 600:
		return True

	message = str(exc)
	if re.search(r"\b5\d\d\b", message):
		return True

	message_lower = message.lower()
	if "timeout" in message_lower or "temporar" in message_lower or "connection" in message_lower:
		return True

	return False


def _chunked(records: Sequence[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
	for start in range(0, len(records), size):
		yield list(records[start : start + size])


def _extract_ids(batch: Sequence[dict[str, Any]], conflict_column: str) -> list[Any]:
	ids = [row.get(conflict_column) for row in batch if row.get(conflict_column) is not None]
	if len(ids) > 20:
		return ids[:20] + ["..."]
	return ids


def _is_missing(value: Any) -> bool:
	if value is None:
		return True
	try:
		if pd.isna(value):
			return True
	except Exception:
		pass
	return isinstance(value, str) and value.strip() == ""


def _to_bytes(value: bytes | bytearray | memoryview) -> bytes:
	if isinstance(value, bytes):
		return value
	if isinstance(value, bytearray):
		return bytes(value)
	return value.tobytes()


def _to_iso_utc_datetime(value: datetime) -> str:
	if value.tzinfo is None:
		value = value.replace(tzinfo=timezone.utc)
	else:
		value = value.astimezone(timezone.utc)
	return value.isoformat()


def _utc_now_iso() -> str:
	return datetime.now(timezone.utc).isoformat()


__all__ = [
	"BATCH_SIZE",
	"CONFLICT_COLUMN",
	"DEFAULT_TABLE",
	"RETRY_ATTEMPTS",
	"RETRY_BACKOFF",
	"init_client",
	"load",
]
