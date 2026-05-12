"""ETL loader: persiste DataFrames no Supabase via upsert em lote.

Responsabilidades:
- conectar no Supabase
- enviar registros em lotes com retry em erros 5xx
- executar upsert com chave de conflito (composta ou simples)
- filtrar colunas para o schema da tabela alvo
- preencher coletado_em em UTC e payload_bruto como JSON string
- isolar falhas por lote sem abortar o pipeline
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, Iterable

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

log = logging.getLogger("etl.loader")

DEFAULT_TABLE = "raw_contratos"
DEFAULT_CONFLICT = ("id_contrato", "fonte")
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
	conflict_column: str | Sequence[str] = DEFAULT_CONFLICT,
	allowed_columns: Sequence[str] | None = None,
	retry_attempts: int = RETRY_ATTEMPTS,
	retry_backoff: float = RETRY_BACKOFF,
	client: Any | None = None,
) -> int:
	"""Grava DataFrame no Supabase via upsert em lote e retorna total gravado."""
	if df is None or df.empty:
		return 0

	if batch_size <= 0:
		raise ValueError("batch_size deve ser maior que zero")

	conflict_cols = _conflict_as_tuple(conflict_column)
	allowed = set(allowed_columns) if allowed_columns is not None else None
	on_conflict = ",".join(conflict_cols)

	client = client or init_client()
	records = _prepare_records(df, allowed_columns=allowed)
	records = _dedup_por_chave(records, conflict_cols)
	total_gravado = 0

	for batch_index, batch in enumerate(_chunked(records, batch_size), start=1):
		try:
			_upsert_batch_with_retry(
				client=client,
				tabela=tabela,
				batch=batch,
				on_conflict=on_conflict,
				retry_attempts=retry_attempts,
				retry_backoff=retry_backoff,
			)
			total_gravado += len(batch)
			log.info(
				"Batch %s gravado em %s (%s registros)",
				batch_index,
				tabela,
				len(batch),
			)
		except Exception as exc:
			ids_falha = _extract_ids(batch, conflict_cols[0])
			log.error(
				"Falha no batch %s em %s (%s registros). Chaves: %s. Erro: %s",
				batch_index,
				tabela,
				len(batch),
				ids_falha,
				exc,
			)

	return total_gravado


def _conflict_as_tuple(value: str | Sequence[str]) -> tuple[str, ...]:
	if isinstance(value, str):
		parts = [p.strip() for p in value.split(",") if p.strip()]
		return tuple(parts) if parts else (value,)
	return tuple(value)


def _prepare_records(
	df: pd.DataFrame,
	allowed_columns: set[str] | None,
) -> list[dict[str, Any]]:
	collected_at = _utc_now_iso()
	raw_records = df.to_dict(orient="records")
	prepared: list[dict[str, Any]] = []
	for record in raw_records:
		row = _normalize_record(record)
		row["coletado_em"] = collected_at
		row["payload_bruto"] = _serialize_payload_bruto(record)
		if allowed_columns is not None:
			row = {k: v for k, v in row.items() if k in allowed_columns}
		prepared.append(row)
	return prepared


def _serialize_payload_bruto(original: dict[str, Any]) -> str:
	"""Serializa o registro original como string JSON estavel."""
	return json.dumps(original, ensure_ascii=False, default=str, sort_keys=False)


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
		return "\\x" + blob.hex()

	if hasattr(value, "item") and not isinstance(value, (str, dict, list, tuple)):
		try:
			return _to_supabase_value(value.item())
		except Exception:
			pass

	if isinstance(value, dict):
		return {k: _to_supabase_value(v) for k, v in value.items()}

	if isinstance(value, list):
		return [_to_supabase_value(v) for v in value]

	if isinstance(value, bool):
		return value

	if isinstance(value, float):
		# Preserva floats inteiros como int para colunas INTEGER do Postgres
		# (e.g. ano="2018.0" — rejeitado por colunas INTEGER).
		if value.is_integer():
			return int(value)
		return value

	if isinstance(value, (str, int)):
		return value

	return str(value)


def _upsert_batch_with_retry(
	client: Any,
	tabela: str,
	batch: Sequence[dict[str, Any]],
	on_conflict: str,
	retry_attempts: int,
	retry_backoff: float,
) -> None:
	last_error: Exception | None = None

	for attempt in range(1, retry_attempts + 1):
		try:
			client.table(tabela).upsert(batch, on_conflict=on_conflict).execute()
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


def _dedup_por_chave(
	records: list[dict[str, Any]],
	conflict_cols: tuple[str, ...],
) -> list[dict[str, Any]]:
	"""Remove duplicatas pela chave de conflito mantendo a ultima ocorrencia."""
	chaves_validas = [c for c in conflict_cols if records and c in records[0]]
	if not chaves_validas:
		return records
	visto: dict[tuple, int] = {}
	for idx, row in enumerate(records):
		chave = tuple(row.get(c) for c in chaves_validas)
		visto[chave] = idx
	indices = sorted(visto.values())
	if len(indices) == len(records):
		return records
	removidos = len(records) - len(indices)
	log.info("Deduplicados %s registros pela chave %s", removidos, chaves_validas)
	return [records[i] for i in indices]


def _chunked(records: Sequence[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
	for start in range(0, len(records), size):
		yield list(records[start : start + size])


def _extract_ids(batch: Sequence[dict[str, Any]], chave_principal: str) -> list[Any]:
	ids = [row.get(chave_principal) for row in batch if row.get(chave_principal) is not None]
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


def colunas_da_tabela(client: Any, tabela: str) -> list[str]:
	"""Descobre as colunas da tabela via select de uma linha (ou estrutura vazia)."""
	try:
		resp = client.table(tabela).select("*").limit(1).execute()
		if resp.data:
			return list(resp.data[0].keys())
	except Exception as exc:
		log.warning("Nao foi possivel descobrir colunas de %s: %s", tabela, exc)
	return []


def registrar_ingestao(
	client: Any,
	fonte: str,
	status: str,
	qtd_registros: int,
	duracao_segundos: float,
	qtd_erros: int = 0,
	mensagem: str | None = None,
) -> None:
	"""Grava log de ingestao na tabela `ingestoes` (best-effort, nao falha o pipeline)."""
	registro = {
		"fonte": fonte,
		"status": status,
		"qtd_registros": qtd_registros,
		"qtd_erros": qtd_erros,
		"duracao_segundos": round(duracao_segundos, 3),
	}
	if mensagem:
		registro["mensagem"] = mensagem
	try:
		client.table("ingestoes").insert(registro).execute()
	except Exception as exc:
		log.warning("Falha ao registrar ingestao para fonte=%s: %s", fonte, exc)


__all__ = [
	"BATCH_SIZE",
	"DEFAULT_CONFLICT",
	"DEFAULT_TABLE",
	"RETRY_ATTEMPTS",
	"RETRY_BACKOFF",
	"colunas_da_tabela",
	"init_client",
	"load",
	"registrar_ingestao",
]
