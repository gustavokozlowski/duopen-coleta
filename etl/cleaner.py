"""ETL cleaner: padroniza DataFrames brutos para o schema minimo do projeto.

Este modulo deve ser agnostico a fonte. Ele recebe um DataFrame bruto e
devolve um DataFrame limpo, padronizado e validado sem acoplar com compressao
ou persistencia.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Iterable

import pandas as pd

log = logging.getLogger("etl.cleaner")

REQUIRED_SCHEMA_MIN: tuple[str, ...] = (
	"id_contrato",
	"municipio",
	"fonte",
	"coletado_em",
)

DEFAULT_VALUES: dict[str, Any] = {
	"municipio": "Macae",
	"fonte": "desconhecida",
}

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

MONETARY_COLUMN_HINTS: tuple[str, ...] = (
	"valor",
	"montante",
	"preco",
	"quantia",
	"orcamento",
	"total",
)


def clean(df: pd.DataFrame) -> pd.DataFrame:
	"""Executa o pipeline completo de limpeza em ordem deterministica."""
	cleaned = pd.DataFrame() if df is None else df.copy()
	cleaned = normalize_dates(cleaned)
	cleaned = normalize_cnpj(cleaned)
	cleaned = remove_duplicates(cleaned)
	cleaned = normalize_monetary(cleaned)
	cleaned = fill_defaults(cleaned)
	cleaned = validate_schema(cleaned)
	return cleaned


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
	"""Converte colunas de data para datetime64[UTC]."""
	cleaned = df.copy()
	for column in cleaned.columns:
		if not _is_date_column(column):
			continue

		parsed = cleaned[column].apply(_parse_datetime_utc)
		cleaned = cleaned.assign(
			**{column: pd.to_datetime(parsed, utc=True, errors="coerce")}
		)

	return cleaned


def normalize_cnpj(df: pd.DataFrame) -> pd.DataFrame:
	"""Normaliza e valida CNPJ em todas as colunas com indicio de CNPJ."""
	cleaned = df.copy()
	for column in cleaned.columns:
		if "cnpj" not in column.lower():
			continue
		cleaned = cleaned.assign(**{column: cleaned[column].apply(_normalize_cnpj_value)})

	return cleaned


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
	"""Remove linhas duplicadas e aplica unicidade por id_contrato quando existir."""
	if df.empty:
		return df.copy()

	cleaned = df.drop_duplicates(keep="first")
	if "id_contrato" in cleaned.columns:
		cleaned = cleaned.drop_duplicates(subset=["id_contrato"], keep="first")

	return cleaned.reset_index(drop=True)


def normalize_monetary(df: pd.DataFrame) -> pd.DataFrame:
	"""Converte colunas monetarias para float com tolerancia a formatos BR/US."""
	cleaned = df.copy()
	for column in cleaned.columns:
		if not _is_monetary_column(column):
			continue
		cleaned = cleaned.assign(**{column: cleaned[column].apply(_parse_money)})

	return cleaned


def fill_defaults(df: pd.DataFrame) -> pd.DataFrame:
	"""Preenche campos obrigatorios ausentes ou nulos com valores padrao."""
	cleaned = df.copy()

	for column, default_value in DEFAULT_VALUES.items():
		if column not in cleaned.columns:
			cleaned = cleaned.assign(**{column: default_value})
			continue
		cleaned = cleaned.assign(
			**{
				column: cleaned[column].apply(
					lambda value: default_value if _is_missing(value) else value
				)
			}
		)

	now_utc = pd.Timestamp.now(tz="UTC")
	if "coletado_em" not in cleaned.columns:
		cleaned = cleaned.assign(coletado_em=now_utc)
	else:
		parsed = cleaned["coletado_em"].apply(_parse_datetime_utc)
		cleaned = cleaned.assign(
			coletado_em=pd.to_datetime(parsed, utc=True, errors="coerce").fillna(now_utc)
		)

	return cleaned


def validate_schema(
	df: pd.DataFrame,
	required_columns: Iterable[str] = REQUIRED_SCHEMA_MIN,
) -> pd.DataFrame:
	"""Valida schema minimo e registra warning para colunas criticas ausentes."""
	missing = [column for column in required_columns if column not in df.columns]
	if missing:
		log.warning(
			"Schema minimo incompleto. Colunas ausentes: %s",
			", ".join(missing),
		)
	return df


def _is_missing(value: Any) -> bool:
	if pd.isna(value):
		return True
	return isinstance(value, str) and value.strip() == ""


def _is_date_column(column_name: str) -> bool:
	normalized = column_name.lower()
	return any(hint in normalized for hint in DATE_COLUMN_HINTS)


def _is_monetary_column(column_name: str) -> bool:
	normalized = column_name.lower()
	return any(hint in normalized for hint in MONETARY_COLUMN_HINTS)


def _parse_datetime_utc(value: Any) -> pd.Timestamp | None:
	if _is_missing(value):
		return None

	if isinstance(value, pd.Timestamp):
		if value.tzinfo is None:
			return value.tz_localize("UTC")
		return value.tz_convert("UTC")

	if isinstance(value, datetime):
		if value.tzinfo is None:
			value = value.replace(tzinfo=timezone.utc)
		else:
			value = value.astimezone(timezone.utc)
		return pd.Timestamp(value)

	if isinstance(value, (int, float)):
		return _parse_epoch(value)

	text = str(value).strip()
	if not text:
		return None

	if text.isdigit():
		return _parse_epoch(text)

	parsed = _parse_known_datetime_formats(text)
	if parsed is not None:
		return parsed

	if re.match(r"^\d{1,2}/\d{1,2}/\d{4}(?:\s+.*)?$", text):
		# Evita aceitar MM/DD/YYYY como fallback automatico.
		return None

	fallback = pd.to_datetime(text, utc=True, errors="coerce")
	return None if pd.isna(fallback) else fallback


def _parse_epoch(value: int | float | str) -> pd.Timestamp | None:
	try:
		ts = float(value)
	except (TypeError, ValueError):
		return None

	if ts > 1e11:
		ts = ts / 1000.0

	parsed = pd.to_datetime(ts, unit="s", utc=True, errors="coerce")
	return None if pd.isna(parsed) else parsed


def _parse_known_datetime_formats(text: str) -> pd.Timestamp | None:
	# dd/mm/yyyy [HH:MM[:SS]]
	match_dmy_slash = re.match(
		r"^(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$",
		text,
	)
	if match_dmy_slash:
		day, month, year, hour, minute, second = match_dmy_slash.groups()
		try:
			dt = datetime(
				int(year),
				int(month),
				int(day),
				int(hour or 0),
				int(minute or 0),
				int(second or 0),
				tzinfo=timezone.utc,
			)
			return pd.Timestamp(dt)
		except ValueError:
			return None

	# yyyy-mm-dd [HH:MM[:SS]]
	match_ymd_hyphen = re.match(
		r"^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$",
		text,
	)
	if match_ymd_hyphen:
		year, month, day, hour, minute, second = match_ymd_hyphen.groups()
		try:
			dt = datetime(
				int(year),
				int(month),
				int(day),
				int(hour or 0),
				int(minute or 0),
				int(second or 0),
				tzinfo=timezone.utc,
			)
			return pd.Timestamp(dt)
		except ValueError:
			return None

	# dd-mm-yyyy [HH:MM[:SS]]
	match_dmy_hyphen = re.match(
		r"^(\d{1,2})-(\d{1,2})-(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$",
		text,
	)
	if match_dmy_hyphen:
		day, month, year, hour, minute, second = match_dmy_hyphen.groups()
		try:
			dt = datetime(
				int(year),
				int(month),
				int(day),
				int(hour or 0),
				int(minute or 0),
				int(second or 0),
				tzinfo=timezone.utc,
			)
			return pd.Timestamp(dt)
		except ValueError:
			return None

	return None


def _normalize_cnpj_value(value: Any) -> str | None:
	if _is_missing(value):
		return None

	digits = re.sub(r"\D", "", str(value))
	if len(digits) != 14:
		return None

	if digits == digits[0] * 14:
		return None

	if not _is_valid_cnpj(digits):
		return None

	return digits


def _is_valid_cnpj(cnpj: str) -> bool:
	def calc_digit(numbers: str, weights: list[int]) -> str:
		total = sum(int(num) * weight for num, weight in zip(numbers, weights))
		remainder = total % 11
		return "0" if remainder < 2 else str(11 - remainder)

	first_weights = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
	second_weights = [6] + first_weights

	first_digit = calc_digit(cnpj[:12], first_weights)
	second_digit = calc_digit(cnpj[:12] + first_digit, second_weights)
	return cnpj[-2:] == first_digit + second_digit


def _parse_money(value: Any) -> float | None:
	if _is_missing(value):
		return None

	if isinstance(value, (int, float)):
		return float(value)

	text = str(value).strip()
	if not text:
		return None

	text = text.replace("R$", "").replace("$", "")
	text = re.sub(r"\s+", "", text)
	text = re.sub(r"[^\d,.-]", "", text)
	if text in {"", "-", ".", ",", "-.", "-,"}:
		return None

	if "," in text and "." in text:
		# Escolhe o separador decimal pela ultima ocorrencia.
		if text.rfind(",") > text.rfind("."):
			text = text.replace(".", "").replace(",", ".")
		else:
			text = text.replace(",", "")
	elif "," in text:
		text = text.replace(".", "").replace(",", ".")
	else:
		if text.count(".") > 1:
			text = text.replace(".", "")
		elif text.count(".") == 1:
			integer, decimal = text.split(".")
			if len(decimal) == 3:
				text = integer + decimal

	try:
		return float(text)
	except ValueError:
		return None


__all__ = [
	"DEFAULT_VALUES",
	"REQUIRED_SCHEMA_MIN",
	"clean",
	"fill_defaults",
	"normalize_cnpj",
	"normalize_dates",
	"normalize_monetary",
	"remove_duplicates",
	"validate_schema",
]
