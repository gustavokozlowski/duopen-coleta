# ── Configuração ──────────────────────────────────────────────────────────────
import os
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Union

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("etl.fallback")


def _configure_logging() -> None:
	level_name = os.getenv("LOG_LEVEL", "INFO").upper()
	level = logging.getLevelName(level_name)
	if isinstance(level, int):
		log.setLevel(level)
	else:
		log.setLevel(logging.INFO)


_configure_logging()

UTC = timezone.utc


# ── Utilitários internos ──────────────────────────────────────────────────────
def _get_cache_dir() -> Path:
	return Path(os.getenv("CACHE_DIR", "cache"))


def _get_cache_max_dias() -> int:
	try:
		return int(os.getenv("CACHE_MAX_DIAS", "1"))
	except ValueError:
		log.warning("CACHE_MAX_DIAS invalido; usando 1")
		return 1


def _cache_path(nome: str) -> Path:
	return _get_cache_dir() / f"{nome}.json"


def _normalize_records(dados: Union[pd.DataFrame, dict, list]) -> list:
	if isinstance(dados, pd.DataFrame):
		return dados.to_dict(orient="records")
	if isinstance(dados, dict):
		return [dados]
	if isinstance(dados, list):
		return dados
	return [dados]


def _parse_iso_datetime(valor: object) -> Optional[datetime]:
	if not isinstance(valor, str) or not valor:
		return None
	try:
		dt = datetime.fromisoformat(valor)
	except ValueError:
		return None
	if dt.tzinfo is None:
		return dt.replace(tzinfo=UTC)
	return dt.astimezone(UTC)


def _age_hours(salvo_em: Optional[datetime]) -> Optional[float]:
	if salvo_em is None:
		return None
	delta = datetime.now(UTC) - salvo_em
	return delta.total_seconds() / 3600.0


def _relative_path(path: Path) -> str:
	try:
		return str(path.relative_to(Path.cwd()))
	except ValueError:
		return str(path)


# ── salvar_cache ──────────────────────────────────────────────────────────────
def salvar_cache(
	nome: str,
	dados: Union[pd.DataFrame, dict, list],
) -> bool:
	try:
		cache_dir = _get_cache_dir()
		cache_dir.mkdir(parents=True, exist_ok=True)

		registros = _normalize_records(dados)
		if len(registros) == 0:
			log.warning("Cache vazio: %s.json", nome)

		payload = {
			"metadata": {
				"nome": nome,
				"salvo_em": datetime.now(UTC).isoformat(),
				"total_registros": len(registros),
				"versao": "1.0",
			},
			"dados": registros,
		}

		texto = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
		caminho = cache_dir / f"{nome}.json"
		caminho.write_text(texto, encoding="utf-8")

		log.info("Cache salvo: %s.json (%s registros)", nome, len(registros))
		return True
	except Exception as exc:
		log.error("Falha ao salvar cache %s.json: %s", nome, exc)
		return False


# ── carregar_cache ────────────────────────────────────────────────────────────
def carregar_cache(nome: str) -> Optional[pd.DataFrame]:
	caminho = _cache_path(nome)
	if not caminho.exists():
		return None

	try:
		conteudo = caminho.read_text(encoding="utf-8")
		payload = json.loads(conteudo)
		if not isinstance(payload, dict) or "metadata" not in payload or "dados" not in payload:
			log.error("Cache invalido (estrutura): %s.json", nome)
			return None

		metadata = payload.get("metadata")
		dados = payload.get("dados")
		if not isinstance(metadata, dict) or not isinstance(dados, list):
			log.error("Cache invalido (tipos): %s.json", nome)
			return None

		salvo_em = _parse_iso_datetime(metadata.get("salvo_em"))
		if salvo_em is None:
			log.error("Cache invalido (salvo_em): %s.json", nome)
			return None

		idade_horas = _age_hours(salvo_em)
		max_dias = _get_cache_max_dias()
		if idade_horas is not None and idade_horas > max_dias * 24:
			log.warning("Cache desatualizado: %s.json (%.1fh)", nome, idade_horas)

		log.warning(
			"Usando cache local: %s.json (%s registros, %.1fh atras)",
			nome,
			len(dados),
			idade_horas or 0.0,
		)

		return pd.DataFrame(dados)
	except Exception as exc:
		log.error("Falha ao carregar cache %s.json: %s", nome, exc)
		return None


# ── cache_valido ──────────────────────────────────────────────────────────────
def cache_valido(nome: str, max_dias: int = 1) -> bool:
	caminho = _cache_path(nome)
	if not caminho.exists():
		return False

	try:
		conteudo = caminho.read_text(encoding="utf-8")
		payload = json.loads(conteudo)
		metadata = payload.get("metadata") if isinstance(payload, dict) else None
		if not isinstance(metadata, dict):
			log.debug("Cache %s.json invalido (metadata ausente)", nome)
			return False

		salvo_em = _parse_iso_datetime(metadata.get("salvo_em"))
		if salvo_em is None:
			log.debug("Cache %s.json invalido (salvo_em ausente)", nome)
			return False

		idade = datetime.now(UTC) - salvo_em
		valido = idade <= timedelta(days=max_dias)
		idade_horas = idade.total_seconds() / 3600.0
		if not valido:
			log.debug("Cache %s.json expirado (%.1fh)", nome, idade_horas)
		else:
			log.debug("Cache %s.json valido (%.1fh)", nome, idade_horas)

		return valido
	except Exception as exc:
		log.error("Falha ao validar cache %s.json: %s", nome, exc)
		return False


# ── listar_caches ─────────────────────────────────────────────────────────────
def listar_caches() -> list[dict]:
	cache_dir = _get_cache_dir()
	if not cache_dir.exists() or not cache_dir.is_dir():
		return []

	arquivos = list(cache_dir.glob("*.json"))
	if not arquivos:
		return []

	resultado: list[dict] = []
	max_dias = _get_cache_max_dias()
	agora = datetime.now(UTC)

	for caminho in arquivos:
		try:
			conteudo = caminho.read_text(encoding="utf-8")
			payload = json.loads(conteudo)
			if not isinstance(payload, dict):
				log.error("Cache invalido (estrutura): %s", caminho.name)
				continue

			metadata = payload.get("metadata")
			dados = payload.get("dados", [])
			if not isinstance(metadata, dict) or not isinstance(dados, list):
				log.error("Cache invalido (tipos): %s", caminho.name)
				continue

			salvo_em = _parse_iso_datetime(metadata.get("salvo_em"))
			idade_horas = None
			valido = False
			if salvo_em is not None:
				idade = agora - salvo_em
				idade_horas = idade.total_seconds() / 3600.0
				valido = idade <= timedelta(days=max_dias)

			registros = metadata.get("total_registros")
			if not isinstance(registros, int):
				registros = len(dados)

			tamanho_kb = caminho.stat().st_size / 1024.0
			resultado.append(
				{
					"nome": caminho.stem,
					"arquivo": _relative_path(caminho),
					"tamanho_kb": round(tamanho_kb, 1),
					"salvo_em": metadata.get("salvo_em"),
					"idade_horas": round(idade_horas or 0.0, 1),
					"registros": registros,
					"valido": valido,
				}
			)
		except Exception as exc:
			log.error("Falha ao listar cache %s: %s", caminho.name, exc)

	return resultado


if __name__ == "__main__":
	"""Diagnostico do estado atual do cache - util para debugging."""
	caches = listar_caches()

	if not caches:
		print("Nenhum cache encontrado.")
	else:
		print(f"\n── Estado do cache ({len(caches)} arquivos) ──\n")
		for c in sorted(caches, key=lambda x: x["idade_horas"]):
			status = "✓" if c["valido"] else "⚠ desatualizado"
			print(
				f"  {status:20} {c['nome']:35} "
				f"{c['registros']:>6} registros  "
				f"{c['idade_horas']:>6.1f}h  "
				f"{c['tamanho_kb']:>8.1f} KB"
			)
