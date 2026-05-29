"""Geocoding de endereços de Macaé via Nominatim (OpenStreetMap).

Preenche latitude/longitude de obras derivadas de contratos, que não têm
coordenadas na fonte mas têm bairro/endereço extraídos do objeto.

Cuidados de uso (política do Nominatim):
- Máximo de 1 requisição por segundo (respeitado por RATE_LIMIT_S).
- User-Agent identificável obrigatório.
- Cache local persistente para nunca geocodificar a mesma query duas vezes.

Resiliência: qualquer falha (rede, timeout, sem resultado) retorna None — o
geocoding é um enriquecimento opcional, nunca bloqueia o ETL.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import requests

log = logging.getLogger("etl.geocoding")

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "duopen-coleta/1.0 (hackathon DUOPEN 2026)"
RATE_LIMIT_S = float(os.getenv("GEOCODING_RATE_LIMIT_S", "1.1"))
REQUEST_TIMEOUT = int(os.getenv("GEOCODING_TIMEOUT", "20"))
ENABLED = os.getenv("GEOCODING_ENABLED", "true").lower() == "true"
# Teto de geocodificações novas por execução — evita estourar o tempo do CI
MAX_NOVOS = int(os.getenv("GEOCODING_MAX_NOVOS", "300"))

CACHE_DIR = Path(__file__).resolve().parents[1] / "cache"
CACHE_FILE = CACHE_DIR / "geocode_cache.json"

# Bounding box aproximada de Macaé/RJ — resultados fora são descartados
MACAE_BBOX = {"lat_min": -22.65, "lat_max": -22.05, "lon_min": -42.15, "lon_max": -41.55}

_ultimo_request = 0.0


def _dentro_de_macae(lat: float, lon: float) -> bool:
    return (
        MACAE_BBOX["lat_min"] <= lat <= MACAE_BBOX["lat_max"]
        and MACAE_BBOX["lon_min"] <= lon <= MACAE_BBOX["lon_max"]
    )


def _carregar_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _salvar_cache(cache: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")


def _consultar_nominatim(query: str) -> Optional[tuple[float, float]]:
    """Uma consulta ao Nominatim respeitando o rate limit. Retorna (lat, lon) ou None."""
    global _ultimo_request
    espera = RATE_LIMIT_S - (time.monotonic() - _ultimo_request)
    if espera > 0:
        time.sleep(espera)
    _ultimo_request = time.monotonic()

    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={"q": query, "format": "json", "limit": 1, "countrycodes": "br"},
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        dados = resp.json()
        if not dados:
            return None
        lat, lon = float(dados[0]["lat"]), float(dados[0]["lon"])
        if not _dentro_de_macae(lat, lon):
            return None
        return lat, lon
    except Exception as exc:
        log.debug("Nominatim falhou para %r: %s", query, exc)
        return None


def geocodificar(
    endereco: Optional[str],
    bairro: Optional[str],
    cache: dict,
    municipio: str = "Macaé",
    uf: str = "RJ",
) -> Optional[tuple[float, float]]:
    """
    Geocodifica com fallback de granularidade: endereço completo → só bairro.
    Usa e atualiza o dict `cache` (persistido pelo chamador). Retorna (lat, lon) ou None.
    """
    # Monta candidatos do mais específico ao menos específico
    candidatos = []
    if endereco:
        partes = [endereco]
        if bairro:
            partes.append(bairro)
        candidatos.append(", ".join(partes + [municipio, uf, "Brasil"]))
    if bairro:
        candidatos.append(", ".join([bairro, municipio, uf, "Brasil"]))

    for query in candidatos:
        if query in cache:
            r = cache[query]
            if r is not None:
                return tuple(r)
            continue  # cache negativo: tenta o próximo candidato menos específico
        coords = _consultar_nominatim(query)
        cache[query] = list(coords) if coords else None
        if coords:
            return coords
    return None


def geocodificar_dataframe(df, col_lat="latitude", col_lon="longitude",
                           col_end="endereco", col_bairro="bairro"):
    """
    Preenche lat/long de linhas sem coordenadas que tenham endereço ou bairro.
    Modifica e retorna o DataFrame. Faz no máximo MAX_NOVOS geocodificações novas.
    """
    import pandas as pd

    if not ENABLED or df.empty:
        return df
    if col_lat not in df.columns or col_lon not in df.columns:
        return df

    cache = _carregar_cache()
    lat = pd.to_numeric(df[col_lat], errors="coerce")
    lon = pd.to_numeric(df[col_lon], errors="coerce")
    sem_coord = lat.isna() | lon.isna()
    tem_local = df.get(col_end, pd.Series("", index=df.index)).notna() | \
        df.get(col_bairro, pd.Series("", index=df.index)).notna()
    alvo = df[sem_coord & tem_local].index

    novos = preenchidos = 0
    for idx in alvo:
        end = df.at[idx, col_end] if col_end in df.columns else None
        bai = df.at[idx, col_bairro] if col_bairro in df.columns else None
        # Conta apenas geocodificações que vão à rede (não estão em cache)
        chave_nova = not any(
            q in cache for q in [str(end), str(bai)] if q
        )
        if chave_nova and novos >= MAX_NOVOS:
            break
        coords = geocodificar(end, bai, cache)
        if chave_nova:
            novos += 1
        if coords:
            df.at[idx, col_lat] = coords[0]
            df.at[idx, col_lon] = coords[1]
            preenchidos += 1

    _salvar_cache(cache)
    if preenchidos:
        log.info("geocoding: %d obras geolocalizadas (%d consultas novas)", preenchidos, novos)
    return df
