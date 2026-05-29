"""Testes do módulo de geocoding (Nominatim). Rede mockada — rodam offline."""

import pandas as pd
import pytest

from etl import geocoding

pytestmark = pytest.mark.unit


def test_dentro_de_macae():
    assert geocoding._dentro_de_macae(-22.37, -41.78) is True
    assert geocoding._dentro_de_macae(-23.55, -46.63) is False  # São Paulo


def test_geocodificar_usa_cache_sem_rede(monkeypatch):
    """Query em cache não deve disparar requisição."""
    def nao_chamar(q):
        raise AssertionError("não deveria consultar a rede")
    monkeypatch.setattr(geocoding, "_consultar_nominatim", nao_chamar)
    cache = {"Lagomar, Macaé, RJ, Brasil": [-22.30, -41.70]}
    r = geocoding.geocodificar(None, "Lagomar", cache)
    assert r == (-22.30, -41.70)


def test_geocodificar_fallback_granularidade(monkeypatch):
    """Endereço completo falha → cai para só bairro."""
    chamadas = []
    def fake(q):
        chamadas.append(q)
        return (-22.36, -41.79) if q.startswith("Aroeira") else None
    monkeypatch.setattr(geocoding, "_consultar_nominatim", fake)
    cache = {}
    r = geocoding.geocodificar("RUA INEXISTENTE", "Aroeira", cache)
    assert r == (-22.36, -41.79)
    assert len(chamadas) == 2  # tentou endereço completo, depois só bairro


def test_geocodificar_cache_negativo(monkeypatch):
    """Resultado nulo é cacheado para não repetir a consulta."""
    chamadas = []
    def fake(q):
        chamadas.append(q)
        return None
    monkeypatch.setattr(geocoding, "_consultar_nominatim", fake)
    cache = {}
    assert geocoding.geocodificar(None, "Inexistente", cache) is None
    # segunda chamada usa cache negativo, não consulta de novo
    geocoding.geocodificar(None, "Inexistente", cache)
    assert len(chamadas) == 1


def test_geocodificar_dataframe_preenche_sem_coords(monkeypatch):
    monkeypatch.setattr(geocoding, "ENABLED", True)
    monkeypatch.setattr(geocoding, "_carregar_cache", lambda: {})
    monkeypatch.setattr(geocoding, "_salvar_cache", lambda c: None)
    monkeypatch.setattr(geocoding, "_consultar_nominatim", lambda q: (-22.36, -41.79))

    df = pd.DataFrame([
        {"latitude": None, "longitude": None, "endereco": None, "bairro": "Aroeira"},
        {"latitude": -22.0, "longitude": -41.0, "endereco": None, "bairro": "Centro"},  # já tem
        {"latitude": None, "longitude": None, "endereco": None, "bairro": None},  # sem local
    ])
    out = geocoding.geocodificar_dataframe(df)
    assert out.iloc[0]["latitude"] == -22.36   # geocodificado
    assert out.iloc[1]["latitude"] == -22.0    # preservado
    assert pd.isna(out.iloc[2]["latitude"])    # sem local, fica nulo


def test_geocodificar_dataframe_desabilitado(monkeypatch):
    """Com GEOCODING_ENABLED=false, não altera nada."""
    monkeypatch.setattr(geocoding, "ENABLED", False)
    df = pd.DataFrame([{"latitude": None, "longitude": None, "bairro": "Aroeira", "endereco": None}])
    out = geocoding.geocodificar_dataframe(df)
    assert pd.isna(out.iloc[0]["latitude"])
