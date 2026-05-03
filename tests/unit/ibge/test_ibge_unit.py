import json

import pandas as pd
import pytest
import requests

from scrappers.ibge import ibge

pytestmark = pytest.mark.unit


class DummyResponse:
    def __init__(self, status_code=200, payload=None, http_error=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self._http_error = http_error

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self._http_error is not None:
            raise self._http_error


def _http_error(status_code: int) -> requests.exceptions.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    return requests.exceptions.HTTPError(f"HTTP {status_code}", response=response)


def test_get_timeout_retorna_runtime_error(monkeypatch):
    calls = {"n": 0}
    sleeps = []

    def fake_get(*_args, **_kwargs):
        calls["n"] += 1
        raise requests.exceptions.Timeout("timeout")

    monkeypatch.setattr(ibge.requests, "get", fake_get)
    monkeypatch.setattr(ibge.time, "sleep", lambda s: sleeps.append(s))

    with pytest.raises(RuntimeError):
        ibge._get("https://servicodados.ibge.gov.br/api/v1/localidades/municipios/3302403")

    assert calls["n"] == ibge.RETRY_ATTEMPTS
    assert sleeps == [2.0, 4.0]


def test_get_lanca_http_error(monkeypatch):
    monkeypatch.setattr(
        ibge.requests,
        "get",
        lambda *a, **k: DummyResponse(status_code=500, http_error=_http_error(500)),
    )

    with pytest.raises(requests.exceptions.HTTPError):
        ibge._get("https://servicodados.ibge.gov.br/api/v1/localidades/municipios/3302403")


def test_fetch_localidade_schema_invalido(monkeypatch):
    monkeypatch.setattr(ibge, "_get", lambda *_a, **_k: DummyResponse(payload={}))

    out = ibge.fetch_localidade()

    assert out["municipio_id"] is None
    assert out["municipio_nome"] is None
    assert json.loads(out["payload_bruto"]) == {}


def test_fetch_geojson_schema_invalido(monkeypatch):
    monkeypatch.setattr(ibge, "_get", lambda *_a, **_k: DummyResponse(payload={}))

    out = ibge.fetch_geojson()

    assert out == {}


def test_fetch_sidra_resposta_vazia(monkeypatch):
    monkeypatch.setattr(ibge, "_get", lambda *_a, **_k: DummyResponse(payload=[]))

    out = ibge.fetch_sidra("9514", [93], "2022")

    assert out[93] is None


def test_fetch_sidra_timeout_retorna_none(monkeypatch):
    monkeypatch.setattr(ibge, "_get", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")))

    out = ibge.fetch_sidra("9514", [93], "2022")

    assert out[93] is None


def test_run_fallback_para_cache(monkeypatch):
    monkeypatch.setattr(ibge, "fetch_localidade", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(ibge, "_carregar_cache", lambda: {"metadados": {}, "geojson": {}})

    out = ibge.run()

    assert out == {"metadados": {}, "geojson": {}}


def test_run_retorna_vazio_quando_cache_vazio(monkeypatch):
    monkeypatch.setattr(ibge, "fetch_localidade", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(ibge, "_carregar_cache", lambda: None)

    out = ibge.run()

    assert out == {}


def test_fetch_todos_sidra_consolida(monkeypatch):
    monkeypatch.setattr(ibge, "TABELAS_SIDRA", {
        "9514": {"descricao": "Pop", "variaveis": [93], "periodo": "2022"},
    })
    monkeypatch.setattr(ibge, "fetch_sidra", lambda *a, **k: {93: "10"})
    monkeypatch.setattr(ibge.time, "sleep", lambda *_: None)

    out = ibge.fetch_todos_sidra()

    assert out["9514"]["valores"][93] == "10"


def test_normalizar_converte_indicadores():
    localidade = {
        "municipio_id": "3302403",
        "municipio_nome": "Macae",
        "uf_sigla": "RJ",
        "uf_nome": "Rio de Janeiro",
        "mesorregiao_nome": "Norte Fluminense",
        "microrregiao_nome": "Macae",
        "regiao_imediata_nome": "Macae",
        "regiao_intermediaria_nome": "Campos",
        "payload_bruto": "{}",
    }
    geojson = {"type": "FeatureCollection", "features": []}
    sidra = {
        "9514": {"valores": {93: "100"}},
        "6579": {"valores": {9324: "110"}},
        "4714": {"valores": {614: "50,5", 616: "2"}},
        "5938": {"valores": {37: "1000"}},
        "7735": {"valores": {30255: "0.7"}},
    }

    out = ibge.normalizar(localidade, geojson, sidra)

    meta = out["metadados"]
    assert meta["municipio_id"] == "3302403"
    assert meta["populacao_censo_2022"] == 100
    assert meta["area_territorial_km2"] == 50.5
    assert meta["idhm"] == 0.7
