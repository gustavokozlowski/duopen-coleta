import pandas as pd
import pytest
import requests

from scrappers.federal import sismob

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


def test_get_retorna_json_com_sucesso(monkeypatch):
    called = {}

    def fake_get(url, headers, params, timeout):
        called["url"] = url
        called["headers"] = headers
        called["params"] = params
        called["timeout"] = timeout
        return DummyResponse(status_code=200, payload={"ok": True})

    monkeypatch.setattr(sismob.requests, "get", fake_get)

    out = sismob._get("/obras", {"page": 0})

    assert out == {"ok": True}
    assert called["url"].endswith("/obras")
    assert called["params"] == {"page": 0}
    assert called["timeout"] == sismob.REQUEST_TIMEOUT


def test_get_lanca_http_error(monkeypatch):
    monkeypatch.setattr(
        sismob.requests,
        "get",
        lambda *a, **k: DummyResponse(status_code=500, http_error=_http_error(500)),
    )

    with pytest.raises(requests.exceptions.HTTPError):
        sismob._get("/obras", {})


def test_get_retry_ate_runtime_error_em_timeout(monkeypatch):
    calls = {"n": 0}
    sleeps = []

    def fake_get(*args, **kwargs):
        calls["n"] += 1
        raise requests.exceptions.Timeout("timeout")

    monkeypatch.setattr(sismob.requests, "get", fake_get)
    monkeypatch.setattr(sismob.time, "sleep", lambda s: sleeps.append(s))

    with pytest.raises(RuntimeError):
        sismob._get("/obras", {})

    assert calls["n"] == sismob.RETRY_ATTEMPTS
    assert sleeps == [2.0, 4.0]


def test_listar_obras_paginas_e_params(monkeypatch):
    pages = [
        {
            "content": [{"propostaId": 1}],
            "totalPages": 2,
            "totalElements": 2,
            "last": False,
        },
        {
            "content": [{"propostaId": 2}],
            "totalPages": 2,
            "totalElements": 2,
            "last": True,
        },
    ]
    calls = []

    def fake_get(path, params=None):
        calls.append((path, params))
        return pages.pop(0)

    monkeypatch.setattr(sismob, "_get", fake_get)
    monkeypatch.setattr(sismob.time, "sleep", lambda *_: None)

    out = sismob.listar_obras()

    assert [o["propostaId"] for o in out] == [1, 2]
    assert len(calls) == 2
    assert calls[0][0] == "/obras"
    assert calls[0][1]["page"] == 0
    assert calls[1][1]["page"] == 1
    assert calls[0][1]["municipioIbge"] == sismob.MUNICIPIO_IBGE
    assert calls[0][1]["ufIbge"] == sismob.UF_IBGE


def test_buscar_detalhe_retorna_none_em_404(monkeypatch):
    monkeypatch.setattr(
        sismob,
        "_get",
        lambda *_: (_ for _ in ()).throw(_http_error(404)),
    )

    out = sismob.buscar_detalhe(123)

    assert out is None


def test_buscar_todos_detalhes_mescla_e_fallback(monkeypatch):
    obras = [
        {"propostaId": 10, "situacaoObra": "resumo"},
        {"propostaId": 20, "tipoObra": "UPA"},
        {"sem": "id"},
    ]

    def fake_buscar_detalhe(pid):
        if pid == 10:
            return {"propostaId": 10, "situacaoObra": "detalhe", "extra": "ok"}
        return None

    monkeypatch.setattr(sismob, "buscar_detalhe", fake_buscar_detalhe)
    monkeypatch.setattr(sismob.time, "sleep", lambda *_: None)

    out = sismob.buscar_todos_detalhes(obras)

    assert len(out) == 2
    assert out[0]["situacaoObra"] == "detalhe"
    assert out[0]["extra"] == "ok"
    assert out[1] == {"propostaId": 20, "tipoObra": "UPA"}


def test_run_sucesso(monkeypatch):
    monkeypatch.setattr(sismob, "listar_obras", lambda: [{"propostaId": 1}])
    monkeypatch.setattr(sismob, "buscar_todos_detalhes", lambda obras: obras)

    saved = []
    monkeypatch.setattr(sismob, "_salvar_cache", lambda dados: saved.append(dados))
    monkeypatch.setattr(
        sismob,
        "normalizar",
        lambda registros: pd.DataFrame([{"proposta_id": registros[0]["propostaId"]}]),
    )

    out = sismob.run()

    assert len(out) == 1
    assert out.iloc[0]["proposta_id"] == 1
    assert saved == [[{"propostaId": 1}]]


def test_run_fallback_para_cache(monkeypatch):
    monkeypatch.setattr(
        sismob,
        "listar_obras",
        lambda: (_ for _ in ()).throw(RuntimeError("api indisponivel")),
    )
    monkeypatch.setattr(sismob, "_carregar_cache", lambda: [{"propostaId": 9}])
    monkeypatch.setattr(
        sismob,
        "normalizar",
        lambda registros: pd.DataFrame([{"proposta_id": registros[0]["propostaId"]}]),
    )

    out = sismob.run()

    assert len(out) == 1
    assert out.iloc[0]["proposta_id"] == 9


def test_run_retorna_vazio_quando_cache_tambem_vazio(monkeypatch):
    monkeypatch.setattr(
        sismob,
        "listar_obras",
        lambda: (_ for _ in ()).throw(RuntimeError("api indisponivel")),
    )
    monkeypatch.setattr(sismob, "_carregar_cache", lambda: [])

    out = sismob.run()

    assert out.empty is True