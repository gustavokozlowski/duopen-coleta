import json

import pandas as pd
import pytest
import requests

from scrappers.tce import tce_rj

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


def test_headers_sem_token(monkeypatch):
    monkeypatch.setattr(tce_rj, "TOKEN", "")

    headers = tce_rj._headers()

    assert headers["Accept"] == "application/json"
    assert "Authorization" not in headers


def test_headers_com_token(monkeypatch):
    monkeypatch.setattr(tce_rj, "TOKEN", "abc123")

    headers = tce_rj._headers()

    assert headers["Authorization"] == "Bearer abc123"


def test_get_retry_timeout_ate_runtime_error(monkeypatch):
    calls = {"n": 0}
    sleeps = []

    def fake_get(*args, **kwargs):
        calls["n"] += 1
        raise requests.exceptions.Timeout("timeout")

    monkeypatch.setattr(tce_rj.requests, "get", fake_get)
    monkeypatch.setattr(tce_rj.time, "sleep", lambda s: sleeps.append(s))

    with pytest.raises(RuntimeError):
        tce_rj._get("contratos_municipio", {"municipio": "macae"})

    assert calls["n"] == tce_rj.RETRY_ATTEMPTS
    assert sleeps == [2.0, 4.0]


def test_get_retry_em_429_e_sucesso(monkeypatch):
    responses = [
        DummyResponse(status_code=429, payload={"erro": "rate"}),
        DummyResponse(status_code=200, payload={"ok": True}),
    ]
    sleep_calls = []

    def fake_get(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(tce_rj.requests, "get", fake_get)
    monkeypatch.setattr(tce_rj.time, "sleep", lambda s: sleep_calls.append(s))

    out = tce_rj._get("contratos_municipio", {"municipio": "macae"})

    assert out == {"ok": True}
    assert sleep_calls == [2.0]


def test_get_lanca_http_error(monkeypatch):
    monkeypatch.setattr(
        tce_rj.requests,
        "get",
        lambda *a, **k: DummyResponse(status_code=500, http_error=_http_error(500)),
    )

    with pytest.raises(requests.exceptions.HTTPError):
        tce_rj._get("contratos_municipio", {})


def test_extract_records_dict_com_chave_preferida():
    payload = {"Contratos": [{"id": 1}, {"id": 2}], "Count": 2}

    out = tce_rj._extract_records(payload, preferred_keys=("Contratos",))

    assert [item["id"] for item in out] == [1, 2]


def test_extract_records_lista_e_chave_generica():
    out_list = tce_rj._extract_records([{"id": 10}, {"id": 20}])
    out_dict = tce_rj._extract_records({"items": [{"id": 30}]})

    assert [item["id"] for item in out_list] == [10, 20]
    assert [item["id"] for item in out_dict] == [30]


def test_fetch_paginated_agrega_paginas(monkeypatch):
    monkeypatch.setattr(tce_rj, "PAGE_SIZE", 2)
    monkeypatch.setattr(tce_rj, "MAX_PAGES", 4)

    pages = [
        {"Contratos": [{"Ente": "MACAE", "NumeroContrato": "1"}, {"Ente": "MACAE", "NumeroContrato": "2"}]},
        {"Contratos": [{"Ente": "MACAE", "NumeroContrato": "3"}]},
    ]

    monkeypatch.setattr(tce_rj, "_get", lambda *a, **k: pages.pop(0))
    monkeypatch.setattr(tce_rj.time, "sleep", lambda *_: None)

    out = tce_rj._fetch_paginated(
        endpoint="contratos_municipio",
        params_base={"municipio": "macae"},
        data_keys=("Contratos",),
    )

    assert [item["NumeroContrato"] for item in out] == ["1", "2", "3"]


def test_fetch_aditivos_filtra_municipio_e_campos_aditivo(monkeypatch):
    payload = [
        {
            "Ente": "MACAE",
            "NumeroConvenio": "A-1",
            "AnoConvenio": 2024,
            "QuantidadeAditivos": 2,
            "ValorAditivos": 100.0,
        },
        {
            "Ente": "MACAE",
            "NumeroConvenio": "A-2",
            "AnoConvenio": 2024,
            "QuantidadeAditivos": 0,
            "ValorAditivos": 0,
        },
        {
            "Ente": "NITEROI",
            "NumeroConvenio": "A-3",
            "AnoConvenio": 2024,
            "QuantidadeAditivos": 1,
            "ValorAditivos": 50.0,
        },
    ]

    monkeypatch.setattr(tce_rj, "_get", lambda *a, **k: payload)

    out = tce_rj.fetch_aditivos()

    assert len(out) == 1
    assert out[0]["NumeroConvenio"] == "A-1"


def test_fetch_obras_filtra_municipio(monkeypatch):
    payload = {
        "Obras": [
            {"Ente": "MACAE", "NumeroContrato": "001"},
            {"Ente": "ANGRA DOS REIS", "NumeroContrato": "002"},
        ]
    }
    monkeypatch.setattr(tce_rj, "_get", lambda *a, **k: payload)

    out = tce_rj.fetch_obras()

    assert len(out) == 1
    assert out[0]["NumeroContrato"] == "001"


def test_fetch_contratos_filtra_municipio(monkeypatch):
    monkeypatch.setattr(
        tce_rj,
        "_fetch_paginated",
        lambda *a, **k: [
            {"Ente": "MACAE", "NumeroContrato": "1"},
            {"Ente": "NITEROI", "NumeroContrato": "2"},
        ],
    )

    out = tce_rj.fetch_contratos()

    assert len(out) == 1
    assert out[0]["NumeroContrato"] == "1"


def test_normalizar_contratos_mapeia_campos():
    registros = [
        {
            "Ente": "MACAE",
            "NumeroContrato": "C-10",
            "Objeto": "Reforma de escola",
            "Modalidade": "Pregao",
            "TipoContrato": "SERVICOS",
            "CNPJCPFContratado": "12345678000199",
            "Contratado": "Empresa X",
            "ValorContrato": "R$ 1.234,56",
            "DataAssinaturaContrato": "2026-01-10",
            "DataVencimentoContrato": "2026-12-31",
        }
    ]

    df = tce_rj.normalizar_contratos(registros)

    assert len(df) == 1
    assert df.iloc[0]["id_contrato"] == "C-10"
    assert df.iloc[0]["valor_contrato"] == 1234.56
    assert df.iloc[0]["fonte"] == "tce_rj_contratos_municipio"
    assert json.loads(df.iloc[0]["payload_bruto"])["NumeroContrato"] == "C-10"


def test_normalizar_aditivos_mapeia_campos():
    registros = [
        {
            "Ente": "MACAE",
            "NumeroConvenio": "CV-9",
            "AnoConvenio": 2025,
            "MesConvenio": 5,
            "ComvenioAditivo": "Aditivo",
            "QuantidadeAditivos": 2,
            "ValorAditivos": 1000.0,
            "DataAssinatura": 1735689600000,
        }
    ]

    df = tce_rj.normalizar_aditivos(registros)

    assert len(df) == 1
    assert df.iloc[0]["id_aditivo"] == "CV-9-2025"
    assert df.iloc[0]["quantidade_aditivos"] == 2.0
    assert df.iloc[0]["fonte"] == "tce_rj_convenios_municipio"


def test_to_iso_datetime_converte_epoch_ms():
    iso_value = tce_rj._to_iso_datetime(1735689600000)

    assert iso_value.startswith("2025-01-01T00:00:00")


def test_to_float_e_to_iso_datetime_branches():
    assert tce_rj._to_float("R$ 2.500,00") == 2500.0
    assert tce_rj._to_float("invalido") is None
    assert tce_rj._to_iso_datetime("01/02/2026").startswith("2026-02-01T00:00:00")
    assert tce_rj._to_iso_datetime("sem-data") == "sem-data"


def test_normalize_text_e_match_municipio():
    assert tce_rj._normalize_text("Macaé") == "macae"
    assert tce_rj._municipio_match("MACAÉ") is True


def test_run_fallback_para_cache(monkeypatch):
    monkeypatch.setattr(
        tce_rj,
        "fetch_contratos",
        lambda: (_ for _ in ()).throw(RuntimeError("api indisponivel")),
    )
    monkeypatch.setattr(tce_rj, "_carregar_cache", lambda: {
        "contratos": pd.DataFrame([{"id_contrato": "cache-1"}]),
        "aditivos": pd.DataFrame(),
        "obras": pd.DataFrame(),
    })

    out = tce_rj.run()

    assert len(out["contratos"]) == 1
    assert out["contratos"].iloc[0]["id_contrato"] == "cache-1"


def test_run_sucesso_salva_cache(monkeypatch):
    monkeypatch.setattr(tce_rj, "fetch_contratos", lambda: [{"NumeroContrato": "C1", "Ente": "MACAE"}])
    monkeypatch.setattr(tce_rj, "fetch_aditivos", lambda: [])
    monkeypatch.setattr(tce_rj, "fetch_obras", lambda: [])

    monkeypatch.setattr(
        tce_rj,
        "normalizar_contratos",
        lambda registros: pd.DataFrame([{"id_contrato": registros[0]["NumeroContrato"]}]),
    )
    monkeypatch.setattr(tce_rj, "normalizar_aditivos", lambda _: pd.DataFrame())
    monkeypatch.setattr(tce_rj, "normalizar_obras", lambda _: pd.DataFrame())

    saved = []
    monkeypatch.setattr(tce_rj, "_salvar_cache", lambda datasets: saved.append(datasets))

    out = tce_rj.run()

    assert len(out["contratos"]) == 1
    assert out["contratos"].iloc[0]["id_contrato"] == "C1"
    assert len(saved) == 1


def test_salvar_e_carregar_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(tce_rj, "CACHE_DIR", tmp_path)

    datasets = {
        "contratos": pd.DataFrame([{"id_contrato": "A"}]),
        "aditivos": pd.DataFrame([{"id_aditivo": "B"}]),
        "obras": pd.DataFrame([{"id_obra": "C"}]),
    }
    tce_rj._salvar_cache(datasets)

    out = tce_rj._carregar_cache()

    assert out["contratos"].iloc[0]["id_contrato"] == "A"
    assert out["aditivos"].iloc[0]["id_aditivo"] == "B"
    assert out["obras"].iloc[0]["id_obra"] == "C"
