import json

import pandas as pd
import pytest
import requests

from scrappers.tce import tce_licitacoes

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


def test_constantes_licitacoes_endpoint():
    assert tce_licitacoes.BASE_URL == "https://dados.tcerj.tc.br/api/v1"
    assert tce_licitacoes.LICITACOES_PAGE_LIMIT == 1000


def test_get_404_retorna_dict_vazio(monkeypatch):
    monkeypatch.setattr(
        tce_licitacoes.requests,
        "get",
        lambda *args, **kwargs: DummyResponse(status_code=404, http_error=_http_error(404)),
    )

    out = tce_licitacoes._get("/contratos", {"municipio": "MACAE"})

    assert out == {}


def test_fetch_licitacoes_pagina_e_filtra_macae(monkeypatch):
    calls = []
    pages = {
        0: {
            "Licitacoes": [
                {"Ente": "MACAE", "ProcessoLicitatorio": "1", "NumeroEdital": "001"},
                {"Ente": "NITEROI", "ProcessoLicitatorio": "2", "NumeroEdital": "002"},
            ]
        },
        2: {
            "Licitacoes": [
                {"Ente": "Macaé", "ProcessoLicitatorio": "3", "NumeroEdital": "003"},
                {"Ente": "NOVA FRIBURGO", "ProcessoLicitatorio": "4", "NumeroEdital": "004"},
            ]
        },
        4: {"Licitacoes": []},
    }

    def fake_get(endpoint, params=None):
        calls.append((endpoint, dict(params or {})))
        return pages[params["inicio"]]

    monkeypatch.setattr(tce_licitacoes, "LICITACOES_PAGE_LIMIT", 2)
    monkeypatch.setattr(tce_licitacoes, "_get", fake_get)
    monkeypatch.setattr(tce_licitacoes.time, "sleep", lambda *_: None)

    out = tce_licitacoes.fetch_licitacoes()

    assert [item["ProcessoLicitatorio"] for item in out] == ["1", "3"]
    assert calls[0][0] == "/licitacoes"
    assert calls[0][1]["inicio"] == 0
    assert calls[0][1]["limite"] == 2
    assert calls[0][1]["csv"] == "false"
    assert calls[0][1]["jsonfull"] == "false"
    assert calls[1][1]["inicio"] == 2
    assert calls[2][1]["inicio"] == 4


def test_coletar_endpoint_sem_ano_quando_desativado(monkeypatch):
    chamadas = []

    monkeypatch.setattr(tce_licitacoes, "ANO_INICIO", None)
    monkeypatch.setattr(tce_licitacoes, "ANO_FIM", None)

    monkeypatch.setattr(
        tce_licitacoes,
        "_paginar",
        lambda endpoint, params: chamadas.append((endpoint, dict(params))) or [{"id": "1"}],
    )

    out = tce_licitacoes._coletar_endpoint_por_anos("contratos")

    assert chamadas == [("/contratos", {"municipio": "MACAE"})]
    assert out == [{"id": "1"}]


def test_run_isola_falhas_por_etapa(monkeypatch):
    monkeypatch.setattr(tce_licitacoes, "_salvar_cache", lambda datasets: None)
    monkeypatch.setattr(tce_licitacoes, "_carregar_cache_dataset", lambda nome: pd.DataFrame())

    monkeypatch.setattr(
        tce_licitacoes,
        "fetch_licitacoes",
        lambda: [{"Ente": "MACAE", "ProcessoLicitatorio": "1", "NumeroEdital": "001"}],
    )
    monkeypatch.setattr(
        tce_licitacoes,
        "normalizar_licitacoes",
        lambda registros: pd.DataFrame([
            {"id_licitacao": registros[0]["ProcessoLicitatorio"], "numero": registros[0]["NumeroEdital"]}
        ]),
    )

    monkeypatch.setattr(
        tce_licitacoes,
        "fetch_contratos",
        lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(tce_licitacoes, "fetch_compras_diretas", lambda: [])
    monkeypatch.setattr(tce_licitacoes, "fetch_obras_paralisadas", lambda: [])

    resultado = tce_licitacoes.run()

    assert not resultado["licitacoes"].empty
    assert resultado["licitacoes"].iloc[0]["id_licitacao"] == "1"
    assert resultado["contratos"].empty


def test_normalizar_licitacoes_mapeia_campos_do_endpoint():
    registros = [
        {
            "Ente": "MACAE",
            "Unidade": "SECRETARIA MUNICIPAL DE OBRAS",
            "Ano": 2022,
            "Mes": 9,
            "Modalidade": "PREGÃO ELETRÔNICO",
            "Tipo": "MENOR PREÇO GLOBAL",
            "ProcessoLicitatorio": "294023",
            "NumeroEdital": "PE Nº 066/2022",
            "PublicacaoOficial": "BO Nº 7192",
            "DataPublicacaoOficial": "2022-09-29",
            "DataHomologacao": "2022-10-26",
            "DataPublicacaoEdital": "2022-09-16",
            "Objeto": "CONTRATAÇÃO DE SERVIÇOS",
            "Parecer": None,
            "ValorEstimado": 24690481.92,
            "AdiadoSineDie": "NÃO",
            "OrcamentoSigiloso": "NÃO",
            "PercentualRecursosUniao": 53.0,
        }
    ]

    df = tce_licitacoes.normalizar_licitacoes(registros)

    assert len(df) == 1
    row = df.iloc[0]
    assert row["municipio"] == "MACAE"
    assert row["processo_licitatorio"] == "294023"
    assert row["numero"] == "PE Nº 066/2022"
    assert row["valor_estimado"] == 24690481.92
    assert row["data_publicacao_oficial"].startswith("2022-09-29T00:00:00")
    assert row["data_publicacao_edital"].startswith("2022-09-16T00:00:00")
    assert row["fonte"] == "tce_rj_licitacoes"
    assert json.loads(row["payload_bruto"])["Ente"] == "MACAE"
