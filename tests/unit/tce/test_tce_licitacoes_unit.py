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
    assert tce_licitacoes.CONTRATOS_ENDPOINT == "/contratos_municipio"


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


def test_fetch_contratos_usa_contratos_municipio(monkeypatch):
    chamadas = []

    def fake_get(endpoint, params=None):
        chamadas.append((endpoint, dict(params or {})))
        return {
            "Contratos": [
                {"Ente": "MACAE", "NumeroContrato": "1"},
                {"Ente": "NITEROI", "NumeroContrato": "2"},
            ]
        }

    monkeypatch.setattr(tce_licitacoes, "_get", fake_get)
    monkeypatch.setattr(tce_licitacoes.time, "sleep", lambda *_: None)

    out = tce_licitacoes.fetch_contratos()

    assert chamadas == [
        (
            "/contratos_municipio",
            {
                "ano": 0,
                "inicio": 0,
                "limite": 1000,
                "municipio": "MACAE",
                "csv": "false",
                "jsonfull": "false",
            },
        )
    ]
    assert [item["NumeroContrato"] for item in out] == ["1"]


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

    resultado = tce_licitacoes.run()

    assert set(resultado) == {"licitacoes", "contratos", "perfil_fornecedores"}
    assert not resultado["licitacoes"].empty
    assert resultado["licitacoes"].iloc[0]["id_licitacao"] == "1"
    assert resultado["contratos"].empty
    assert resultado["perfil_fornecedores"].empty


def test_normalizar_contratos_mapeia_campos_do_contratos_municipio():
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
            "UnidadeGestora": "SECRETARIA MUNICIPAL DE OBRAS",
        }
    ]

    df = tce_licitacoes.normalizar_contratos(registros)

    assert len(df) == 1
    row = df.iloc[0]
    assert row["id_contrato"] == "C-10"
    assert row["municipio"] == "MACAE"
    assert row["cnpj_fornecedor"] == "12345678000199"
    assert row["nome_fornecedor"] == "Empresa X"
    assert row["valor_contrato"] == 1234.56
    assert row["data_assinatura"].startswith("2026-01-10T00:00:00")
    assert row["data_fim"].startswith("2026-12-31T00:00:00")
    assert row["fonte"] == "tce_rj_contratos_municipio"


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


def test_calcular_perfil_fornecedores_com_aditivos():
    df = pd.DataFrame([
        {
            "cnpj_fornecedor": "123",
            "nome_fornecedor": "Fornecedor A",
            "valor_contrato": 100.0,
            "qtd_aditivos": 1,
            "possui_aditivo": "Sim",
            "ano": 2024,
        },
        {
            "cnpj_fornecedor": "123",
            "nome_fornecedor": "Fornecedor A",
            "valor_contrato": 200.0,
            "qtd_aditivos": 0,
            "possui_aditivo": "Nao",
            "ano": 2025,
        },
        {
            "cnpj_fornecedor": "456",
            "nome_fornecedor": "Fornecedor B",
            "valor_contrato": 50.0,
            "qtd_aditivos": 0,
            "possui_aditivo": "Nao",
            "ano": 2024,
        },
    ])

    out = tce_licitacoes.calcular_perfil_fornecedores(df)

    assert len(out) == 2
    row = out[out["cnpj_fornecedor"] == "123"].iloc[0]
    assert row["total_contratos"] == 2
    assert row["valor_total"] == 300.0
    assert row["taxa_aditivo"] == 0.5


def test_carregar_cache_dataset_retorna_df(tmp_path, monkeypatch):
    monkeypatch.setattr(tce_licitacoes, "CACHE_DIR", tmp_path)
    path = tmp_path / tce_licitacoes.CACHE_FILES["contratos"]
    df_in = pd.DataFrame([{"id_contrato": "C1"}])
    df_in.to_json(path, orient="records")

    out = tce_licitacoes._carregar_cache_dataset("contratos")

    assert len(out) == 1
    assert out.iloc[0]["id_contrato"] == "C1"


def test_executar_etapa_usa_cache_em_falha(monkeypatch):
    fallback_df = pd.DataFrame([{"id": 1}])
    monkeypatch.setattr(tce_licitacoes, "_carregar_cache_dataset", lambda *_: fallback_df)

    out = tce_licitacoes._executar_etapa(
        "licitacoes",
        fetcher=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        normalizador=lambda registros: pd.DataFrame(registros),
    )

    assert len(out) == 1
