import json

import pandas as pd
import pytest

from scrappers.federal import sinapi

pytestmark = pytest.mark.unit


def test_run_retorna_dataframe(tmp_path, monkeypatch):
    monkeypatch.setattr(sinapi, "CACHE_DIR", tmp_path)
    df = sinapi.run()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_run_uma_linha_por_tipo(tmp_path, monkeypatch):
    monkeypatch.setattr(sinapi, "CACHE_DIR", tmp_path)
    df = sinapi.run()
    assert len(df) == len(sinapi.SINAPI_REFERENCIA_RJ)
    assert set(df["tipo_obra"]) == set(sinapi.SINAPI_REFERENCIA_RJ)


def test_schema_dataframe(tmp_path, monkeypatch):
    monkeypatch.setattr(sinapi, "CACHE_DIR", tmp_path)
    df = sinapi.run()
    assert list(df.columns) == [
        "uf", "competencia", "tipo_obra", "custo_m2", "coletado_em",
    ]
    # fonte é definida pelo routing, não pelo scraper
    assert "fonte" not in df.columns
    assert (df["uf"] == "RJ").all()


def test_cache_lista_de_registros(tmp_path, monkeypatch):
    monkeypatch.setattr(sinapi, "CACHE_DIR", tmp_path)
    sinapi.run()
    dados = json.loads((tmp_path / "sinapi.json").read_text(encoding="utf-8"))
    assert isinstance(dados, list)
    assert all(isinstance(r, dict) for r in dados)
    assert len(dados) == len(sinapi.SINAPI_REFERENCIA_RJ)


@pytest.mark.parametrize("entrada,esperado", [
    ("UBS Lagomar", "ubs"),
    ("Construção de UPA 24h", "upa"),
    ("Ampliação da Escola Municipal", "escola"),
    ("Reforma de Creche", "escola"),
    ("Pavimentação da Rua X", "pavimentacao_asfalto"),
    ("Recapeamento Asfáltico", "pavimentacao_asfalto"),
    ("Drenagem e galeria", "drenagem"),
    ("Praça da Juventude", "praca_urbanizacao"),
    ("Quadra Poliesportiva", "quadra_esportiva"),
])
def test_mapear_tipo_sinapi(entrada, esperado):
    assert sinapi.mapear_tipo_sinapi(entrada) == esperado


def test_mapear_tipo_sinapi_desconhecido():
    assert sinapi.mapear_tipo_sinapi("xpto") == "padrao"


def test_mapear_tipo_sinapi_vazio():
    assert sinapi.mapear_tipo_sinapi("") == "padrao"
    assert sinapi.mapear_tipo_sinapi(None) == "padrao"


def test_custo_referencia():
    assert sinapi.custo_referencia("UBS Lagomar") == sinapi.SINAPI_REFERENCIA_RJ["ubs"]
    assert sinapi.custo_referencia("xpto") == sinapi.SINAPI_REFERENCIA_RJ["padrao"]


def test_routing_sinapi_consistente():
    """A rota 'sinapi' deve apontar para raw_sinapi com colunas válidas."""
    from etl.routing import RAW_LAYER_ROUTING, colunas_alvo

    rota = RAW_LAYER_ROUTING["sinapi"]
    assert rota["tabela"] == "raw_sinapi"
    assert rota["conflict"] == ("uf", "competencia", "tipo_obra")
    colunas = colunas_alvo("raw_sinapi")
    # as colunas produzidas pelo run() devem existir no schema alvo
    for col in ("uf", "competencia", "tipo_obra", "custo_m2", "coletado_em"):
        assert col in colunas
