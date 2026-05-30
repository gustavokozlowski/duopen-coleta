import pandas as pd
import pytest

from scrappers.federal import transparencia_convenios as tc

pytestmark = pytest.mark.unit


def test_so_digitos():
    assert tc._so_digitos("29.115.474/0001-60") == "29115474000160"
    assert tc._so_digitos(None) is None
    assert tc._so_digitos("") is None


def test_normalizar_extrai_data_conclusao_e_cnpj():
    brutos = [{
        "situacao": "PRESTAÇÃO DE CONTAS APROVADA",
        "dataConclusao": "2015-05-30",
        "dataInicioVigencia": "2011-12-01",
        "dataFinalVigencia": "2015-05-30",
        "valor": 495000.0, "valorLiberado": 490000.0,
        "dimConvenio": {"codigo": "757206", "numero": "07908/2011", "objeto": "Pavimentação"},
        "convenente": {"cnpjFormatado": "29.115.474/0001-60", "nome": "MUNICIPIO DE MACAE"},
        "orgao": {"nome": "Ministério X"},
    }]
    row = tc.normalizar(brutos).iloc[0]
    assert row["nr_convenio"] == "757206"
    assert row["data_conclusao"] == "2015-05-30"
    assert row["cnpj_proponente"] == "29115474000160"
    assert row["valor"] == 495000.0
    assert row["objeto"] == "Pavimentação"


def test_normalizar_descarta_sem_codigo_e_dedup():
    brutos = [
        {"dimConvenio": {"codigo": "1", "numero": "a"}, "situacao": "X"},
        {"dimConvenio": {"codigo": "1", "numero": "a"}, "situacao": "X"},  # dup
        {"dimConvenio": {"codigo": None}, "situacao": "Y"},                 # sem código
    ]
    df = tc.normalizar(brutos)
    assert len(df) == 1
    assert df.iloc[0]["nr_convenio"] == "1"


def test_normalizar_vazio():
    assert tc.normalizar([]).empty


def test_routing_consistente():
    from etl.routing import RAW_LAYER_ROUTING, colunas_alvo
    rota = RAW_LAYER_ROUTING["transparencia_convenios"]
    assert rota["tabela"] == "raw_convenios_federais"
    assert rota["conflict"] == ("nr_convenio", "fonte")
    cols = colunas_alvo("raw_convenios_federais")
    for c in ("nr_convenio", "data_conclusao", "cnpj_proponente", "valor"):
        assert c in cols
