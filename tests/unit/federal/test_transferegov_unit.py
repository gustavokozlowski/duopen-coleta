import json

import pandas as pd
import pytest

from scrappers.federal import transferegov

pytestmark = pytest.mark.unit


def test_valor_formato_br_us_e_vazio():
    assert transferegov._valor("324023,45") == pytest.approx(324023.45)  # BR
    assert transferegov._valor("356400") == pytest.approx(356400.0)      # inteiro
    assert transferegov._valor("1.200.000,00") == pytest.approx(1200000.0)  # BR milhar
    assert transferegov._valor("") == 0.0
    assert transferegov._valor(None) == 0.0


def test_chave_nr_convenio_tolera_bom():
    assert transferegov._chave_nr_convenio({"﻿NR_CONVENIO": "757206"}) == "757206"
    assert transferegov._chave_nr_convenio({"NR_CONVENIO": " 775661 "}) == "775661"
    assert transferegov._chave_nr_convenio({"OUTRA": "x"}) == ""


def test_convenios_do_legado_le_cache(tmp_path, monkeypatch):
    cache = tmp_path / "painel_legado_obras.json"
    cache.write_text(json.dumps([
        {"num_licitacao": "757206"},
        {"num_licitacao": "775661"},
        {"num_licitacao": "775661"},   # duplicado
        {"num_licitacao": None},        # ignorado
        {"num_licitacao": "None"},      # ignorado
    ]), encoding="utf-8")
    monkeypatch.setattr(transferegov, "LEGADO_CACHE", cache)
    assert transferegov.convenios_do_legado() == {"757206", "775661"}


def test_normalizar_agrega_e_vigencia_zero():
    # 757206: 2 aditivos só de vigência (valor 0); 800000: 1 aditivo financeiro
    dados = {
        "757206": {"nr_convenio": "757206", "id_proposta": "527445", "valor_global": 495000.0,
                   "situacao": "Prestação de Contas Aprovada", "qtd_aditivos": 2,
                   "valor_aditivos": 0.0, "_tem_aditivo": True},
        "800000": {"nr_convenio": "800000", "id_proposta": None, "valor_global": 100000.0,
                   "situacao": "Em execução", "qtd_aditivos": 1,
                   "valor_aditivos": 25000.0, "_tem_aditivo": True},
        "913439": {"nr_convenio": "913439", "id_proposta": None, "valor_global": 1200000.0,
                   "situacao": "Convênio Anulado", "qtd_aditivos": 0,
                   "valor_aditivos": 0.0, "_tem_aditivo": False},
    }
    df = transferegov.normalizar(dados).set_index("nr_convenio")
    assert df.loc["757206", "qtd_aditivos"] == 2
    assert df.loc["757206", "valor_aditivos"] == 0.0      # vigência → 0 (informativo)
    assert df.loc["800000", "valor_aditivos"] == 25000.0  # aditivo financeiro
    assert pd.isna(df.loc["913439", "valor_aditivos"])    # sem aditivo → None
    assert df.loc["757206", "cnpj_proponente"] is None    # não vem destes 2 arquivos


def test_normalizar_vazio():
    assert transferegov.normalizar({}).empty


def test_run_sem_cache_legado_retorna_vazio(tmp_path, monkeypatch):
    monkeypatch.setattr(transferegov, "LEGADO_CACHE", tmp_path / "inexistente.json")
    assert transferegov.run().empty


def test_routing_transferegov_consistente():
    from etl.routing import RAW_LAYER_ROUTING, colunas_alvo
    rota = RAW_LAYER_ROUTING["transferegov_aditivos"]
    assert rota["tabela"] == "raw_aditivos_federais"
    assert rota["conflict"] == ("nr_convenio", "fonte")
    cols = colunas_alvo("raw_aditivos_federais")
    for c in ("nr_convenio", "valor_aditivos", "qtd_aditivos"):
        assert c in cols
