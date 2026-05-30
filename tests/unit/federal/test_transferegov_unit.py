import json

import pandas as pd
import pytest

from scrappers.federal import transferegov

pytestmark = pytest.mark.unit


def test_so_digitos():
    assert transferegov._so_digitos("12.345.678/0001-99") == "12345678000199"
    assert transferegov._so_digitos(None) is None
    assert transferegov._so_digitos("") is None


def test_float_formato_br():
    assert transferegov._float("R$ 1.234,56") == pytest.approx(1234.56)
    assert transferegov._float(None) is None
    assert transferegov._float("xpto") is None


def test_normalizar_agrega_aditivos_e_cnpj():
    brutos = [{
        "nr_convenio": "757206",
        "id_proposta": "527445",
        "identif_proponente": "12.345.678/0001-99",
        "nm_proponente": "Prefeitura de Macaé",
        "vl_global_conv": "324023.45",
        "sit_convenio": "Em execução",
        "_aditivos": [
            {"vl_global_ta": "10000.00"},
            {"vl_global_ta": "5000.00"},
        ],
    }]
    df = transferegov.normalizar(brutos)
    row = df.iloc[0]
    assert row["nr_convenio"] == "757206"
    assert row["cnpj_proponente"] == "12345678000199"  # só dígitos
    assert row["qtd_aditivos"] == 2
    assert row["valor_aditivos"] == pytest.approx(15000.0)
    assert row["valor_global"] == pytest.approx(324023.45)


def test_normalizar_sem_aditivos():
    df = transferegov.normalizar([{"nr_convenio": "3766", "_aditivos": []}])
    row = df.iloc[0]
    assert row["qtd_aditivos"] == 0
    assert row["valor_aditivos"] is None  # sem aditivos → None (não 0)


def test_normalizar_vazio():
    assert transferegov.normalizar([]).empty


def test_convenios_do_legado_le_cache(tmp_path, monkeypatch):
    cache = tmp_path / "painel_legado_obras.json"
    cache.write_text(json.dumps([
        {"num_licitacao": "757206"},
        {"num_licitacao": "3766"},
        {"num_licitacao": "3766"},     # duplicado
        {"num_licitacao": None},        # ignorado
        {"num_licitacao": "None"},      # ignorado
    ]), encoding="utf-8")
    monkeypatch.setattr(transferegov, "LEGADO_CACHE", cache)
    convenios = transferegov.convenios_do_legado()
    assert convenios == ["3766", "757206"]  # únicos, ordenados, sem nulos


def test_run_sem_cache_legado_retorna_vazio(tmp_path, monkeypatch):
    monkeypatch.setattr(transferegov, "LEGADO_CACHE", tmp_path / "inexistente.json")
    assert transferegov.run().empty


def test_routing_transferegov_consistente():
    from etl.routing import RAW_LAYER_ROUTING, colunas_alvo
    rota = RAW_LAYER_ROUTING["transferegov_aditivos"]
    assert rota["tabela"] == "raw_aditivos_federais"
    assert rota["conflict"] == ("nr_convenio", "fonte")
    cols = colunas_alvo("raw_aditivos_federais")
    for c in ("nr_convenio", "cnpj_proponente", "valor_aditivos", "qtd_aditivos"):
        assert c in cols
