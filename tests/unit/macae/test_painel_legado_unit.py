from __future__ import annotations

import json
from unittest import mock

import pandas as pd
import pytest

from scrappers.macae import painel_legado as legado


class FakeDriver:
    def __init__(self) -> None:
        self.quit_called = False

    def quit(self) -> None:
        self.quit_called = True


# ── Testes de seleção de localidade ──────────────────────────────────────────

def test_selecionar_localidade_prefere_variacao_acentuada() -> None:
    candidatos = [
        {"qText": "MACAE/RJ", "qElemNumber": 2393, "qState": "O"},
        {"qText": "MACAÉ/RJ", "qElemNumber": 7419, "qState": "O"},
    ]

    selecionada = legado._selecionar_localidade(candidatos, "Macae")

    assert selecionada["qText"] == "MACAÉ/RJ"
    assert selecionada["qElemNumber"] == 7419


def test_selecionar_localidade_exata_para_variacao_sem_acento() -> None:
    candidatos = [
        {"qText": "MACAE/RJ", "qElemNumber": 2393, "qState": "O"},
        {"qText": "MACAÉ/RJ", "qElemNumber": 7419, "qState": "O"},
    ]

    selecionada = legado._selecionar_localidade(candidatos, "Macae/RJ", preferir_exata=True)

    assert selecionada["qText"] == "MACAE/RJ"
    assert selecionada["qElemNumber"] == 2393


# ── Testes de coleta via Qlik ─────────────────────────────────────────────────

def test_fetch_obras_mapeia_hypercube_e_seleciona_municipio_acentuado(monkeypatch) -> None:
    fake_driver = FakeDriver()
    candidatos = [
        {"qText": "MACAE/RJ", "qElemNumber": 2393, "qState": "O"},
        {"qText": "MACAÉ/RJ", "qElemNumber": 7419, "qState": "O"},
    ]
    metadados = {
        "columns": [
            {"name": "ID_OBRA_OBRAS"},
            {"name": "MUNIC_PROPONENTE_OBRAS"},
            {"name": "Execução Física"},
        ],
        "row_count": 1,
    }
    rows = [["757206", "Macaé/RJ", "60,00%"]]

    monkeypatch.setattr(legado, "_inicializar_driver", lambda: fake_driver)
    monkeypatch.setattr(legado, "_abrir_painel", lambda driver: None)
    monkeypatch.setattr(legado, "_obter_candidatos_localidade", lambda driver: candidatos)
    monkeypatch.setattr(legado, "LOCALIDADE_COMPLEMENTAR", "")

    def _fake_obter_metadados_obras_qlik(driver, q_elem_number):
        assert q_elem_number == 2393
        return metadados

    def _fake_obter_pagina_obras_qlik(driver, q_top, q_height, q_width):
        assert q_top == 0
        assert q_height == 1
        assert q_width == 3
        return rows

    monkeypatch.setattr(legado, "_obter_metadados_obras_qlik", _fake_obter_metadados_obras_qlik)
    monkeypatch.setattr(legado, "_obter_pagina_obras_qlik", _fake_obter_pagina_obras_qlik)

    registros = legado.fetch_obras("Macae")

    assert fake_driver.quit_called is True
    assert registros == [
        {
            "id_obra_obras": "757206",
            "munic_proponente_obras": "Macaé/RJ",
            "execucao_fisica": "60,00%",
        }
    ]


def test_fetch_obras_padrao_coleta_localidade_complementar(monkeypatch) -> None:
    fake_driver = FakeDriver()
    candidatos = [
        {"qText": "MACAE/RJ", "qElemNumber": 2393, "qState": "O"},
        {"qText": "MACAÉ/RJ", "qElemNumber": 7419, "qState": "O"},
    ]
    qelems_coletados = []

    monkeypatch.setattr(legado, "_inicializar_driver", lambda: fake_driver)
    monkeypatch.setattr(legado, "_abrir_painel", lambda driver: None)
    monkeypatch.setattr(legado, "_obter_candidatos_localidade", lambda driver: candidatos)
    monkeypatch.setattr(legado, "LOCALIDADE_PADRAO", "Macaé/RJ")
    monkeypatch.setattr(legado, "LOCALIDADE_COMPLEMENTAR", "Macae/RJ")

    def _fake_obter_metadados_obras_qlik(driver, q_elem_number):
        qelems_coletados.append(q_elem_number)
        return {"columns": [{"name": "ID_OBRA_OBRAS"}], "row_count": 0}

    monkeypatch.setattr(legado, "_obter_metadados_obras_qlik", _fake_obter_metadados_obras_qlik)
    monkeypatch.setattr(legado, "_obter_pagina_obras_qlik", lambda *args, **kwargs: [])

    registros = legado.fetch_obras()

    assert fake_driver.quit_called is True
    assert registros == []
    assert qelems_coletados == [7419, 2393]


# ── Testes de run() ───────────────────────────────────────────────────────────

def test_run_salva_cache_quando_coleta_sucesso(monkeypatch) -> None:
    registros = [{"id_obra_obras": "757206", "munic_proponente_obras": "Macaé/RJ"}]
    salvar_cache = mock.MagicMock()
    monkeypatch.setattr(legado, "fetch_obras", lambda localidade=None: registros)
    monkeypatch.setattr(legado, "_salvar_cache", salvar_cache)

    df = legado.run("Macae")

    assert len(df) == 1
    salvar_cache.assert_called_once()


def test_run_usa_cache_em_falha(monkeypatch) -> None:
    cache_df = pd.DataFrame([{"id_obra": "cache-1", "fonte": "cache"}])

    def boom(localidade=None):
        raise RuntimeError("falha")

    monkeypatch.setattr(legado, "fetch_obras", boom)
    monkeypatch.setattr(legado, "_carregar_cache", lambda: cache_df)

    df = legado.run()

    assert df.equals(cache_df)


# ── Testes de _converter_valor_monetario() ────────────────────────────────────

def test_converter_valor_monetario_formato_br() -> None:
    """'R$773.000,00' → 773000.00"""
    assert legado._converter_valor_monetario("R$773.000,00") == pytest.approx(773000.00)


def test_converter_valor_monetario_zero() -> None:
    """'R$0,00' → 0.0"""
    assert legado._converter_valor_monetario("R$0,00") == pytest.approx(0.0)


def test_converter_valor_monetario_nat_retorna_none() -> None:
    """'NaT' → None"""
    assert legado._converter_valor_monetario("NaT") is None


def test_converter_valor_monetario_none_retorna_none() -> None:
    """None → None"""
    assert legado._converter_valor_monetario(None) is None


# ── Testes de _converter_percentual() ────────────────────────────────────────

def test_converter_percentual_formato_br() -> None:
    """'40,00%' → 40.00"""
    assert legado._converter_percentual("40,00%") == pytest.approx(40.00)


def test_converter_percentual_cem_porcento() -> None:
    """'100,00%' → 100.00"""
    assert legado._converter_percentual("100,00%") == pytest.approx(100.00)


# ── Testes de _percentual_financeiro() ────────────────────────────────────────

def test_percentual_financeiro_calcula() -> None:
    """executado/contrato * 100, arredondado a 2 casas."""
    assert legado._percentual_financeiro(618_400.0, 773_000.0) == pytest.approx(80.0)


def test_percentual_financeiro_contrato_zero_ou_nulo_retorna_none() -> None:
    """Contrato ausente ou zero → None (sem divisão por zero)."""
    assert legado._percentual_financeiro(100.0, 0) is None
    assert legado._percentual_financeiro(100.0, None) is None
    assert legado._percentual_financeiro(None, 1000.0) is None


def test_normalizar_linha_ano_conclusao_para_int() -> None:
    """ano_conclusao_obras (ano de conclusão do legado) é convertido para int."""
    assert legado._normalizar_linha({"ano_conclusao": "2014"})["ano_conclusao"] == 2014
    assert legado._normalizar_linha({"ano_conclusao": None})["ano_conclusao"] is None


# ── Testes de _converter_data() ───────────────────────────────────────────────

def test_converter_data_formato_br() -> None:
    """'09/07/2013' → '2013-07-09T00:00:00+00:00'"""
    assert legado._converter_data("09/07/2013") == "2013-07-09T00:00:00+00:00"


def test_converter_data_nat_retorna_none() -> None:
    """'NaT' → None"""
    assert legado._converter_data("NaT") is None


def test_converter_data_nan_retorna_none() -> None:
    """'nan' → None"""
    assert legado._converter_data("nan") is None


# ── Testes de _parse_coord() ─────────────────────────────────────────────────

def test_parse_coord_dms_sul() -> None:
    """'22.21.21.S' → -22.355833..."""
    assert legado._parse_coord("22.21.21.S") == pytest.approx(-22.355833, rel=1e-6)


def test_parse_coord_dms_oeste() -> None:
    """'41.46.11.W' → -41.769722..."""
    assert legado._parse_coord("41.46.11.W") == pytest.approx(-41.769722, rel=1e-6)


# ── Testes de _extrair_campos() ───────────────────────────────────────────────

def test_extrair_campos_mapeia_latitude() -> None:
    """'latitude_obras' → coluna 'latitude'"""
    row = {"latitude_obras": "-22.30345040975075", "longitude_obras": "-41.70458436012268"}
    resultado = legado._extrair_campos(row)
    assert resultado["latitude"] == "-22.30345040975075"


def test_extrair_campos_mapeia_sistema_origem() -> None:
    """'sistema_obras' → coluna 'sistema_origem'"""
    row = {"sistema_obras": "TRANSFEREGOV.BR"}
    resultado = legado._extrair_campos(row)
    assert resultado["sistema_origem"] == "TRANSFEREGOV.BR"


def test_extrair_campos_mapeia_valor_contrato() -> None:
    """'nome_tipo_obras' com 'R$773.000,00' → valor_contrato_str"""
    row = {"nome_tipo_obras": "R$773.000,00"}
    resultado = legado._extrair_campos(row)
    assert resultado["valor_contrato_str"] == "R$773.000,00"


# ── Testes de _normalizar_linha() ─────────────────────────────────────────────

def test_normalizar_linha_gera_nome_descritivo() -> None:
    """'UBS' + 'CONSTRUCAO' → 'UBS — CONSTRUCAO'"""
    row = {
        "nome_obra": "CONSTRUCAO",
        "objeto": "UBS",
        "valor_contrato_str": None,
        "execucao_fisica_str": None,
        "percentual_executado_str": None,
        "data_inicio_str": None,
        "data_prevista_fim_str": None,
        "latitude": None,
        "longitude": None,
        "ano_referencia": None,
    }
    resultado = legado._normalizar_linha(row)
    assert resultado["nome_obra"] == "UBS — CONSTRUCAO"


def test_normalizar_linha_remove_campos_auxiliares() -> None:
    """Campos _str devem ser removidos após conversão"""
    row = {
        "nome_obra": "Obra",
        "objeto": None,
        "valor_contrato_str": "R$100.000,00",
        "execucao_fisica_str": "R$50.000,00",
        "percentual_executado_str": "50,00%",
        "data_inicio_str": "01/01/2020",
        "data_prevista_fim_str": "31/12/2020",
        "latitude": None,
        "longitude": None,
        "ano_referencia": None,
    }
    resultado = legado._normalizar_linha(row)
    assert "valor_contrato_str" not in resultado
    assert "execucao_fisica_str" not in resultado
    assert "percentual_executado_str" not in resultado
    assert "data_inicio_str" not in resultado
    assert "data_prevista_fim_str" not in resultado
    assert resultado["valor_contrato"] == pytest.approx(100000.00)
    assert resultado["valor_final"] == pytest.approx(50000.00)
    assert resultado["percentual_executado"] == pytest.approx(50.00)


# ── Testes de run() com pipeline completo ────────────────────────────────────

def test_run_retorna_dataframe_nao_vazio(monkeypatch) -> None:
    """run() retorna DataFrame com registros ao processar dados válidos."""
    registros = [
        {
            "id_obra_obras": "111",
            "titulo_obras": "UBS",
            "nome_tipo_obras": "R$500.000,00",
            "latitude_obras": "-22.3",
            "longitude_obras": "-41.7",
            "situacao_agrupada_obras": "Concluída",
        }
    ]
    monkeypatch.setattr(legado, "fetch_obras", lambda localidade=None: registros)
    monkeypatch.setattr(legado, "_salvar_cache", mock.MagicMock())

    df = legado.run()

    assert not df.empty
    assert len(df) == 1


def test_run_latitude_preenchida(monkeypatch) -> None:
    """Após run(), latitude não deve ser nula para registros com latitude_obras."""
    registros = [
        {
            "id_obra_obras": "222",
            "latitude_obras": "-22.30345040975075",
            "longitude_obras": "-41.70458436012268",
        }
    ]
    monkeypatch.setattr(legado, "fetch_obras", lambda localidade=None: registros)
    monkeypatch.setattr(legado, "_salvar_cache", mock.MagicMock())

    df = legado.run()

    assert df.iloc[0]["latitude"] == pytest.approx(-22.30345040975075)


def test_run_valor_contrato_preenchido(monkeypatch) -> None:
    """Após run(), valor_contrato não deve ser nulo para registros com nome_tipo_obras."""
    registros = [
        {
            "id_obra_obras": "333",
            "nome_tipo_obras": "R$773.000,00",
        }
    ]
    monkeypatch.setattr(legado, "fetch_obras", lambda localidade=None: registros)
    monkeypatch.setattr(legado, "_salvar_cache", mock.MagicMock())

    df = legado.run()

    assert df.iloc[0]["valor_contrato"] == pytest.approx(773000.00)


def test_extrair_campos_mapeia_campos_financeiros_qlik() -> None:
    """Campos com rótulo enganoso no Qlik devem ser mapeados para nomes semânticos."""
    row = {
        "data_atualizacao_obras": "R$258.632.179,00",
        "data_previsao_retomada_tratativa_obras": "R$109.572.761,00",
        "data_criacao_obras": "R$149.059.417,00",
    }
    resultado = legado._extrair_campos(row)
    assert resultado["valor_repasse_str"] == "R$258.632.179,00"
    assert resultado["valor_contrapartida_str"] == "R$109.572.761,00"
    assert resultado["valor_executado_financeiro_str"] == "R$149.059.417,00"


def test_normalizar_linha_converte_campos_financeiros_qlik() -> None:
    """_normalizar_linha deve converter os três campos financeiros do Qlik para float."""
    row = {
        "nome_obra": "Obra",
        "objeto": None,
        "valor_contrato_str": None,
        "execucao_fisica_str": None,
        "percentual_executado_str": None,
        "data_inicio_str": None,
        "data_prevista_fim_str": None,
        "latitude": None,
        "longitude": None,
        "ano_referencia": None,
        "valor_repasse_str": "R$258.632.179,00",
        "valor_contrapartida_str": "R$109.572.761,00",
        "valor_executado_financeiro_str": "R$149.059.417,00",
    }
    resultado = legado._normalizar_linha(row)
    assert resultado["valor_repasse"] == pytest.approx(258_632_179.00)
    assert resultado["valor_contrapartida"] == pytest.approx(109_572_761.00)
    assert resultado["valor_executado_financeiro"] == pytest.approx(149_059_417.00)
    assert "valor_repasse_str" not in resultado
    assert "valor_contrapartida_str" not in resultado
    assert "valor_executado_financeiro_str" not in resultado


def test_run_sistema_origem_preenchido(monkeypatch) -> None:
    """Após run(), sistema_origem deve ser preservado a partir de sistema_obras."""
    registros = [
        {
            "id_obra_obras": "444",
            "sistema_obras": "TRANSFEREGOV.BR",
        }
    ]
    monkeypatch.setattr(legado, "fetch_obras", lambda localidade=None: registros)
    monkeypatch.setattr(legado, "_salvar_cache", mock.MagicMock())

    df = legado.run()

    assert df.iloc[0]["sistema_origem"] == "TRANSFEREGOV.BR"
