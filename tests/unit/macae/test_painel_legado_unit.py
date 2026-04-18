from __future__ import annotations

import json
from unittest import mock

import pandas as pd

from scrappers.macae import painel_legado as legado


class FakeDriver:
    def __init__(self) -> None:
        self.quit_called = False

    def quit(self) -> None:
        self.quit_called = True


def test_selecionar_localidade_prefere_variacao_acentuada() -> None:
    candidatos = [
        {"qText": "MACAE/RJ", "qElemNumber": 2393, "qState": "O"},
        {"qText": "MACAÉ/RJ", "qElemNumber": 7419, "qState": "O"},
    ]

    selecionada = legado._selecionar_localidade(candidatos, "Macae")

    assert selecionada["qText"] == "MACAÉ/RJ"
    assert selecionada["qElemNumber"] == 7419


def test_fetch_obras_mapeia_hypercube_e_seleciona_municipio_acentuado(monkeypatch) -> None:
    fake_driver = FakeDriver()
    candidatos = [
        {"qText": "MACAE/RJ", "qElemNumber": 2393, "qState": "O"},
        {"qText": "MACAÉ/RJ", "qElemNumber": 7419, "qState": "O"},
    ]
    payload = {
        "columns": [
            {"name": "ID_OBRA_OBRAS"},
            {"name": "MUNIC_PROPONENTE_OBRAS"},
            {"name": "Execução Física"},
        ],
        "rows": [["757206", "Macaé/RJ", "60,00%"]],
    }

    monkeypatch.setattr(legado, "_inicializar_driver", lambda: fake_driver)
    monkeypatch.setattr(legado, "_abrir_painel", lambda driver: None)
    monkeypatch.setattr(legado, "_obter_candidatos_localidade", lambda driver: candidatos)

    def _fake_obter_obras_qlik(driver, q_elem_number):
        assert q_elem_number == 7419
        return payload

    monkeypatch.setattr(legado, "_obter_obras_qlik", _fake_obter_obras_qlik)

    registros = legado.fetch_obras("Macae")

    assert fake_driver.quit_called is True
    assert registros == [
        {
            "id_obra_obras": "757206",
            "munic_proponente_obras": "Macaé/RJ",
            "execucao_fisica": "60,00%",
        }
    ]


def test_normalizar_obras_cria_schema_e_metadados() -> None:
    registros = [
        {
            "id_obra_obras": "757206",
            "cod_transacao_obras": "TR-1",
            "origem_obras": "TRANSFEREGOV.BR",
            "numero_instrumento_obras": "12345",
            "uf_proponente_obras": "RJ",
            "munic_proponente_obras": "Macaé/RJ",
            "latitude_obras": "-22,30",
            "longitude_obras": "-41,80",
            "ano_inicio_obras": "2024",
            "data_inicio_obras": "01/02/2024",
            "data_atualizacao_obras": "2024-03-01",
            "execucao_fisica": "60,00%",
            "investimento_total": "R$450.000,00",
        }
    ]

    df = legado.normalizar_obras(registros)

    assert list(df.columns) == legado.NORMALIZED_COLUMNS
    assert len(df) == 1
    assert df.loc[0, "id_obra"] == "757206"
    assert df.loc[0, "codigo_transacao_obra"] == "TR-1"
    assert df.loc[0, "ano_inicio_obra"] == 2024
    assert df.loc[0, "latitude"] == -22.3
    assert df.loc[0, "longitude"] == -41.8
    assert df.loc[0, "data_inicio_obra"].startswith("2024-02-01T00:00:00")
    assert df.loc[0, "data_atualizacao_obra"].startswith("2024-03-01T00:00:00")
    assert df.loc[0, "execucao_fisica"] == "60,00%"
    assert df.loc[0, "fonte"] == "painel_legado_obras_serpro"
    assert json.loads(df.loc[0, "payload_bruto"])["munic_proponente_obras"] == "Macaé/RJ"


def test_run_salva_cache_quando_coleta_sucesso(monkeypatch) -> None:
    registros = [
        {
            "id_obra_obras": "757206",
            "munic_proponente_obras": "Macaé/RJ",
        }
    ]
    salvar_cache = mock.MagicMock()
    monkeypatch.setattr(legado, "fetch_obras", lambda localidade=None: registros)
    monkeypatch.setattr(legado, "_salvar_cache", salvar_cache)

    df = legado.run("Macae")

    assert len(df) == 1
    salvar_cache.assert_called_once()


def test_run_usa_cache_em_falha(monkeypatch) -> None:
    cache_df = pd.DataFrame([
        {"id_obra": "cache-1", "fonte": "cache"},
    ])

    def boom(localidade=None):
        raise RuntimeError("falha")

    monkeypatch.setattr(legado, "fetch_obras", boom)
    monkeypatch.setattr(legado, "_carregar_cache", lambda: cache_df)

    df = legado.run()

    assert df.equals(cache_df)