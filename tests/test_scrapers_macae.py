import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrappers.macae import painel_atual as pa


def _df_normalizado_minimo() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id_obra": ["obra-1"],
            "nome_obra": ["Obra teste"],
            "situacao": ["em andamento"],
            "percentual_executado": [45.0],
            "valor_contrato": [1500.0],
            "data_inicio": ["2026-01-10T00:00:00+00:00"],
            "data_prevista_fim": ["2026-12-10T00:00:00+00:00"],
            "secretaria": ["Obras"],
            "bairro": ["Centro"],
            "fonte": ["painel_obras_atual_macae"],
            "coletado_em": ["2026-04-21T00:00:00+00:00"],
            "payload_bruto": ["{}"],
        }
    )


def test_ler_arquivo_csv_utf8():
    """_ler_arquivo() deve parsear CSV UTF-8 com separador ponto-e-vírgula."""
    conteudo = "id_obra;nome_obra;situacao\n1;Obra A;Em andamento".encode("utf-8")
    df = pa._ler_arquivo(conteudo, content_type="text/csv")

    assert len(df) == 1
    assert list(df.columns) == ["id_obra", "nome_obra", "situacao"]


def test_ler_arquivo_csv_latin1():
    """_ler_arquivo() deve parsear CSV latin-1 com separador vírgula."""
    conteudo = "id_obra,nome_obra,situacao\n2,Pavimentação,Em andamento".encode("latin-1")
    df = pa._ler_arquivo(conteudo, content_type="text/csv")

    assert len(df) == 1
    assert "nome_obra" in df.columns


def test_filtrar_obras_ativas_remove_canceladas():
    """_filtrar_obras_ativas() deve remover obras canceladas."""
    df = pd.DataFrame(
        {
            "id_obra": ["1", "2"],
            "situacao": ["Cancelada", "Em andamento"],
            "data_inicio": ["2020-01-01", "2020-01-01"],
            "data_prevista_fim": ["2020-01-01", "2020-01-01"],
        }
    )

    filtrado = pa._filtrar_obras_ativas(df)

    assert len(filtrado) == 1
    assert filtrado.iloc[0]["id_obra"] == "2"


def test_filtrar_obras_ativas_mantem_andamento():
    """_filtrar_obras_ativas() deve manter obras em andamento."""
    df = pd.DataFrame(
        {
            "id_obra": ["1"],
            "situacao": ["Em andamento"],
            "data_inicio": [None],
            "data_prevista_fim": [None],
        }
    )

    filtrado = pa._filtrar_obras_ativas(df)
    assert len(filtrado) == 1


def test_normalizar_converte_valor_monetario():
    """normalizar() deve converter 'R$ 1.234.567,89' para 1234567.89."""
    df = pd.DataFrame(
        {
            "Nome da Obra": ["Reforma da praça"],
            "Situação": ["Em andamento"],
            "Valor do Contrato (R$)": ["R$ 1.234.567,89"],
        }
    )

    normalizado = pa.normalizar(df)
    assert normalizado.iloc[0]["valor_contrato"] == 1234567.89


def test_normalizar_converte_data_br():
    """normalizar() deve converter '15/03/2026' para ISO 8601 UTC."""
    df = pd.DataFrame(
        {
            "Nome da Obra": ["Obra de drenagem"],
            "Situação": ["Vigente"],
            "Data Início": ["15/03/2026"],
        }
    )

    normalizado = pa.normalizar(df)
    data_inicio = normalizado.iloc[0]["data_inicio"]
    assert data_inicio == "2026-03-15T00:00:00+00:00"


def test_run_usa_cache_quando_todas_falham(mocker):
    """run() deve carregar cache quando todas as estratégias falham."""
    mocker.patch("scrappers.macae.painel_atual._tentar_csv_direto", return_value=None)
    mocker.patch("scrappers.macae.painel_atual._tentar_portal_transparencia", return_value=None)
    mocker.patch("scrappers.macae.painel_atual._tentar_tce_rj_recente", return_value=None)
    carregar_cache = mocker.patch(
        "scrappers.macae.painel_atual._carregar_cache",
        return_value=_df_normalizado_minimo(),
    )

    df = pa.run()

    carregar_cache.assert_called_once()
    assert not df.empty
    assert len(df) == 1


def test_run_salva_cache_apos_sucesso(mocker, tmp_path):
    """run() deve salvar cache após coleta bem-sucedida."""
    _ = tmp_path

    mocker.patch(
        "scrappers.macae.painel_atual._tentar_csv_direto",
        return_value=pd.DataFrame(
            {
                "id_obra": ["obra-1"],
                "nome_obra": ["Obra pavimentacao"],
                "situacao": ["Em andamento"],
                "valor_contrato": ["R$ 10.000,00"],
                "data_inicio": ["01/01/2026"],
                "data_prevista_fim": ["01/01/2027"],
                "secretaria": ["Obras"],
                "bairro": ["Ajuda"],
            }
        ),
    )
    mocker.patch("scrappers.macae.painel_atual._tentar_portal_transparencia", return_value=None)
    mocker.patch("scrappers.macae.painel_atual._tentar_tce_rj_recente", return_value=None)
    salvar_cache = mocker.patch("scrappers.macae.painel_atual._salvar_cache")
    carregar_cache = mocker.patch("scrappers.macae.painel_atual._carregar_cache", return_value=None)

    df = pa.run()

    assert not df.empty
    salvar_cache.assert_called_once()
    carregar_cache.assert_not_called()