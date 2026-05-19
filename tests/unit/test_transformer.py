"""Testes unitários para etl.transformer."""

import pandas as pd
import pytest

from etl import transformer
from etl.transformer import (
    transformar_aditivos,
    transformar_contratos,
    transformar_fornecedores,
    transformar_obras,
    upsert,
)

pytestmark = pytest.mark.unit


# ── helpers ───────────────────────────────────────────────────────────────────


def _raw_contratos(**overrides) -> pd.DataFrame:
    row = {
        "id_contrato": "C-001",
        "fonte": "portal_transparencia_macae_contratos",
        "cnpj_fornecedor": "12.345.678/0001-99",
        "nome_fornecedor": "Construtora Alfa",
        "objeto": "Construção de galeria pluvial",
        "modalidade": "Concorrência",
        "situacao": "Vigente",
        "valor_inicial": 500_000.0,
        "valor_global": 550_000.0,
        "valor_aditivos": 50_000.0,
        "qtd_aditivos": 1,
        "data_assinatura": "2024-01-15",
        "data_inicio_vigencia": "2024-02-01",
        "data_fim_vigencia": "2025-02-01",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _raw_obras_atual(**overrides) -> pd.DataFrame:
    row = {
        "id_obra": "OBR-001",
        "nome_obra": "Reforma da Escola Municipal",
        "situacao": "Em andamento",
        "secretaria": "Educação",
        "bairro": "Centro",
        "endereco": "Rua A, 100",
        "percentual_executado": 45.0,
        "valor_contrato": 1_000_000.0,
        "valor_aditivos": 0.0,
        "data_inicio": "2024-01-01",
        "data_prevista_fim": "2025-01-01",
        "latitude": -22.37,
        "longitude": -41.78,
    }
    row.update(overrides)
    return pd.DataFrame([row])


# ── fornecedores ──────────────────────────────────────────────────────────────


def test_fornecedores_agrupa_por_cnpj():
    df = pd.DataFrame([
        {"cnpj_fornecedor": "11.111.111/0001-11", "nome_fornecedor": "Alfa", "valor_inicial": 100_000.0,
         "data_assinatura": "2023-01-01", "qtd_aditivos": 0},
        {"cnpj_fornecedor": "11.111.111/0001-11", "nome_fornecedor": "Alfa", "valor_inicial": 200_000.0,
         "data_assinatura": "2024-01-01", "qtd_aditivos": 1},
        {"cnpj_fornecedor": "22.222.222/0001-22", "nome_fornecedor": "Beta", "valor_inicial": 50_000.0,
         "data_assinatura": "2024-06-01", "qtd_aditivos": 0},
    ])
    result = transformar_fornecedores(df)
    assert len(result) == 2
    assert set(result["cnpj"]) == {"11.111.111/0001-11", "22.222.222/0001-22"}
    alfa = result[result["cnpj"] == "11.111.111/0001-11"].iloc[0]
    assert alfa["total_contratos"] == 2
    assert alfa["valor_total"] == 300_000.0


def test_fornecedores_calcula_taxa_aditivo():
    df = pd.DataFrame([
        {"cnpj_fornecedor": "11.111.111/0001-11", "nome_fornecedor": "Alfa",
         "qtd_aditivos": 1, "valor_inicial": 100_000.0},
        {"cnpj_fornecedor": "11.111.111/0001-11", "nome_fornecedor": "Alfa",
         "qtd_aditivos": 0, "valor_inicial": 100_000.0},
        {"cnpj_fornecedor": "11.111.111/0001-11", "nome_fornecedor": "Alfa",
         "qtd_aditivos": 0, "valor_inicial": 100_000.0},
        {"cnpj_fornecedor": "11.111.111/0001-11", "nome_fornecedor": "Alfa",
         "qtd_aditivos": 2, "valor_inicial": 100_000.0},
    ])
    result = transformar_fornecedores(df)
    alfa = result.iloc[0]
    # 2 de 4 têm qtd_aditivos > 0 → 50%
    assert alfa["taxa_aditivo"] == 50.0


def test_fornecedores_ignora_cnpj_nulo():
    df = pd.DataFrame([
        {"cnpj_fornecedor": None, "nome_fornecedor": "X", "valor_inicial": 100.0},
        {"cnpj_fornecedor": "", "nome_fornecedor": "Y", "valor_inicial": 100.0},
        {"cnpj_fornecedor": "11.111.111/0001-11", "nome_fornecedor": "Z", "valor_inicial": 100.0},
    ])
    result = transformar_fornecedores(df)
    assert len(result) == 1
    assert result.iloc[0]["cnpj"] == "11.111.111/0001-11"


# ── obras: mapeamentos ────────────────────────────────────────────────────────


def test_obras_raw_obras_atual_mapeamento():
    atual = _raw_obras_atual()
    result = transformar_obras(
        pd.DataFrame(), atual, pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_atual_macae"].iloc[0]
    assert row["nome"] == "Reforma da Escola Municipal"
    assert row["tipo"] == "Obra Municipal"
    assert row["municipio"] == "Macaé"
    assert row["uf"] == "RJ"
    assert row["id_origem"] == "OBR-001"


def test_obras_situacao_saude_mapeada():
    saude = pd.DataFrame([{
        "proposta_id": 42,
        "nome_estabelecimento": "UBS Norte",
        "tipo_obra": "Ampliação",
        "situacao_obra": "Em Execução",
        "bairro": "Norte",
        "logradouro": "Rua B",
        "percentual_executado": 60.0,
        "valor_proposta": 200_000.0,
        "dt_prevista_conclusao": "2025-06-01",
        "dt_conclusao_final": None,
        "latitude": -22.38,
        "longitude": -41.79,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        saude, pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "sismob_cidadao"].iloc[0]
    assert row["situacao"] == "Em andamento"


def test_obras_situacao_georef_mapeada():
    georef = pd.DataFrame([{
        "nome_obra": "Praça do Sol",
        "descricao": "Revitalização",
        "status": "Concluída",
        "secretaria": "Obras",
        "bairro": "Centro",
        "endereco": "Praça Central",
        "latitude": -22.37,
        "longitude": -41.78,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        pd.DataFrame(), georef, pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "egim_google_mymaps"].iloc[0]
    assert row["situacao"] == "Concluída"


def test_obras_paralisadas_sempre_paralisada():
    paralisadas = pd.DataFrame([{
        "id_obra": "P-001",
        "nome_obra": "Viaduto Norte",
        "tipo_obra": "Viaduto",
        "orgao": "Secretaria de Obras",
        "percentual_executado": 30.0,
        "valor_contrato": 5_000_000.0,
        "data_inicio": "2020-01-01",
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), paralisadas,
    )
    row = result[result["fonte_origem"] == "tce_rj_obras_paralisadas"].iloc[0]
    assert row["situacao"] == "Paralisada"


def test_obras_geometry_longitude_primeiro():
    atual = _raw_obras_atual(latitude=-22.37, longitude=-41.78)
    result = transformar_obras(
        pd.DataFrame(), atual, pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_atual_macae"].iloc[0]
    assert row["geometry"] == "POINT(-41.78 -22.37)"


def test_obras_geometry_none_sem_coordenadas():
    atual = _raw_obras_atual(latitude=None, longitude=None)
    result = transformar_obras(
        pd.DataFrame(), atual, pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_atual_macae"].iloc[0]
    assert row["geometry"] is None


def test_obras_prioridade_atual_sobre_legado():
    # mesma id_origem e mesma fonte_origem não conflita por definição,
    # mas fontes diferentes com mesmo id_origem: atual tem prioridade
    atual = pd.DataFrame([{
        "id_obra": "OBRA-XYZ",
        "nome_obra": "Nome do Atual",
        "situacao": "Em andamento",
        "secretaria": None,
        "bairro": None,
        "endereco": None,
        "percentual_executado": None,
        "valor_contrato": None,
        "valor_aditivos": None,
        "data_inicio": None,
        "data_prevista_fim": None,
        "latitude": None,
        "longitude": None,
    }])
    legado = pd.DataFrame([{
        "id_obra": "OBRA-XYZ",
        "nome_obra": "Nome do Legado",
        "situacao": "Concluída",
        "secretaria": None,
        "bairro": None,
        "percentual_executado": None,
        "valor_contrato": None,
        "valor_aditivos": None,
        "valor_final": None,
        "data_inicio": None,
        "data_prevista_fim": None,
        "data_conclusao": None,
        "dias_atraso": None,
        "latitude": None,
        "longitude": None,
    }])
    result = transformar_obras(
        pd.DataFrame(), atual, legado,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    # Atual e Legado têm fonte_origem distintas, então ambas aparecem
    # (dedup é por (fonte_origem, id_origem))
    fontes = set(result["fonte_origem"].tolist())
    assert "painel_obras_atual_macae" in fontes
    assert "painel_obras_legado_macae" in fontes


# ── contratos ─────────────────────────────────────────────────────────────────


def _obras_df_com_id(id_origem: str, fonte_origem: str, obra_id: int = 1) -> pd.DataFrame:
    return pd.DataFrame([{
        "id": obra_id,
        "id_origem": id_origem,
        "fonte_origem": fonte_origem,
        "nome": "Obra X",
    }])


def _fornecedores_df_com_id(cnpj: str, forn_id: int = 10) -> pd.DataFrame:
    return pd.DataFrame([{
        "id": forn_id,
        "cnpj": cnpj,
        "razao_social": "Construtora X",
    }])


def test_contratos_linka_obra_por_id_origem():
    raw = _raw_contratos(id_contrato="C-001", fonte="portal_transparencia_macae_contratos")
    obras = _obras_df_com_id("C-001", "portal_transparencia_macae_contratos", obra_id=99)
    forn = _fornecedores_df_com_id("12.345.678/0001-99")
    result = transformar_contratos(raw, obras, forn)
    assert len(result) == 1
    assert result.iloc[0]["id_obra"] == 99


def test_contratos_descarta_sem_obra():
    raw = _raw_contratos(id_contrato="C-INEXISTENTE", fonte="portal_transparencia_macae_contratos")
    obras = _obras_df_com_id("C-OUTRO", "portal_transparencia_macae_contratos")
    forn = _fornecedores_df_com_id("12.345.678/0001-99")
    result = transformar_contratos(raw, obras, forn)
    assert result.empty


def test_contratos_id_fornecedor_nullable():
    raw = _raw_contratos(cnpj_fornecedor="99.999.999/0001-99")
    obras = _obras_df_com_id("C-001", "portal_transparencia_macae_contratos")
    forn = _fornecedores_df_com_id("11.111.111/0001-11")  # CNPJ diferente
    result = transformar_contratos(raw, obras, forn)
    assert len(result) == 1
    # id_fornecedor deve ser None/NaN quando não encontrado
    assert result.iloc[0]["id_fornecedor"] is None or pd.isna(result.iloc[0]["id_fornecedor"])


def test_contratos_qtd_aditivos_default_zero():
    raw = _raw_contratos(qtd_aditivos=None)
    obras = _obras_df_com_id("C-001", "portal_transparencia_macae_contratos")
    forn = pd.DataFrame()
    result = transformar_contratos(raw, obras, forn)
    assert len(result) == 1
    assert result.iloc[0]["qtd_aditivos"] == 0


# ── aditivos ──────────────────────────────────────────────────────────────────


def _contratos_df(numero: str, fonte: str, contrato_id: int = 100) -> pd.DataFrame:
    return pd.DataFrame([{
        "id": contrato_id,
        "numero": numero,
        "fonte_origem": fonte,
    }])


def test_aditivos_apenas_valor_positivo():
    raw = pd.DataFrame([
        {"id_contrato": "C-001", "fonte": "fonte_a", "valor_aditivos": 50_000.0},
        {"id_contrato": "C-002", "fonte": "fonte_a", "valor_aditivos": 0.0},
        {"id_contrato": "C-003", "fonte": "fonte_a", "valor_aditivos": None},
    ])
    contratos = pd.DataFrame([
        {"id": 1, "numero": "C-001", "fonte_origem": "fonte_a"},
        {"id": 2, "numero": "C-002", "fonte_origem": "fonte_a"},
        {"id": 3, "numero": "C-003", "fonte_origem": "fonte_a"},
    ])
    result = transformar_aditivos(raw, contratos)
    assert len(result) == 1
    assert result.iloc[0]["valor"] == 50_000.0


def test_aditivos_um_por_contrato():
    raw = pd.DataFrame([
        {"id_contrato": "C-001", "fonte": "fonte_a", "valor_aditivos": 10_000.0},
    ])
    contratos = _contratos_df("C-001", "fonte_a", contrato_id=55)
    result = transformar_aditivos(raw, contratos)
    assert len(result) == 1
    assert result.iloc[0]["id_contrato"] == 55
    assert result.iloc[0]["tipo"] == "Consolidado"
    assert result.iloc[0]["prazo_dias"] is None


# ── pipeline run() ────────────────────────────────────────────────────────────


def test_run_ordem_fk_respeitada(mocker):
    """Garante que fornecedores e obras são processados antes de contratos/aditivos."""
    call_order = []

    def fake_transformar_fornecedores(*a, **kw):
        call_order.append("fornecedores")
        return pd.DataFrame()

    def fake_transformar_obras(*a, **kw):
        call_order.append("obras")
        return pd.DataFrame()

    def fake_transformar_contratos(*a, **kw):
        call_order.append("contratos")
        return pd.DataFrame()

    def fake_transformar_aditivos(*a, **kw):
        call_order.append("aditivos")
        return pd.DataFrame()

    mocker.patch.object(transformer, "get_client", return_value=mocker.MagicMock())
    mocker.patch.object(transformer, "ler_raw", return_value=pd.DataFrame())
    mocker.patch.object(transformer, "transformar_fornecedores", side_effect=fake_transformar_fornecedores)
    mocker.patch.object(transformer, "transformar_obras", side_effect=fake_transformar_obras)
    mocker.patch.object(transformer, "transformar_contratos", side_effect=fake_transformar_contratos)
    mocker.patch.object(transformer, "transformar_aditivos", side_effect=fake_transformar_aditivos)
    mocker.patch.object(transformer, "upsert", return_value=0)

    transformer.run()

    assert call_order == ["fornecedores", "obras", "contratos", "aditivos"]


def test_run_falha_parcial_continua(mocker):
    """Falha em fornecedores não deve abortar obras, contratos ou aditivos."""
    etapas_executadas = []

    mocker.patch.object(transformer, "get_client", return_value=mocker.MagicMock())
    mocker.patch.object(transformer, "ler_raw", return_value=pd.DataFrame())
    mocker.patch.object(
        transformer, "transformar_fornecedores",
        side_effect=RuntimeError("erro simulado"),
    )
    mocker.patch.object(
        transformer, "transformar_obras",
        side_effect=lambda *a, **kw: etapas_executadas.append("obras") or pd.DataFrame(),
    )
    mocker.patch.object(
        transformer, "transformar_contratos",
        side_effect=lambda *a, **kw: etapas_executadas.append("contratos") or pd.DataFrame(),
    )
    mocker.patch.object(
        transformer, "transformar_aditivos",
        side_effect=lambda *a, **kw: etapas_executadas.append("aditivos") or pd.DataFrame(),
    )
    mocker.patch.object(transformer, "upsert", return_value=0)

    resultado = transformer.run()

    assert "obras" in etapas_executadas
    assert "contratos" in etapas_executadas
    assert "aditivos" in etapas_executadas
    assert resultado["fornecedores"] == 0
