"""Testes unitários para etl.transformer."""

import pandas as pd
import pytest

from etl import transformer
from etl.transformer import (
    _ajustar_percentual,
    _calcular_dias_atraso,
    _col,
    _contem_palavra_obra,
    _gerar_geometry,
    _get,
    _nan_to_none,
    _obras_de_contratos,
    _prioridade,
    ler_raw,
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


def test_obras_atual_propaga_chave_juncao():
    """_obras_de_atual deve propagar cnpj_executora, num_contrato e num_licitacao."""
    atual = _raw_obras_atual(
        cnpj_executora="98765432000111",
        num_contrato="022/2026SEMINF",
        num_licitacao="PE-007/2026",
    )
    result = transformar_obras(
        pd.DataFrame(), atual, pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_atual_macae"].iloc[0]
    assert row["cnpj_executora"] == "98765432000111"
    assert row["num_contrato"] == "022/2026SEMINF"
    assert row["num_licitacao"] == "PE-007/2026"


def test_obras_legado_propaga_chave_juncao():
    """_obras_de_legado deve propagar cnpj_executora, num_contrato e num_licitacao."""
    legado = pd.DataFrame([{
        "id_obra": "757206",
        "nome_obra": "Reforma Escola X",
        "situacao": "Concluída",
        "cnpj_executora": "12345678000199",
        "num_contrato": "010/2025SEMINF",
        "num_licitacao": "PL-042/2024",
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), legado,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_legado_macae"].iloc[0]
    assert row["cnpj_executora"] == "12345678000199"
    assert row["num_contrato"] == "010/2025SEMINF"
    assert row["num_licitacao"] == "PL-042/2024"


def test_obras_enriquece_aditivos_federais():
    """valor_aditivos do legado é preenchido pelos aditivos federais (num_licitacao==nr_convenio)."""
    legado = pd.DataFrame([
        {"id_obra": "A", "nome_obra": "Obra A", "situacao": "Concluída", "num_licitacao": "757206"},
        {"id_obra": "B", "nome_obra": "Obra B", "situacao": "Concluída", "num_licitacao": "800000"},
        {"id_obra": "C", "nome_obra": "Obra C", "situacao": "Concluída", "num_licitacao": "999999"},
    ])
    federais = pd.DataFrame([
        {"nr_convenio": "757206", "valor_aditivos": 0.0},      # vigência → 0 (informativo)
        {"nr_convenio": "800000", "valor_aditivos": 25000.0},  # aditivo financeiro
    ])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), legado,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        federais,
    )
    by = result.set_index("num_licitacao")
    assert by.loc["757206", "valor_aditivos"] == 0.0
    assert by.loc["800000", "valor_aditivos"] == 25000.0
    assert pd.isna(by.loc["999999", "valor_aditivos"])  # sem convênio federal → permanece nulo


def test_obras_enriquece_qtd_aditivos():
    """qtd_aditivos do convênio federal é propagado para obras."""
    legado = pd.DataFrame([{"id_obra": "A", "nome_obra": "Obra A", "situacao": "Concluída",
                            "num_licitacao": "775661"}])
    federais = pd.DataFrame([{"nr_convenio": "775661", "qtd_aditivos": 4, "valor_aditivos": 0.0,
                              "situacao": "Prestação de Contas Concluída"}])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), legado,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), federais,
    )
    assert result.iloc[0]["qtd_aditivos"] == 4


def test_obras_enriquece_conclusao_e_atraso_diario():
    """Convênio concluído: data_conclusao=fim vigência, prazo=fim original → atraso diário."""
    legado = pd.DataFrame([{"id_obra": "A", "nome_obra": "Obra A", "situacao": "Concluída",
                            "num_licitacao": "775661"}])
    federais = pd.DataFrame([{
        "nr_convenio": "775661", "qtd_aditivos": 4, "valor_aditivos": 0.0,
        "situacao": "Prestação de Contas Concluída",
        "data_fim_vigencia": "2019-06-30", "data_fim_vigencia_original": "2014-06-30",
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), legado,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), federais,
    )
    row = result.iloc[0]
    assert str(row["data_conclusao"]).startswith("2019-06-30")
    assert str(row["data_prevista_fim"]).startswith("2014-06-30")
    assert row["dias_atraso"] > 1800  # ~5 anos de atraso (2014→2019)


def test_obras_aditivo_federal_anulado_nao_preenche_conclusao():
    """Convênio anulado não deve gerar data_conclusao (não foi concluído)."""
    legado = pd.DataFrame([{"id_obra": "A", "nome_obra": "Obra A", "situacao": "Cancelada",
                            "num_licitacao": "913439"}])
    federais = pd.DataFrame([{
        "nr_convenio": "913439", "qtd_aditivos": 0, "valor_aditivos": None,
        "situacao": "Convênio Anulado",
        "data_fim_vigencia": "2024-10-29", "data_fim_vigencia_original": "2024-10-29",
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), legado,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), federais,
    )
    assert pd.isna(result.iloc[0]["data_conclusao"])


def test_obras_legado_propaga_percentual_financeiro():
    """_obras_de_legado deve propagar percentual_executado_financeiro."""
    legado = pd.DataFrame([{
        "id_obra": "757206",
        "nome_obra": "Reforma Escola X",
        "situacao": "Concluída",
        "percentual_executado": 75.0,
        "percentual_executado_financeiro": 80.0,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), legado,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_legado_macae"].iloc[0]
    assert row["percentual_executado_financeiro"] == 80.0


def test_obras_legado_propaga_ano_conclusao():
    """_obras_de_legado deve propagar ano_conclusao (única pista de conclusão do legado)."""
    legado = pd.DataFrame([{
        "id_obra": "757206",
        "nome_obra": "Reforma Escola X",
        "situacao": "Concluída",
        "ano_conclusao": 2014,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), legado,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_legado_macae"].iloc[0]
    assert row["ano_conclusao"] == 2014


def test_obras_situacao_saude_mapeada():
    saude = pd.DataFrame([{
        "proposta_id": 42,
        "nome_estabelecimento": "UBS Norte",
        "tipo_obra": "Ampliação",
        "situacao": "Em Execução",
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


def test_obras_saude_usa_prevista_conclusao_final_para_prazo():
    """data_prevista_fim deve usar dt_prevista_conclusao_final (preenchido no SISMOB),
    com fallback para dt_prevista_conclusao, habilitando o cálculo de dias_atraso."""
    saude = pd.DataFrame([{
        "proposta_id": 7,
        "nome_estabelecimento": "UBS Lagomar",
        "tipo_obra": "Construção",
        "situacao": "Concluída",
        "percentual_executado": 100.0,
        "valor_proposta": 500_000.0,
        "dt_prevista_conclusao": None,                 # quase sempre nulo no SISMOB
        "dt_prevista_conclusao_final": "2014-07-13",   # prazo de fato preenchido
        "dt_conclusao_final": "2014-08-05",            # conclusão real (23 dias depois)
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        saude, pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "sismob_cidadao"].iloc[0]
    assert row["data_prevista_fim"] == "2014-07-13"
    assert row["dias_atraso"] == 23


def test_previsao_termino_mes_ano_para_iso():
    """'Dezembro/2023' deve virar timestamp ISO do primeiro dia do mês."""
    from etl.transformer import _previsao_termino_para_iso
    assert _previsao_termino_para_iso("Dezembro/2023", None) == "2023-12-01T00:00:00+00:00"
    assert _previsao_termino_para_iso("Março/2022", None) == "2022-03-01T00:00:00+00:00"


def test_previsao_termino_prazo_dias_soma_data_inicio():
    """'360 DIAS' deve somar ao data_inicio."""
    from etl.transformer import _previsao_termino_para_iso
    r = _previsao_termino_para_iso("360 DIAS", "2022-01-01T00:00:00+00:00")
    assert r is not None and r.startswith("2022-12-27")


def test_previsao_termino_invalido_retorna_none():
    """Texto não-parseável deve virar None, nunca quebrar o upsert timestamp."""
    from etl.transformer import _previsao_termino_para_iso
    assert _previsao_termino_para_iso("360 DIAS", None) is None  # sem data_inicio
    assert _previsao_termino_para_iso("-", None) is None
    assert _previsao_termino_para_iso(None, None) is None
    assert _previsao_termino_para_iso("", None) is None


def test_obras_georef_data_prevista_fim_nunca_texto_livre():
    """Regressão: previsao_termino texto livre não pode ir cru para data_prevista_fim."""
    georef = pd.DataFrame([{
        "nome_obra": "Obra X", "descricao": "d", "situacao": "em andamento",
        "secretaria": "Obras", "bairro": "Centro", "endereco": "Rua A",
        "percentual": 50.0, "valor": "R$ 100",
        "data_inicio": "2022-01-01T00:00:00+00:00",
        "previsao_termino": "Dezembro/2023",  # texto livre
        "latitude": -22.3, "longitude": -41.7,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        pd.DataFrame(), georef, pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "egim_google_mymaps"].iloc[0]
    # Deve ser ISO válido, não "Dezembro/2023"
    assert row["data_prevista_fim"] == "2023-12-01T00:00:00+00:00"


def test_obras_georef_mapeia_valor_percentual_datas():
    """_obras_de_georef deve mapear percentual, valor, data_inicio e data_prevista_fim."""
    georef = pd.DataFrame([{
        "nome_obra": "Reforma Teatro",
        "descricao": "Reforma geral",
        "situacao": "em andamento",
        "secretaria": "Obras",
        "bairro": "Centro",
        "endereco": "Rua A",
        "percentual": 65.0,
        "valor": "R$ 2.378.752",
        "data_inicio": "2022-07-01T00:00:00+00:00",
        "previsao_termino": "360 DIAS",
        "latitude": -22.377,
        "longitude": -41.777,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        pd.DataFrame(), georef, pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "egim_google_mymaps"].iloc[0]
    assert row["percentual_executado"] == 65.0
    assert row["valor_contrato"] == pytest.approx(2_378_752.0)
    assert row["data_inicio"] == "2022-07-01T00:00:00+00:00"
    # "360 DIAS" + data_inicio (01/07/2022) → 2023-06-26 (timestamp ISO, não texto)
    assert row["data_prevista_fim"].startswith("2023-06-26")


def test_obras_situacao_georef_mapeada():
    georef = pd.DataFrame([{
        "nome_obra": "Praça do Sol",
        "descricao": "Revitalização",
        "situacao": "Concluída",
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


def test_obras_situacao_cadastrada_vira_fase_planejamento():
    atual = _raw_obras_atual(situacao="Cadastrada")
    result = transformar_obras(
        pd.DataFrame(), atual, pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_atual_macae"].iloc[0]
    assert row["situacao"] == "Em fase de planejamento"


def test_obras_situacao_em_execucao_vira_em_andamento():
    legado = pd.DataFrame([{
        "id_obra": "L-001",
        "nome_obra": "Obra Legado",
        "situacao": "Em Execução",
        "secretaria": None, "bairro": None, "percentual_executado": None,
        "valor_contrato": None, "valor_aditivos": None, "valor_final": None,
        "data_inicio": None, "data_prevista_fim": None, "data_conclusao": None,
        "dias_atraso": None, "latitude": None, "longitude": None,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), legado,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_legado_macae"].iloc[0]
    assert row["situacao"] == "Em andamento"


def test_obras_legado_usa_campo_objeto_quando_disponivel():
    """_obras_de_legado deve usar o campo 'objeto' real, não nome_obra, para obras.objeto."""
    legado_df = pd.DataFrame([{
        "id_obra": "L-999",
        "nome_obra": "CONSTRUCAO",
        "objeto": "IMPLANTACAO DE UBS NO BAIRRO NORTE",
        "situacao": "Concluída",
        "secretaria": None, "bairro": None, "percentual_executado": None,
        "valor_contrato": None, "valor_aditivos": None, "valor_final": None,
        "data_inicio": None, "data_prevista_fim": None, "data_conclusao": None,
        "dias_atraso": None, "latitude": None, "longitude": None,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), legado_df,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_legado_macae"].iloc[0]
    assert row["objeto"] == "IMPLANTACAO DE UBS NO BAIRRO NORTE"
    assert row["nome"] == "CONSTRUCAO"


def test_obras_legado_objeto_fallback_para_nome_obra_quando_ausente():
    """_obras_de_legado deve usar nome_obra como fallback quando objeto é None."""
    legado_df = pd.DataFrame([{
        "id_obra": "L-998",
        "nome_obra": "Obra sem objeto",
        "objeto": None,
        "situacao": "Em andamento",
        "secretaria": None, "bairro": None, "percentual_executado": None,
        "valor_contrato": None, "valor_aditivos": None, "valor_final": None,
        "data_inicio": None, "data_prevista_fim": None, "data_conclusao": None,
        "dias_atraso": None, "latitude": None, "longitude": None,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), legado_df,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_legado_macae"].iloc[0]
    assert row["objeto"] == "Obra sem objeto"


def test_obras_situacao_em_funcionamento_vira_concluida():
    saude = pd.DataFrame([{
        "proposta_id": 99,
        "nome_estabelecimento": "UPA Sul",
        "tipo_obra": "Reforma",
        "situacao": "Em funcionamento",
        "bairro": None, "logradouro": None, "percentual_executado": None,
        "valor_proposta": None, "dt_prevista_conclusao": None,
        "dt_conclusao_final": None, "latitude": None, "longitude": None,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        saude, pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "sismob_cidadao"].iloc[0]
    assert row["situacao"] == "Concluída"


def _df_situacao(situacao: str, percentual=None, data_inicio=None, data_prevista_fim=None):
    return pd.DataFrame([{
        "situacao": situacao,
        "percentual_executado": percentual,
        "data_inicio": data_inicio,
        "data_prevista_fim": data_prevista_fim,
    }])


def test_ajustar_percentual_concluida_vira_100():
    df = _df_situacao("Concluída")
    assert _ajustar_percentual(df).iloc[0]["percentual_executado"] == 100.0


def test_ajustar_percentual_em_funcionamento_vira_100():
    df = _df_situacao("Em funcionamento")
    assert _ajustar_percentual(df).iloc[0]["percentual_executado"] == 100.0


def test_ajustar_percentual_cancelada_vira_0():
    df = _df_situacao("Cancelada")
    assert _ajustar_percentual(df).iloc[0]["percentual_executado"] == 0.0


def test_ajustar_percentual_rescindida_vira_0():
    df = _df_situacao("Rescindida")
    assert _ajustar_percentual(df).iloc[0]["percentual_executado"] == 0.0


def test_ajustar_percentual_planejada_vira_0():
    df = _df_situacao("Em fase de planejamento")
    assert _ajustar_percentual(df).iloc[0]["percentual_executado"] == 0.0


def test_ajustar_percentual_cadastrada_vira_0():
    df = _df_situacao("Cadastrada")
    assert _ajustar_percentual(df).iloc[0]["percentual_executado"] == 0.0


def test_ajustar_percentual_nao_sobrescreve_valor_existente():
    """Se percentual já está preenchido, não deve ser alterado."""
    df = _df_situacao("Concluída", percentual=75.0)
    assert _ajustar_percentual(df).iloc[0]["percentual_executado"] == 75.0


def test_ajustar_percentual_andamento_sem_datas_vira_0():
    """Em andamento sem datas → 0 (default final)."""
    df = _df_situacao("Em andamento")
    assert _ajustar_percentual(df).iloc[0]["percentual_executado"] == 0.0


def test_ajustar_percentual_andamento_prazo_expirado_vira_99():
    """Obra em andamento com prazo expirado deve ficar em 99, não 100."""
    df = _df_situacao("Em andamento",
                      data_inicio="2020-01-01T00:00:00+00:00",
                      data_prevista_fim="2021-01-01T00:00:00+00:00")
    result = _ajustar_percentual(df).iloc[0]["percentual_executado"]
    assert result == 99.0


def test_ajustar_percentual_andamento_futuro_entre_0_e_99():
    """Obra em andamento com prazo futuro deve ter percentual entre 0 e 99."""
    from datetime import datetime, timezone, timedelta
    ini = (datetime.now(timezone.utc) - timedelta(days=180)).isoformat()
    fim = (datetime.now(timezone.utc) + timedelta(days=180)).isoformat()
    df = _df_situacao("Em andamento", data_inicio=ini, data_prevista_fim=fim)
    result = _ajustar_percentual(df).iloc[0]["percentual_executado"]
    assert 0 < result < 99


# ── Estratégia 1: dias_atraso ─────────────────────────────────────────────────

def test_dias_atraso_concluida_com_atraso():
    """Concluída após o prazo: dias_atraso = conclusão - prazo previsto."""
    df = pd.DataFrame([{
        "situacao": "Concluída",
        "data_prevista_fim": "2023-01-01T00:00:00+00:00",
        "data_conclusao": "2023-01-31T00:00:00+00:00",
    }])
    assert _calcular_dias_atraso(df).iloc[0]["dias_atraso"] == 30


def test_dias_atraso_concluida_no_prazo_zero():
    """Concluída antes/no prazo: dias_atraso = 0 (adiantamento não é atraso)."""
    df = pd.DataFrame([{
        "situacao": "Concluída",
        "data_prevista_fim": "2023-02-01T00:00:00+00:00",
        "data_conclusao": "2023-01-01T00:00:00+00:00",
    }])
    assert _calcular_dias_atraso(df).iloc[0]["dias_atraso"] == 0


def test_dias_atraso_andamento_prazo_vencido():
    """Em andamento com prazo vencido: dias desde o vencimento até hoje."""
    from datetime import datetime, timezone, timedelta
    fim = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
    df = pd.DataFrame([{
        "situacao": "Em andamento",
        "data_prevista_fim": fim,
        "data_conclusao": None,
    }])
    r = _calcular_dias_atraso(df).iloc[0]["dias_atraso"]
    assert 98 <= r <= 102  # ~100 dias


def test_dias_atraso_sem_prazo_fica_nulo():
    """Sem data_prevista_fim não há como calcular — fica nulo."""
    df = pd.DataFrame([{
        "situacao": "Em andamento",
        "data_prevista_fim": None,
        "data_conclusao": None,
    }])
    assert pd.isna(_calcular_dias_atraso(df).iloc[0]["dias_atraso"])


# ── Estratégia 2: secretaria SISMOB ───────────────────────────────────────────

def test_obras_saude_secretaria_saude():
    """Obras do SISMOB devem ter secretaria='Saúde'."""
    saude = pd.DataFrame([{
        "proposta_id": 1, "nome_estabelecimento": "UBS Norte", "tipo_obra": "Construção",
        "situacao": "Em Execução", "bairro": "Norte", "logradouro": "Rua B",
        "percentual_executado": 50.0, "valor_proposta": 100.0,
        "dt_prevista_conclusao": None, "dt_conclusao_final": None,
        "latitude": -22.3, "longitude": -41.7,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        saude, pd.DataFrame(), pd.DataFrame(),
    )
    assert result[result["fonte_origem"] == "sismob_cidadao"].iloc[0]["secretaria"] == "Saúde"


# ── Estratégia 3: data_inicio ← data_assinatura ───────────────────────────────

def test_obras_contratos_extrai_bairro_do_objeto():
    """bairro deve ser extraído de 'no BAIRRO X' no objeto do contrato."""
    raw = pd.DataFrame([{
        "id_contrato": "C-1", "fonte": "portal_transparencia_macae_contratos",
        "objeto": "CONSTRUÇÃO DE PRAÇA NO BAIRRO LAGOMAR, MACAÉ/RJ, COM MÃO DE OBRA",
    }])
    result = _obras_de_contratos(raw)
    assert result.iloc[0]["bairro"] == "Lagomar"


def test_obras_contratos_extrai_logradouro_do_objeto():
    """endereco deve ser extraído de 'LOCALIZADA NA RUA X'."""
    raw = pd.DataFrame([{
        "id_contrato": "C-1", "fonte": "portal_transparencia_macae_contratos",
        "objeto": "REFORMA DA PRAÇA, LOCALIZADA NA RUA ALFREDO TANOS, MACAÉ/RJ",
    }])
    result = _obras_de_contratos(raw)
    assert result.iloc[0]["endereco"] == "RUA ALFREDO TANOS"


def test_obras_contratos_sem_local_no_objeto_fica_none():
    """Objeto sem menção a bairro/endereço → bairro e endereco nulos."""
    raw = pd.DataFrame([{
        "id_contrato": "C-1", "fonte": "tce_rj_contratos",
        "objeto": "CONTRATAÇÃO DE EMPRESA PARA REFORMA DE ESCOLA MUNICIPAL",
    }])
    result = _obras_de_contratos(raw)
    assert result.iloc[0]["bairro"] is None
    assert result.iloc[0]["endereco"] is None


def test_obras_contratos_data_inicio_fallback_assinatura():
    """Sem data_inicio_vigencia, usa data_assinatura como início."""
    raw = pd.DataFrame([{
        "id_contrato": "C-1", "objeto": "Reforma de escola", "fonte": "tce_rj_contratos",
        "data_inicio_vigencia": None, "data_assinatura": "2024-03-10T00:00:00+00:00",
    }])
    result = _obras_de_contratos(raw)
    assert result.iloc[0]["data_inicio"] == "2024-03-10T00:00:00+00:00"


def test_obras_contratos_data_inicio_prefere_vigencia():
    """Com data_inicio_vigencia, ela tem prioridade sobre assinatura."""
    raw = pd.DataFrame([{
        "id_contrato": "C-1", "objeto": "Reforma de escola", "fonte": "tce_rj_contratos",
        "data_inicio_vigencia": "2024-04-01T00:00:00+00:00",
        "data_assinatura": "2024-03-10T00:00:00+00:00",
    }])
    result = _obras_de_contratos(raw)
    assert result.iloc[0]["data_inicio"] == "2024-04-01T00:00:00+00:00"


def test_obras_concluida_percentual_zero_vira_100():
    legado = pd.DataFrame([{
        "id_obra": "L-002",
        "nome_obra": "Obra Concluida",
        "situacao": "Concluída",
        "secretaria": None,
        "bairro": None,
        "percentual_executado": 0.0,
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
        pd.DataFrame(), pd.DataFrame(), legado,
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_legado_macae"].iloc[0]
    assert row["percentual_executado"] == 100.0


def test_obras_situacao_obra_cancelada_vira_cancelada():
    saude = pd.DataFrame([{
        "proposta_id": 88,
        "nome_estabelecimento": "UBS Leste",
        "tipo_obra": "Ampliação",
        "situacao": "Obra cancelada",
        "bairro": None, "logradouro": None, "percentual_executado": None,
        "valor_proposta": None, "dt_prevista_conclusao": None,
        "dt_conclusao_final": None, "latitude": None, "longitude": None,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        saude, pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "sismob_cidadao"].iloc[0]
    assert row["situacao"] == "Cancelada"


def test_obras_situacao_null_vira_indefinido():
    atual = _raw_obras_atual(situacao=None)
    result = transformar_obras(
        pd.DataFrame(), atual, pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_atual_macae"].iloc[0]
    assert row["situacao"] == "Indefinido"


def test_obras_situacao_rescindida_normalizada():
    atual = _raw_obras_atual(situacao="rescindido")
    result = transformar_obras(
        pd.DataFrame(), atual, pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_atual_macae"].iloc[0]
    assert row["situacao"] == "Rescindida"


def test_obras_situacao_nao_mapeada_preserva_original():
    atual = _raw_obras_atual(situacao="Em vistoria")
    result = transformar_obras(
        pd.DataFrame(), atual, pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    row = result[result["fonte_origem"] == "painel_obras_atual_macae"].iloc[0]
    assert row["situacao"] == "Em vistoria"


def test_obras_paralisadas_calcula_percentual_financeiro():
    """percentual_executado deve ser calculado de valor_pago_obra/valor_contrato."""
    paralisadas = pd.DataFrame([{
        "id_obra": "P-001",
        "nome_obra": "FUNDO MUN SAÚDE",
        "tipo_obra": "SAÚDE (UBS)",
        "orgao": None,
        "funcao_governo": "SAÚDE",
        "percentual_executado": None,
        "valor_contrato": 1_000_000.0,
        "valor_pago_obra": 520_000.0,
        "data_inicio": "2015-10-31",
        "data_paralisacao": "2016-12-31",
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), paralisadas,
    )
    row = result[result["fonte_origem"] == "tce_rj_obras_paralisadas"].iloc[0]
    assert row["percentual_executado"] == pytest.approx(52.0)
    assert row["valor_final"] == pytest.approx(520_000.0)
    assert row["data_prevista_fim"] == "2016-12-31"
    assert row["secretaria"] == "SAÚDE"


def test_obras_paralisadas_secretaria_fallback_nome_obra():
    """secretaria deve usar nome_obra quando orgao e funcao_governo são nulos."""
    paralisadas = pd.DataFrame([{
        "id_obra": "P-002",
        "nome_obra": "PREFEITURA MACAE",
        "tipo_obra": None,
        "orgao": None,
        "funcao_governo": None,
        "percentual_executado": None,
        "valor_contrato": 500_000.0,
        "valor_pago_obra": None,
        "data_inicio": None,
        "data_paralisacao": None,
    }])
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), paralisadas,
    )
    row = result[result["fonte_origem"] == "tce_rj_obras_paralisadas"].iloc[0]
    assert row["secretaria"] == "PREFEITURA MACAE"


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


# ── funções auxiliares ────────────────────────────────────────────────────────


def test_contem_palavra_obra_nao_string():
    assert _contem_palavra_obra(None) is False
    assert _contem_palavra_obra(123) is False


def test_contem_palavra_obra_positivo():
    assert _contem_palavra_obra("Construção de galeria pluvial") is True


def test_col_coluna_ausente():
    df = pd.DataFrame([{"a": 1}])
    assert _col(df, "inexistente") is None


def test_col_coluna_presente():
    df = pd.DataFrame([{"a": 1}])
    result = _col(df, "a")
    assert result is not None
    assert list(result) == [1]


def test_get_coluna_ausente_retorna_serie_none():
    df = pd.DataFrame([{"a": 1}, {"a": 2}])
    s = _get(df, "inexistente")
    assert list(s) == [None, None]


def test_prioridade_fonte_conhecida():
    assert _prioridade("painel_obras_atual_macae") == 0
    assert _prioridade("tce_rj_obras_paralisadas") == 8


def test_prioridade_fonte_desconhecida():
    from etl.transformer import PRIORIDADE_FONTES
    resultado = _prioridade("fonte_que_nao_existe")
    assert resultado == len(PRIORIDADE_FONTES)


def test_gerar_geometry_nan_retorna_none():
    import math
    assert _gerar_geometry(float("nan"), -41.78) is None
    assert _gerar_geometry(-22.37, float("nan")) is None


def test_gerar_geometry_invalido_retorna_none():
    assert _gerar_geometry("abc", -41.78) is None


def test_nan_to_none_none():
    assert _nan_to_none(None) is None


def test_nan_to_none_nan():
    import math
    assert _nan_to_none(float("nan")) is None


def test_nan_to_none_valor_normal():
    assert _nan_to_none(42) == 42
    assert _nan_to_none("texto") == "texto"


def test_nan_to_none_lista_nao_e_nan():
    # listas não são NaN — retorna o próprio valor
    val = [1, 2, 3]
    assert _nan_to_none(val) == val


# ── ler_raw ───────────────────────────────────────────────────────────────────


def test_ler_raw_sucesso(mocker):
    fake_client = mocker.MagicMock()
    fake_client.table.return_value.select.return_value.execute.return_value.data = [
        {"id": 1, "nome": "Obra A"}
    ]
    df = ler_raw(fake_client, "raw_obras_atual")
    assert len(df) == 1
    assert df.iloc[0]["nome"] == "Obra A"


def test_ler_raw_dados_vazios(mocker):
    fake_client = mocker.MagicMock()
    fake_client.table.return_value.select.return_value.execute.return_value.data = []
    df = ler_raw(fake_client, "raw_obras_atual")
    assert df.empty


def test_ler_raw_erro_retorna_dataframe_vazio(mocker):
    fake_client = mocker.MagicMock()
    fake_client.table.side_effect = RuntimeError("connection refused")
    df = ler_raw(fake_client, "raw_obras_atual")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ── fornecedores: branches não cobertos ──────────────────────────────────────


def test_fornecedores_vazio_retorna_vazio():
    result = transformar_fornecedores(pd.DataFrame())
    assert result.empty


def test_fornecedores_sem_coluna_cnpj_retorna_vazio():
    df = pd.DataFrame([{"nome": "X", "valor": 100}])
    result = transformar_fornecedores(df)
    assert result.empty


def test_fornecedores_sem_nome_fornecedor_usa_fallback():
    df = pd.DataFrame([{
        "cnpj_fornecedor": "11.111.111/0001-11",
        "valor_inicial": 100_000.0,
        "qtd_aditivos": 0,
    }])
    result = transformar_fornecedores(df)
    assert result.iloc[0]["razao_social"] == "Fornecedor 11.111.111/0001-11"


def test_fornecedores_usa_possui_aditivo_sim_nao():
    df = pd.DataFrame([
        {"cnpj_fornecedor": "11.111.111/0001-11", "nome_fornecedor": "A",
         "possui_aditivo": "Sim", "valor_inicial": 100.0},
        {"cnpj_fornecedor": "11.111.111/0001-11", "nome_fornecedor": "A",
         "possui_aditivo": "Não", "valor_inicial": 100.0},
    ])
    result = transformar_fornecedores(df)
    assert result.iloc[0]["taxa_aditivo"] == 50.0


# ── obras: _obras_de_contratos ────────────────────────────────────────────────


def test_obras_contratos_filtra_objeto_sem_palavra_chave():
    df = pd.DataFrame([
        {"id_contrato": "C-1", "objeto": "Aquisição de materiais de escritório",
         "fonte": "tce_rj_contratos"},
        {"id_contrato": "C-2", "objeto": "Reforma de escola municipal",
         "fonte": "tce_rj_contratos"},
    ])
    result = _obras_de_contratos(df)
    assert len(result) == 1
    assert "Reforma" in result.iloc[0]["nome"]


def test_obras_contratos_vazio_sem_objeto():
    df = pd.DataFrame([{"id_contrato": "C-1", "valor": 100}])
    result = _obras_de_contratos(df)
    assert result.empty


def test_obras_contratos_usa_tipo_contrato():
    df = pd.DataFrame([{
        "id_contrato": "C-1",
        "objeto": "Construção de galeria",
        "fonte": "tce_rj_contratos",
        "tipo_contrato": "Empreitada Global",
    }])
    result = _obras_de_contratos(df)
    assert result.iloc[0]["tipo"] == "Empreitada Global"


def test_obras_contratos_fallback_secretaria_para_orgao():
    df = pd.DataFrame([{
        "id_contrato": "C-1",
        "objeto": "Pavimentação de rua",
        "fonte": "tce_rj_contratos",
        "secretaria": None,
        "orgao": "Secretaria de Obras",
    }])
    result = _obras_de_contratos(df)
    assert result.iloc[0]["secretaria"] == "Secretaria de Obras"


def test_obras_contratos_fallback_secretaria_para_unidade_gestora():
    df = pd.DataFrame([{
        "id_contrato": "C-1",
        "objeto": "Drenagem pluvial",
        "fonte": "tce_rj_contratos",
        "secretaria": None,
        "orgao": None,
        "unidade_gestora": "UG Obras",
    }])
    result = _obras_de_contratos(df)
    assert result.iloc[0]["secretaria"] == "UG Obras"


def test_obras_todas_fontes_vazias_retorna_vazio():
    result = transformar_obras(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
    )
    assert result.empty


# ── contratos: branches não cobertos ──────────────────────────────────────────


def test_contratos_raw_vazio_retorna_vazio():
    result = transformar_contratos(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    assert result.empty


def test_contratos_obras_sem_coluna_id_retorna_vazio():
    raw = _raw_contratos()
    obras = pd.DataFrame([{"id_origem": "C-001", "fonte_origem": "f"}])  # sem coluna 'id'
    result = transformar_contratos(raw, obras, pd.DataFrame())
    assert result.empty


def test_contratos_usa_num_licitacao_quando_disponivel():
    raw = _raw_contratos(id_contrato="C-001", fonte="portal_transparencia_macae_contratos")
    raw["num_licitacao"] = "LICIT-2024/001"
    obras = _obras_df_com_id("C-001", "portal_transparencia_macae_contratos")
    result = transformar_contratos(raw, obras, pd.DataFrame())
    assert len(result) == 1
    assert result.iloc[0]["numero"] == "LICIT-2024/001"


def test_contratos_num_licitacao_vazio_usa_id_contrato():
    raw = _raw_contratos(id_contrato="C-001", fonte="portal_transparencia_macae_contratos")
    raw["num_licitacao"] = None
    obras = _obras_df_com_id("C-001", "portal_transparencia_macae_contratos")
    result = transformar_contratos(raw, obras, pd.DataFrame())
    assert result.iloc[0]["numero"] == "C-001"


# ── aditivos: branches não cobertos ──────────────────────────────────────────


def test_aditivos_raw_vazio_retorna_vazio():
    contratos = _contratos_df("C-001", "f")
    result = transformar_aditivos(pd.DataFrame(), contratos)
    assert result.empty


def test_aditivos_contratos_vazio_retorna_vazio():
    raw = pd.DataFrame([{"id_contrato": "C-001", "fonte": "f", "valor_aditivos": 1000.0}])
    result = transformar_aditivos(raw, pd.DataFrame())
    assert result.empty


def test_aditivos_contratos_sem_coluna_id_retorna_vazio():
    raw = pd.DataFrame([{"id_contrato": "C-001", "fonte": "f", "valor_aditivos": 1000.0}])
    contratos = pd.DataFrame([{"numero": "C-001", "fonte_origem": "f"}])  # sem 'id'
    result = transformar_aditivos(raw, contratos)
    assert result.empty


def test_aditivos_sem_coluna_valor_aditivos_retorna_vazio():
    raw = pd.DataFrame([{"id_contrato": "C-001", "fonte": "f", "outro": 1}])
    contratos = _contratos_df("C-001", "f")
    result = transformar_aditivos(raw, contratos)
    assert result.empty


# ── upsert ────────────────────────────────────────────────────────────────────


def test_upsert_df_vazio_retorna_zero(mocker):
    fake_client = mocker.MagicMock()
    result = upsert(fake_client, "obras", pd.DataFrame(), ["fonte_origem", "id_origem"])
    assert result == 0
    fake_client.table.assert_not_called()


def test_upsert_sucesso_retorna_total(mocker):
    fake_client = mocker.MagicMock()
    fake_client.table.return_value.upsert.return_value.execute.return_value = None
    df = pd.DataFrame([{"cnpj": "11.111.111/0001-11", "razao_social": "Alfa"}])
    result = upsert(fake_client, "fornecedores", df, ["cnpj"])
    assert result == 1


def test_upsert_falha_loga_e_continua(mocker, caplog):
    fake_client = mocker.MagicMock()
    fake_client.table.return_value.upsert.return_value.execute.side_effect = RuntimeError("db error")
    df = pd.DataFrame([{"cnpj": "11.111.111/0001-11", "razao_social": "Alfa"}])
    with caplog.at_level("ERROR"):
        result = upsert(fake_client, "fornecedores", df, ["cnpj"])
    assert result == 0
    assert "falha" in caplog.text.lower()


def test_upsert_em_lotes(mocker):
    fake_client = mocker.MagicMock()
    fake_client.table.return_value.upsert.return_value.execute.return_value = None
    df = pd.DataFrame([{"cnpj": f"cnpj_{i}", "razao_social": f"E{i}"} for i in range(1100)])
    result = upsert(fake_client, "fornecedores", df, ["cnpj"], batch_size=500)
    assert result == 1100
    assert fake_client.table.return_value.upsert.call_count == 3


# ── run: outros caminhos de erro ──────────────────────────────────────────────


def test_run_obras_e_contratos_falham_aditivos_continua(mocker):
    mocker.patch.object(transformer, "get_client", return_value=mocker.MagicMock())
    mocker.patch.object(transformer, "ler_raw", return_value=pd.DataFrame())
    mocker.patch.object(transformer, "transformar_fornecedores", return_value=pd.DataFrame())
    mocker.patch.object(transformer, "transformar_obras", side_effect=RuntimeError("obras falhou"))
    mocker.patch.object(transformer, "transformar_contratos", side_effect=RuntimeError("contratos falhou"))
    aditivos_chamado = []
    mocker.patch.object(
        transformer, "transformar_aditivos",
        side_effect=lambda *a, **kw: aditivos_chamado.append(True) or pd.DataFrame(),
    )
    mocker.patch.object(transformer, "upsert", return_value=0)

    resultado = transformer.run()
    assert aditivos_chamado
    assert resultado["obras"] == 0
    assert resultado["contratos"] == 0
