import pandas as pd
import pytest

from etl.cleaner import (
	clean,
	fill_defaults,
	normalize_cnpj,
	normalize_dates,
	normalize_monetary,
	remove_duplicates,
	validate_schema,
)

pytestmark = pytest.mark.unit


def test_normalize_dates_converte_formatos_para_utc() -> None:
	df = pd.DataFrame(
		{
			"data_assinatura": [
				"15/03/2026",
				"2026-03-15",
				"15-03-2026 10:30:00",
				"01/13/2026",  # formato MM/DD invalido no padrao esperado
			]
		}
	)

	out = normalize_dates(df)

	assert str(out["data_assinatura"].dt.tz) == "UTC"
	assert out.loc[0, "data_assinatura"] == pd.Timestamp("2026-03-15T00:00:00Z")
	assert out.loc[1, "data_assinatura"] == pd.Timestamp("2026-03-15T00:00:00Z")
	assert out.loc[2, "data_assinatura"] == pd.Timestamp("2026-03-15T10:30:00Z")
	assert pd.isna(out.loc[3, "data_assinatura"])


def test_normalize_dates_multiplas_colunas_e_coluna_comum_intacta() -> None:
	df = pd.DataFrame(
		{
			"data_inicio": ["2026-01-01", "02/01/2026"],
			"data_fim": ["2026-12-31 23:59:59", "31-12-2026 10:30:00"],
			"descricao": ["obra 1", "obra 2"],
		}
	)

	out = normalize_dates(df)

	assert str(out["data_inicio"].dt.tz) == "UTC"
	assert str(out["data_fim"].dt.tz) == "UTC"
	assert out["descricao"].tolist() == ["obra 1", "obra 2"]


def test_normalize_cnpj_remove_pontuacao_e_invalida() -> None:
	df = pd.DataFrame(
		{
			"cnpj_fornecedor": [
				"04.252.011/0001-10",
				"11.111.111/1111-11",
				"123",
				None,
			]
		}
	)

	out = normalize_cnpj(df)

	assert out.loc[0, "cnpj_fornecedor"] == "04252011000110"
	assert out.loc[1, "cnpj_fornecedor"] is None
	assert out.loc[2, "cnpj_fornecedor"] is None
	assert out.loc[3, "cnpj_fornecedor"] is None


def test_normalize_cnpj_em_multiplas_colunas() -> None:
	df = pd.DataFrame(
		{
			"cnpj_fornecedor": ["45.723.174/0001-10"],
			"cnpj_executora": ["45723174000110"],
			"nome": ["Empresa X"],
		}
	)

	out = normalize_cnpj(df)

	assert out.loc[0, "cnpj_fornecedor"] == "45723174000110"
	assert out.loc[0, "cnpj_executora"] == "45723174000110"
	assert out.loc[0, "nome"] == "Empresa X"


def test_remove_duplicates_por_id_contrato() -> None:
	df = pd.DataFrame(
		[
			{"id_contrato": "C-1", "fonte": "src_a"},
			{"id_contrato": "C-1", "fonte": "src_b"},
			{"id_contrato": "C-2", "fonte": "src_a"},
			{"id_contrato": "C-2", "fonte": "src_a"},
		]
	)

	out = remove_duplicates(df)

	assert len(out) == 2
	assert out["id_contrato"].tolist() == ["C-1", "C-2"]
	assert out["fonte"].tolist() == ["src_a", "src_a"]


def test_normalize_monetary_suporta_formatos_variados() -> None:
	df = pd.DataFrame(
		{
			"valor_contrato": [
				"R$ 1.234.567,89",
				"1234.56",
				" 2 500,10 ",
				"1,234.50",
				"texto",
			]
		}
	)

	out = normalize_monetary(df)

	assert out.loc[0, "valor_contrato"] == 1234567.89
	assert out.loc[1, "valor_contrato"] == 1234.56
	assert out.loc[2, "valor_contrato"] == 2500.10
	assert out.loc[3, "valor_contrato"] == 1234.50
	assert pd.isna(out.loc[4, "valor_contrato"])


def test_fill_defaults_preenche_campos_obrigatorios() -> None:
	df = pd.DataFrame(
		{
			"id_contrato": ["C-1", "C-2"],
			"municipio": [None, "Rio"],
			"fonte": ["", None],
			"coletado_em": [None, "2026-03-15"],
		}
	)

	out = fill_defaults(df)

	assert out.loc[0, "municipio"] == "Macae"
	assert out.loc[1, "municipio"] == "Rio"
	assert out.loc[0, "fonte"] == "desconhecida"
	assert out.loc[1, "fonte"] == "desconhecida"
	assert str(out["coletado_em"].dt.tz) == "UTC"
	assert out.loc[1, "coletado_em"] == pd.Timestamp("2026-03-15T00:00:00Z")


def test_fill_defaults_cria_colunas_obrigatorias_ausentes() -> None:
	df = pd.DataFrame({"id_contrato": ["C-1"]})

	out = fill_defaults(df)

	assert out.loc[0, "municipio"] == "Macae"
	assert out.loc[0, "fonte"] == "desconhecida"
	assert str(out["coletado_em"].dt.tz) == "UTC"


def test_validate_schema_loga_warning_sem_excecao(caplog: pytest.LogCaptureFixture) -> None:
	df = pd.DataFrame(
		{
			"municipio": ["Macae"],
			"fonte": ["x"],
			"coletado_em": [pd.Timestamp("2026-01-01T00:00:00Z")],
		}
	)

	with caplog.at_level("WARNING"):
		out = validate_schema(df, required_columns=("id_contrato", "municipio"))

	assert out is df
	assert "id_contrato" in caplog.text


def test_clean_pipeline_completo() -> None:
	raw = pd.DataFrame(
		[
			{
				"id_contrato": "A1",
				"data_assinatura": "15/03/2026",
				"cnpj_fornecedor": "04.252.011/0001-10",
				"valor_contrato": "R$ 10.500,00",
				"municipio": None,
				"fonte": None,
				"coletado_em": None,
			},
			{
				"id_contrato": "A1",
				"data_assinatura": "2026-03-15",
				"cnpj_fornecedor": "11.111.111/1111-11",
				"valor_contrato": "10500.00",
				"municipio": "Macae",
				"fonte": "origem_x",
				"coletado_em": "2026-03-16",
			},
		]
	)

	out = clean(raw)

	assert len(out) == 1
	assert str(out["data_assinatura"].dt.tz) == "UTC"
	assert out.loc[0, "cnpj_fornecedor"] == "04252011000110"
	assert out.loc[0, "valor_contrato"] == 10500.0
	assert out.loc[0, "municipio"] == "Macae"
	assert out.loc[0, "fonte"] == "desconhecida"
	assert str(out["coletado_em"].dt.tz) == "UTC"
