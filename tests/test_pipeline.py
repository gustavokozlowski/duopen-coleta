"""Testes do orquestrador pipeline.py — leitura de cache e aplicação de rota."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

import pipeline

pytestmark = pytest.mark.unit


# ── _ler_json ───────────────────────────────────────────────────────────────


def test_ler_json_aceita_lista_direta(tmp_path: Path) -> None:
	arquivo = tmp_path / "tce.json"
	arquivo.write_text(json.dumps([{"id": 1}, {"id": 2}]), encoding="utf-8")

	df = pipeline._ler_json(arquivo)

	assert df is not None
	assert len(df) == 2
	assert list(df["id"]) == [1, 2]


def test_ler_json_aceita_metadata_dados(tmp_path: Path) -> None:
	arquivo = tmp_path / "scraper.json"
	arquivo.write_text(
		json.dumps({"metadata": {"v": 1}, "dados": [{"id": 1}, {"id": 2}, {"id": 3}]}),
		encoding="utf-8",
	)

	df = pipeline._ler_json(arquivo)

	assert df is not None
	assert len(df) == 3


def test_ler_json_aceita_dict_plano_como_registro_unico(tmp_path: Path) -> None:
	# Caso ibge_metadados.json — o cache é um dict só, não uma lista.
	arquivo = tmp_path / "ibge.json"
	arquivo.write_text(
		json.dumps({"municipio_id": "3302403", "municipio_nome": "Macaé"}),
		encoding="utf-8",
	)

	df = pipeline._ler_json(arquivo)

	assert df is not None
	assert len(df) == 1
	assert df.loc[0, "municipio_id"] == "3302403"


def test_ler_json_retorna_none_para_lista_vazia(tmp_path: Path) -> None:
	arquivo = tmp_path / "vazio.json"
	arquivo.write_text("[]", encoding="utf-8")

	assert pipeline._ler_json(arquivo) is None


def test_ler_json_retorna_none_para_arquivo_invalido(tmp_path: Path) -> None:
	arquivo = tmp_path / "broken.json"
	arquivo.write_text("not json at all", encoding="utf-8")

	assert pipeline._ler_json(arquivo) is None


def test_ler_json_retorna_none_para_dados_nao_lista(tmp_path: Path) -> None:
	arquivo = tmp_path / "ruim.json"
	arquivo.write_text(json.dumps({"dados": "string ao inves de lista"}), encoding="utf-8")

	# {"dados": "string"} cai no else: dados nao e lista → mas dict plano com chave
	# "dados" deve ser interpretado como single-record dict. Resultado: 1 registro.
	df = pipeline._ler_json(arquivo)
	assert df is not None
	assert len(df) == 1


# ── _aplicar_rota ───────────────────────────────────────────────────────────


def _rota_base() -> dict:
	return {
		"tabela": "raw_contratos",
		"fonte": "fonte_test",
		"conflict": ("id_contrato", "fonte"),
	}


def test_aplicar_rota_sobrescreve_fonte_sempre() -> None:
	df = pd.DataFrame([{"id_contrato": "A", "fonte": "valor_antigo"}])

	out = pipeline._aplicar_rota(df, fonte="fonte_test", rota=_rota_base())

	assert out["fonte"].tolist() == ["fonte_test"]


def test_aplicar_rota_renomeia_coluna_quando_origem_existe_e_destino_livre() -> None:
	rota = _rota_base()
	rota["rename"] = {"valor_contrato": "valor_inicial"}
	df = pd.DataFrame([{"id_contrato": "A", "valor_contrato": 100.0}])

	out = pipeline._aplicar_rota(df, fonte="fonte_test", rota=rota)

	assert "valor_contrato" not in out.columns
	assert out.loc[0, "valor_inicial"] == 100.0


def test_aplicar_rota_pula_rename_se_origem_nao_existe() -> None:
	rota = _rota_base()
	rota["rename"] = {"campo_inexistente": "valor_inicial"}
	df = pd.DataFrame([{"id_contrato": "A", "valor_inicial": 50.0}])

	out = pipeline._aplicar_rota(df, fonte="fonte_test", rota=rota)

	assert out.loc[0, "valor_inicial"] == 50.0


def test_aplicar_rota_pula_rename_se_destino_ja_existe() -> None:
	# Proteção contra sobrescrever dado já presente.
	rota = _rota_base()
	rota["rename"] = {"valor_contrato": "valor_inicial"}
	df = pd.DataFrame(
		[{"id_contrato": "A", "valor_contrato": 100.0, "valor_inicial": 999.0}]
	)

	out = pipeline._aplicar_rota(df, fonte="fonte_test", rota=rota)

	# valor_inicial preservado; valor_contrato continua porque rename foi pulado
	assert out.loc[0, "valor_inicial"] == 999.0
	assert "valor_contrato" in out.columns


def test_aplicar_rota_cria_coluna_default_se_ausente() -> None:
	rota = _rota_base()
	rota["defaults"] = {"municipio_ibge": "3302403"}
	df = pd.DataFrame([{"id_contrato": "A"}])

	out = pipeline._aplicar_rota(df, fonte="fonte_test", rota=rota)

	assert out.loc[0, "municipio_ibge"] == "3302403"


def test_aplicar_rota_preenche_default_apenas_em_celulas_vazias() -> None:
	rota = _rota_base()
	rota["defaults"] = {"municipio_ibge": "3302403"}
	df = pd.DataFrame(
		[
			{"id_contrato": "A", "municipio_ibge": "9999"},
			{"id_contrato": "B", "municipio_ibge": None},
			{"id_contrato": "C", "municipio_ibge": ""},
		]
	)

	out = pipeline._aplicar_rota(df, fonte="fonte_test", rota=rota)

	assert out.loc[0, "municipio_ibge"] == "9999"      # preservado
	assert out.loc[1, "municipio_ibge"] == "3302403"   # None preenchido
	assert out.loc[2, "municipio_ibge"] == "3302403"   # "" preenchido


def test_aplicar_rota_nao_muta_dataframe_original() -> None:
	rota = _rota_base()
	rota["rename"] = {"valor_contrato": "valor_inicial"}
	rota["defaults"] = {"municipio_ibge": "3302403"}
	df = pd.DataFrame([{"id_contrato": "A", "valor_contrato": 100.0, "fonte": "x"}])
	df_original = df.copy()

	pipeline._aplicar_rota(df, fonte="nova_fonte", rota=rota)

	pd.testing.assert_frame_equal(df, df_original)


def test_aplicar_rota_aceita_rota_sem_rename_nem_defaults() -> None:
	df = pd.DataFrame([{"id_contrato": "A", "fonte": "x"}])

	out = pipeline._aplicar_rota(df, fonte="fonte_test", rota=_rota_base())

	assert out.loc[0, "id_contrato"] == "A"
	assert out.loc[0, "fonte"] == "fonte_test"
