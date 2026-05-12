"""Testes do mapeamento dataset → tabela Raw em etl/routing.py."""

import pytest

from etl.routing import (
	RAW_LAYER_ROUTING,
	RAW_TABLE_COLUMNS,
	colunas_alvo,
	resolver_rota,
)

pytestmark = pytest.mark.unit


# ── resolver_rota ───────────────────────────────────────────────────────────


def test_resolver_rota_retorna_rota_para_dataset_conhecido() -> None:
	rota = resolver_rota("tce_rj_contratos")

	assert rota is not None
	assert rota["tabela"] == "raw_contratos"
	assert rota["fonte"] == "tce_rj_contratos"
	assert rota["conflict"] == ("id_contrato", "fonte")


def test_resolver_rota_retorna_none_para_dataset_desconhecido() -> None:
	assert resolver_rota("dataset_inexistente") is None
	assert resolver_rota("") is None


# ── colunas_alvo ────────────────────────────────────────────────────────────


def test_colunas_alvo_retorna_frozenset_para_tabela_conhecida() -> None:
	cols = colunas_alvo("raw_contratos")

	assert isinstance(cols, frozenset)
	assert "id_contrato" in cols
	assert "fonte" in cols
	assert "payload_bruto" in cols


def test_colunas_alvo_retorna_frozenset_vazio_para_tabela_desconhecida() -> None:
	assert colunas_alvo("tabela_inexistente") == frozenset()


# ── Integridade da configuração ─────────────────────────────────────────────


def test_toda_rota_referencia_tabela_com_schema_conhecido() -> None:
	for dataset, rota in RAW_LAYER_ROUTING.items():
		assert rota["tabela"] in RAW_TABLE_COLUMNS, (
			f"Dataset '{dataset}' aponta para tabela '{rota['tabela']}' "
			f"sem schema em RAW_TABLE_COLUMNS"
		)


def test_chave_de_conflito_existe_no_schema_da_tabela() -> None:
	for dataset, rota in RAW_LAYER_ROUTING.items():
		cols = RAW_TABLE_COLUMNS[rota["tabela"]]
		for c in rota["conflict"]:
			assert c in cols, (
				f"Dataset '{dataset}': coluna de conflito '{c}' não existe "
				f"em {rota['tabela']}"
			)


def test_rename_aponta_para_colunas_validas_da_tabela() -> None:
	for dataset, rota in RAW_LAYER_ROUTING.items():
		rename = rota.get("rename") or {}
		cols = RAW_TABLE_COLUMNS[rota["tabela"]]
		for origem, destino in rename.items():
			assert destino in cols, (
				f"Dataset '{dataset}': rename {origem}→{destino} aponta "
				f"para coluna inexistente em {rota['tabela']}"
			)


def test_defaults_apontam_para_colunas_validas_da_tabela() -> None:
	for dataset, rota in RAW_LAYER_ROUTING.items():
		defaults = rota.get("defaults") or {}
		cols = RAW_TABLE_COLUMNS[rota["tabela"]]
		for coluna in defaults:
			assert coluna in cols, (
				f"Dataset '{dataset}': default '{coluna}' não existe "
				f"em {rota['tabela']}"
			)


def test_required_inclui_chave_de_conflito() -> None:
	# Sanidade: se um campo é chave de upsert, tem que estar no schema mínimo.
	for dataset, rota in RAW_LAYER_ROUTING.items():
		required = rota.get("required")
		if required is None:
			continue
		for c in rota["conflict"]:
			assert c in required, (
				f"Dataset '{dataset}': chave de conflito '{c}' não está "
				f"em required={required}"
			)


def test_fontes_sao_unicas_dentro_da_mesma_tabela_e_chave_simples() -> None:
	# Para tabelas onde o conflict é só (chave, fonte), fontes precisam ser
	# distintas entre rotas que apontam para a mesma tabela — caso contrário,
	# dois scrapers gravariam na mesma linha lógica.
	por_tabela: dict[str, list[str]] = {}
	for rota in RAW_LAYER_ROUTING.values():
		if "fonte" not in rota["conflict"]:
			continue
		por_tabela.setdefault(rota["tabela"], []).append(rota["fonte"])

	for tabela, fontes in por_tabela.items():
		assert len(fontes) == len(set(fontes)), (
			f"Tabela '{tabela}' tem fontes duplicadas no routing: {fontes}"
		)


def test_routing_cobre_todos_os_scrapers_documentados() -> None:
	# As fontes documentadas em raw_layer_docs.pdf devem todas existir no routing.
	fontes_esperadas = {
		"tce_rj_contratos",
		"tce_rj_compras_diretas",
		"tce_rj_licitacoes",
		"tce_rj_obras_paralisadas",
		"portal_transparencia_macae_contratos",
		"portal_transparencia_macae_licitacoes",
		"sismob_cidadao",
		"egim_google_mymaps",
		"painel_obras_atual_macae",
		"painel_obras_legado_macae",
		"ibge",
	}
	fontes_no_routing = {r["fonte"] for r in RAW_LAYER_ROUTING.values()}
	faltantes = fontes_esperadas - fontes_no_routing
	assert not faltantes, f"Fontes documentadas sem rota: {faltantes}"
