"""Routing da camada Raw: mapeia datasets do cache para tabelas Supabase.

Cada entrada define para um dataset (stem do arquivo de cache):
- tabela: tabela de destino na camada Raw
- fonte: identificador da origem (gravado na coluna `fonte`)
- conflict: tupla com a chave de upsert (ON CONFLICT)

`RAW_TABLE_COLUMNS` lista as colunas válidas de cada tabela Raw — usado
pelo loader para descartar campos do cache que não existem no schema alvo.
"""

from __future__ import annotations

from typing import TypedDict


class DatasetRoute(TypedDict, total=False):
    tabela: str
    fonte: str
    conflict: tuple[str, ...]
    rename: dict[str, str]
    defaults: dict[str, object]
    required: tuple[str, ...]


_IBGE_MACAE = "3302403"


RAW_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "raw_contratos": frozenset({
        "id", "id_contrato", "fonte", "municipio_ibge", "objeto", "modalidade",
        "situacao", "tipo_contrato", "num_licitacao", "ano", "cnpj_fornecedor",
        "nome_fornecedor", "valor_inicial", "valor_global", "valor_aditivos",
        "valor_final", "data_assinatura", "data_inicio_vigencia",
        "data_fim_vigencia", "data_rescisao", "orgao", "secretaria",
        "unidade_gestora", "qtd_aditivos", "possui_aditivo", "coletado_em",
        "payload_bruto",
    }),
    "raw_licitacoes": frozenset({
        "id", "id_licitacao", "fonte", "municipio_ibge", "ano", "objeto",
        "modalidade", "situacao", "tipo", "numero", "valor_estimado",
        "valor_homologado", "data_abertura", "data_publicacao",
        "data_homologacao", "cnpj_vencedor", "nome_vencedor", "orgao",
        "secretaria", "unidade_gestora", "coletado_em", "payload_bruto",
    }),
    "raw_obras_paralisadas": frozenset({
        "id", "id_obra", "fonte", "municipio", "ano", "nome_obra", "tipo_obra",
        "situacao", "motivo_paralisacao", "orgao", "cnpj_executora",
        "nome_executora", "valor_contrato", "percentual_executado",
        "data_inicio", "data_paralisacao", "coletado_em", "payload_bruto",
    }),
    "raw_obras_saude": frozenset({
        "id", "proposta_id", "numero_proposta", "fonte", "municipio_ibge", "uf",
        "municipio", "cnpj_fundo", "entidade", "esfera_administrativa",
        "situacao_obra", "co_situacao_obra", "tipo_obra", "co_tipo_obra",
        "tipo_recurso", "programa", "rede_programa", "fase_projeto",
        "etapa_proposta", "portaria", "dt_portaria", "ano_referencia",
        "nome_estabelecimento", "nome_estabelecimento_proposta", "cnes",
        "co_unidade", "bairro", "logradouro", "numero", "complemento", "cep",
        "latitude", "longitude", "valor_proposta", "valor_total_contrato",
        "percentual_executado", "valor_1a_parcela", "valor_2a_parcela",
        "valor_3a_parcela", "valor_4a_parcela", "dt_cadastro",
        "dt_inicio_projeto", "dt_prevista_inicio", "dt_prevista_conclusao",
        "dt_conclusao_projeto", "dt_ordem_servico", "dt_inicio_obra",
        "dt_execucao", "dt_conclusao_final", "dt_prevista_conclusao_final",
        "dt_1a_parcela", "dt_2a_parcela", "dt_3a_parcela", "dt_4a_parcela",
        "dt_atualizacao", "dt_inauguracao", "dt_inicio_funcionamento",
        "possui_aditivo_contratual", "qtd_fotos", "qtd_empresas",
        "justificativa", "coletado_em", "payload_bruto",
    }),
    "raw_obras_georef": frozenset({
        "id", "nome_obra", "camada_mapa", "fonte", "map_id", "latitude",
        "longitude", "status", "secretaria", "valor", "previsao_termino",
        "percentual", "programa", "bairro", "endereco", "descricao",
        "coletado_em", "payload_bruto",
    }),
    "raw_obras_atual": frozenset({
        "id", "id_obra", "fonte", "nome_obra", "situacao", "percentual_executado",
        "secretaria", "bairro", "endereco", "num_contrato", "num_licitacao",
        "cnpj_executora", "nome_executora", "valor_contrato", "valor_aditivos",
        "data_inicio", "data_prevista_fim", "latitude", "longitude",
        "coletado_em", "payload_bruto",
    }),
    "raw_obras_legado": frozenset({
        "id", "id_obra", "fonte", "nome_obra", "situacao", "percentual_executado",
        "secretaria", "bairro", "endereco", "ano_referencia", "num_contrato",
        "num_licitacao", "cnpj_executora", "nome_executora", "valor_contrato",
        "valor_aditivos", "valor_final", "data_inicio", "data_prevista_fim",
        "data_conclusao", "dias_atraso", "percentual_aditivo", "latitude",
        "longitude", "coletado_em", "payload_bruto",
    }),
    "raw_geodados": frozenset({
        "id", "municipio_id", "municipio_nome", "uf_sigla", "uf_nome",
        "mesorregiao_nome", "microrregiao_nome", "regiao_imediata_nome",
        "regiao_intermediaria_nome", "populacao_censo_2022", "populacao_estimada",
        "area_territorial_km2", "densidade_demografica", "pib_per_capita", "idhm",
        "geojson", "fonte", "coletado_em", "payload_bruto",
    }),
}


RAW_LAYER_ROUTING: dict[str, DatasetRoute] = {
    "tce_contratos": {
        "tabela": "raw_contratos",
        "fonte": "tce_rj_contratos",
        "conflict": ("id_contrato", "fonte"),
        "rename": {
            "valor_contrato": "valor_inicial",
            "data_inicio": "data_inicio_vigencia",
            "data_fim": "data_fim_vigencia",
        },
        "defaults": {"municipio_ibge": _IBGE_MACAE},
        "required": ("id_contrato", "fonte"),
    },
    "tce_licitacoes": {
        "tabela": "raw_licitacoes",
        "fonte": "tce_rj_licitacoes",
        "conflict": ("id_licitacao", "fonte"),
        "rename": {
            "data_publicacao_edital": "data_publicacao",
            "unidade": "unidade_gestora",
        },
        "defaults": {"municipio_ibge": _IBGE_MACAE},
        "required": ("id_licitacao", "fonte"),
    },
    "tce_compras_diretas": {
        "tabela": "raw_contratos",
        "fonte": "tce_rj_compras_diretas",
        "conflict": ("id_contrato", "fonte"),
        "rename": {
            "valor_contrato": "valor_inicial",
            "data_inicio": "data_inicio_vigencia",
            "data_fim": "data_fim_vigencia",
        },
        "defaults": {"municipio_ibge": _IBGE_MACAE},
        "required": ("id_contrato", "fonte"),
    },
    "tce_obras_paralisadas": {
        "tabela": "raw_obras_paralisadas",
        "fonte": "tce_rj_obras_paralisadas",
        "conflict": ("id_obra", "fonte"),
        "required": ("id_obra", "fonte"),
    },
    "portal_macae_contratos": {
        "tabela": "raw_contratos",
        "fonte": "portal_transparencia_macae_contratos",
        "conflict": ("id_contrato", "fonte"),
        "defaults": {"municipio_ibge": _IBGE_MACAE},
        "required": ("id_contrato", "fonte"),
    },
    "portal_macae_licitacoes": {
        "tabela": "raw_licitacoes",
        "fonte": "portal_transparencia_macae_licitacoes",
        "conflict": ("id_licitacao", "fonte"),
        "defaults": {"municipio_ibge": _IBGE_MACAE},
        "required": ("id_licitacao", "fonte"),
    },
    "transparencia_contratos": {
        "tabela": "raw_contratos",
        "fonte": "portal_transparencia_federal",
        "conflict": ("id_contrato", "fonte"),
        "defaults": {"municipio_ibge": _IBGE_MACAE},
        "required": ("id_contrato", "fonte"),
    },
    "transparencia_licitacoes": {
        "tabela": "raw_licitacoes",
        "fonte": "portal_transparencia_federal",
        "conflict": ("id_licitacao", "fonte"),
        "defaults": {"municipio_ibge": _IBGE_MACAE},
        "required": ("id_licitacao", "fonte"),
    },
    "sismob": {
        "tabela": "raw_obras_saude",
        "fonte": "sismob_cidadao",
        "conflict": ("proposta_id",),
        "required": ("proposta_id", "fonte"),
    },
    "egim": {
        "tabela": "raw_obras_georef",
        "fonte": "egim_google_mymaps",
        "conflict": ("nome_obra", "latitude", "longitude"),
        "required": ("nome_obra", "fonte", "latitude", "longitude"),
    },
    "painel_atual": {
        "tabela": "raw_obras_atual",
        "fonte": "painel_obras_atual_macae",
        "conflict": ("id_obra",),
        "required": ("id_obra", "fonte"),
    },
    "painel_legado": {
        "tabela": "raw_obras_legado",
        "fonte": "painel_obras_legado_macae",
        "conflict": ("id_obra",),
        "required": ("id_obra", "fonte"),
    },
    "ibge": {
        "tabela": "raw_geodados",
        "fonte": "ibge",
        "conflict": ("municipio_id",),
        "required": ("municipio_id", "fonte"),
    },
}


def resolver_rota(dataset: str) -> DatasetRoute | None:
    return RAW_LAYER_ROUTING.get(dataset)


def colunas_alvo(tabela: str) -> frozenset[str]:
    """Retorna o conjunto de colunas válidas da tabela Raw."""
    return RAW_TABLE_COLUMNS.get(tabela, frozenset())


__all__ = [
    "DatasetRoute",
    "RAW_LAYER_ROUTING",
    "RAW_TABLE_COLUMNS",
    "colunas_alvo",
    "resolver_rota",
]
