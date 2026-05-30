-- Migração: criar tabela raw_convenios_federais
--
-- Motivação (issue #14 / investigação de fontes):
--   O Portal da Transparência expõe os convênios federais do município com a
--   DATA DE CONCLUSÃO REAL (dataConclusao), situação, valores e CNPJ do
--   proponente. Diferente do dump SICONV (fim de vigência = proxy), aqui vem a
--   conclusão de fato — usada para enriquecer obras.data_conclusao do legado.
--   Chave: nr_convenio (= dimConvenio.codigo) casa com obras.num_licitacao.
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

CREATE TABLE IF NOT EXISTS raw_convenios_federais (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nr_convenio         TEXT NOT NULL,
    numero              TEXT,
    objeto              TEXT,
    situacao            TEXT,
    data_conclusao      DATE,
    data_inicio_vigencia DATE,
    data_fim_vigencia   DATE,
    data_publicacao     DATE,
    valor               NUMERIC(15, 2),
    valor_liberado      NUMERIC(15, 2),
    valor_contrapartida NUMERIC(15, 2),
    cnpj_proponente     TEXT,
    nome_proponente     TEXT,
    orgao               TEXT,
    municipio_ibge      TEXT,
    fonte               TEXT NOT NULL DEFAULT 'portal_transparencia_convenios',
    coletado_em         TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_bruto       TEXT,

    CONSTRAINT uq_raw_convenios_federais UNIQUE (nr_convenio, fonte)
);

CREATE INDEX IF NOT EXISTS idx_raw_convenios_federais_convenio
    ON raw_convenios_federais (nr_convenio);
