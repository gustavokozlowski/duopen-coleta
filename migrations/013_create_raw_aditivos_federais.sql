-- Migração: criar tabela raw_aditivos_federais
--
-- Motivação:
--   Aditivos + CNPJ por convênio federal (TransfereGov/SICONV), chaveados por
--   nr_convenio (= raw_obras_legado.num_licitacao). Destrava o modelo de estouro
--   e as features de fornecedor no grupo de TREINO (legado), onde o join com
--   raw_contratos (municipal) deu 0% — universos distintos.
--   Ver docs/INSTRUCOES_transferegov.md.
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

CREATE TABLE IF NOT EXISTS raw_aditivos_federais (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nr_convenio     TEXT NOT NULL,
    id_proposta     TEXT,
    cnpj_proponente TEXT,
    nome_proponente TEXT,
    valor_global    NUMERIC(15, 2),
    valor_aditivos  NUMERIC(15, 2),
    qtd_aditivos    INTEGER,
    situacao        TEXT,
    fonte           TEXT NOT NULL DEFAULT 'transferegov',
    coletado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_bruto   TEXT,

    CONSTRAINT uq_raw_aditivos_federais UNIQUE (nr_convenio, fonte)
);

CREATE INDEX IF NOT EXISTS idx_raw_aditivos_federais_convenio
    ON raw_aditivos_federais (nr_convenio);
