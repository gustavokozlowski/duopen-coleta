-- Migração: criar tabela raw_convenios
--
-- Motivação:
--   O dataset tce_rj_aditivos.json contém CONVÊNIOS (tipo_registro='Convenio')
--   com seus aditivos consolidados — não são aditivos de contratos de obras.
--   A tabela estruturada `aditivos` exige FK para contratos(id), que esses
--   convênios não possuem (têm numero_convenio, não número de contrato).
--   Por isso ganham tabela raw própria, preservando objeto, valores, datas e
--   quantidade de aditivos para análise futura sem violar o modelo de aditivos.
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

CREATE TABLE IF NOT EXISTS raw_convenios (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_aditivo          TEXT NOT NULL,
    fonte               TEXT NOT NULL,
    municipio           TEXT,
    unidade_gestora     TEXT,
    numero_convenio     TEXT,
    ano_convenio        TEXT,
    mes_convenio        TEXT,
    tipo_registro       TEXT,
    objeto              TEXT,
    quantidade_aditivos NUMERIC,
    valor_aditivos      NUMERIC(15, 2),
    valor_convenio      NUMERIC(15, 2),
    ultima_data_aditivo TIMESTAMPTZ,
    data_assinatura     TIMESTAMPTZ,
    data_publicacao     TIMESTAMPTZ,
    coletado_em         TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_bruto       TEXT,

    CONSTRAINT raw_convenios_id_aditivo_fonte_key UNIQUE (id_aditivo, fonte)
);

CREATE INDEX IF NOT EXISTS idx_raw_convenios_numero
    ON raw_convenios (numero_convenio)
    WHERE numero_convenio IS NOT NULL;
