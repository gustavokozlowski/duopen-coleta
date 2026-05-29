-- Migração: criar tabela raw_sinapi
--
-- Motivação:
--   O dataset sinapi.json fornece os custos de referência por m² (SINAPI/CUB)
--   para cada tipo de obra no RJ — insumo do componente C do IEOP
--   (razao = custo_real_m2 / sinapi_referencia_m2). Uma linha por tipo_obra,
--   chaveada por (uf, competencia, tipo_obra) para permitir histórico mensal.
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

CREATE TABLE IF NOT EXISTS raw_sinapi (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    uf            TEXT NOT NULL DEFAULT 'RJ',
    competencia   TEXT NOT NULL,            -- formato YYYY-MM
    tipo_obra     TEXT NOT NULL,
    custo_m2      NUMERIC(10, 2) NOT NULL,
    fonte         TEXT NOT NULL DEFAULT 'sinapi_embutida',
    coletado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_raw_sinapi UNIQUE (uf, competencia, tipo_obra)
);
