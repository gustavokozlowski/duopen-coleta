-- Migração: adicionar percentual_executado_financeiro à raw_obras_legado
--
-- Motivação:
--   Melhora o componente E do IEOP (execução), hoje em fallback "= físico".
--   O painel legado já expõe o valor executado financeiro (data_criacao_obras,
--   rótulo enganoso do Qlik) e o valor do contrato; o scraper agora deriva
--   percentual_executado_financeiro = valor_executado / valor_contrato * 100.
--   A coluna de destino na tabela estruturada `obras` já existe — falta apenas
--   carregar o dado pela camada raw.
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

ALTER TABLE raw_obras_legado
    ADD COLUMN IF NOT EXISTS percentual_executado_financeiro NUMERIC(5, 2);

COMMENT ON COLUMN raw_obras_legado.percentual_executado_financeiro IS
    'Percentual executado financeiro derivado (valor_executado_financeiro / valor_contrato * 100). Origem: painel_obras_legado_macae.';
