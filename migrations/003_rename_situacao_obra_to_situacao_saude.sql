-- Migração: renomear coluna situacao_obra → situacao em raw_obras_saude
-- Normaliza nomenclatura para alinhar com todas as outras raw tables de obras.
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

ALTER TABLE raw_obras_saude
    RENAME COLUMN situacao_obra TO situacao;
