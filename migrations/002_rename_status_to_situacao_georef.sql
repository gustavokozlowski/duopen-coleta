-- Migração: renomear coluna status → situacao em raw_obras_georef
-- Normaliza nomenclatura para alinhar com todas as outras raw tables de obras.
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

ALTER TABLE raw_obras_georef
    RENAME COLUMN status TO situacao;
