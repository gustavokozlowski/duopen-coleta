-- Migração: adicionar campos do EGIM à tabela raw_obras_georef
--
-- Motivação:
--   data_inicio          — data de início da obra no formato "Mês/Ano" convertido
--                          para ISO 8601. Presente em 30/36 obras.
--   setor_administrativo — zona administrativa de Macaé (SETOR VERDE, AZUL, etc.).
--                          100% preenchido, único campo de zoneamento disponível.
--   objectid             — ID interno do Google My Maps (único por obra).
--                          Alternativa mais estável para chave de conflito do upsert.
--
-- Correção de bug: previsao_termino estava sempre NULL porque o candidato "fim"
-- não estava na lista de busca do scraper. Após a correção do scraper, este campo
-- passará a ser populado (36/36) com valores como "Março/2023", "360 DIAS", etc.
-- Nenhuma mudança de schema necessária para previsao_termino (já existe).
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

ALTER TABLE raw_obras_georef
    ADD COLUMN IF NOT EXISTS data_inicio          TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS setor_administrativo TEXT,
    ADD COLUMN IF NOT EXISTS objectid             TEXT;

CREATE INDEX IF NOT EXISTS idx_raw_obras_georef_setor
    ON raw_obras_georef (setor_administrativo)
    WHERE setor_administrativo IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_obras_georef_objectid
    ON raw_obras_georef (objectid)
    WHERE objectid IS NOT NULL;
