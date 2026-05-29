-- Migração: adicionar num_processo e prazo_dias em raw_contratos,
--           e num_processo em raw_licitacoes
--
-- Motivação:
--   num_processo  — número do processo administrativo de origem (ex: 76403/2023,
--                   SEI-6021/2026). Chave de rastreabilidade que conecta contratos
--                   às suas licitações e aos documentos no SEI da prefeitura.
--   prazo_dias    — prazo do contrato convertido para inteiro (dias). Permite
--                   calcular data_vigencia_fim estimada para os ~80% de contratos
--                   sem data de fim, e detectar prazos anômalos no score de risco.
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

ALTER TABLE raw_contratos
    ADD COLUMN IF NOT EXISTS num_processo TEXT,
    ADD COLUMN IF NOT EXISTS prazo_dias   INTEGER;

ALTER TABLE raw_licitacoes
    ADD COLUMN IF NOT EXISTS num_processo TEXT;

-- Índice em num_processo para facilitar cruzamentos entre contratos e licitacoes
CREATE INDEX IF NOT EXISTS idx_raw_contratos_num_processo
    ON raw_contratos (num_processo)
    WHERE num_processo IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_licitacoes_num_processo
    ON raw_licitacoes (num_processo)
    WHERE num_processo IS NOT NULL;
