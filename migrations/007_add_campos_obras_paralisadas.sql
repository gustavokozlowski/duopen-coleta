-- Migração: adicionar campos críticos de obras paralisadas
--
-- Motivação:
--   valor_pago_obra — valor efetivamente pago antes da paralisação.
--                     Permite calcular percentual_executado financeiro
--                     (valor_pago / valor_contrato × 100) quando a fonte
--                     não publica esse campo diretamente.
--   funcao_governo  — área de governo da obra (SAÚDE, EDUCAÇÃO, etc.).
--                     Único campo de categorização temática disponível
--                     nessa fonte; viabiliza filtros por setor no front-end.
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

ALTER TABLE raw_obras_paralisadas
    ADD COLUMN IF NOT EXISTS valor_pago_obra NUMERIC(15, 2),
    ADD COLUMN IF NOT EXISTS funcao_governo  TEXT;

CREATE INDEX IF NOT EXISTS idx_raw_obras_paralisadas_funcao
    ON raw_obras_paralisadas (funcao_governo)
    WHERE funcao_governo IS NOT NULL;
