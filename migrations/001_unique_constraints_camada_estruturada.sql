-- Migração: unique constraints na camada Estruturada
-- Necessário para o etl/transformer.py usar ON CONFLICT (upsert).
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

-- fornecedores: um registro por CNPJ
ALTER TABLE fornecedores
    ADD CONSTRAINT fornecedores_cnpj_key UNIQUE (cnpj);

-- obras: uma obra por (fonte, id na fonte)
ALTER TABLE obras
    ADD CONSTRAINT obras_fonte_origem_id_origem_key UNIQUE (fonte_origem, id_origem);

-- contratos: um contrato por (numero, fonte)
ALTER TABLE contratos
    ADD CONSTRAINT contratos_numero_fonte_origem_key UNIQUE (numero, fonte_origem);

-- aditivos: um registro consolidado por contrato
ALTER TABLE aditivos
    ADD CONSTRAINT aditivos_id_contrato_key UNIQUE (id_contrato);
