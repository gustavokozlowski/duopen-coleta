-- Migração: adicionar chave de junção obra↔contrato à tabela estruturada `obras`
--
-- Motivação:
--   O duopen-ml não consegue ligar obras (com histórico) aos contratos
--   (CNPJ, aditivos) porque a tabela `obras` não expõe identificadores de
--   contrato/licitação nem o CNPJ da executora. Esses campos JÁ são coletados
--   pelos scrapers (painel_atual e painel_legado) e preservados na camada raw
--   (raw_obras_atual / raw_obras_legado), mas eram descartados na transformação
--   por não existirem colunas de destino em `obras`.
--
--   Com estas colunas, o transformer passa a propagar a chave e o duopen-ml pode
--   casar obra↔contrato por num_contrato/num_licitacao (formato da fonte) ou cnpj.
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

ALTER TABLE obras
    ADD COLUMN IF NOT EXISTS cnpj_executora TEXT,
    ADD COLUMN IF NOT EXISTS num_contrato   TEXT,
    ADD COLUMN IF NOT EXISTS num_licitacao  TEXT;

-- Índices para acelerar o join obra↔contrato no duopen-ml.
CREATE INDEX IF NOT EXISTS idx_obras_num_contrato
    ON obras (num_contrato)
    WHERE num_contrato IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_obras_cnpj_executora
    ON obras (cnpj_executora)
    WHERE cnpj_executora IS NOT NULL;

COMMENT ON COLUMN obras.cnpj_executora IS
    'CNPJ da empresa executora (origem: painel_atual/painel_legado). Chave para enriquecer fornecedor e cruzar com raw_contratos.';
COMMENT ON COLUMN obras.num_contrato IS
    'Número de contrato da fonte da obra (painel_legado: codigo_transacao_obras; painel_atual: num_contrato). Pode exigir normalização para casar com raw_contratos.id_contrato.';
COMMENT ON COLUMN obras.num_licitacao IS
    'Número de licitação/convênio da fonte da obra (painel_legado: nr_convenio_obras; painel_atual: num_licitacao).';
