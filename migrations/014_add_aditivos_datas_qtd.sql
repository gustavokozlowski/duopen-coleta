-- Migração: qtd_aditivos em obras + datas de vigência em raw_aditivos_federais
--
-- Motivação (follow-up do #13, pedido do ML na issue #14):
--   1. `obras.qtd_aditivos` — propaga a contagem de aditivos federais para a camada
--      estruturada (hoje só em raw_aditivos_federais). É feature do duopen-ml.
--   2. `raw_aditivos_federais.data_fim_vigencia` / `data_fim_vigencia_original` —
--      do siconv_convenio (DIA_FIM_VIGENC_CONV / DIA_FIM_VIGENC_ORIGINAL_CONV).
--      Para convênios concluídos, o transformer usa fim-vigência como proxy de
--      data_conclusao e o fim-original como prazo → atraso DIÁRIO real no legado
--      (ex.: 775661 original 2014 → final 2019).
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

ALTER TABLE obras
    ADD COLUMN IF NOT EXISTS qtd_aditivos SMALLINT;

ALTER TABLE raw_aditivos_federais
    ADD COLUMN IF NOT EXISTS data_fim_vigencia          DATE,
    ADD COLUMN IF NOT EXISTS data_fim_vigencia_original DATE;

COMMENT ON COLUMN obras.qtd_aditivos IS
    'Número de termos aditivos do convênio federal (origem: raw_aditivos_federais via num_licitacao=nr_convenio).';
