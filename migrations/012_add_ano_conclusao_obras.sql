-- Migração: adicionar ano_conclusao (granularidade anual) à camada de obras
--
-- Motivação:
--   O painel legado não publica data de conclusão exata, mas expõe o ANO de
--   conclusão (`ano_conclusao_obras`, ~40% preenchido). É a única pista de
--   desfecho temporal das obras municipais antigas (2010–2021). Exposto cru
--   (sem fabricar data) para o duopen-ml derivar um atraso de granularidade
--   anual no grupo de treino, hoje sem rótulo de conclusão.
--
-- Idempotente (IF NOT EXISTS). Já aplicada no Supabase; este arquivo restaura
-- o registro da migration no repositório (perdido no merge do #9).
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

ALTER TABLE raw_obras_legado
    ADD COLUMN IF NOT EXISTS ano_conclusao SMALLINT;

ALTER TABLE obras
    ADD COLUMN IF NOT EXISTS ano_conclusao SMALLINT;

COMMENT ON COLUMN obras.ano_conclusao IS
    'Ano de conclusão da obra (granularidade anual; origem: painel_obras_legado_macae.ano_conclusao_obras). Sem data exata — usar para atraso aproximado no legado.';
