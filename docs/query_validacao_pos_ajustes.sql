-- ============================================================
-- QUERY DE VALIDAÇÃO — Dados após ajustes de mapeamento
-- Rodar no SQL Editor do Supabase (UMA SEÇÃO POR VEZ):
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql
-- ============================================================


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 1 — Contagem geral de registros em todas as tabelas
-- ════════════════════════════════════════════════════════════
WITH contagens AS (
    SELECT 'raw_contratos'          AS tabela, COUNT(*)::int AS registros FROM raw_contratos
    UNION ALL SELECT 'raw_licitacoes',          COUNT(*) FROM raw_licitacoes
    UNION ALL SELECT 'raw_obras_atual',          COUNT(*) FROM raw_obras_atual
    UNION ALL SELECT 'raw_obras_legado',         COUNT(*) FROM raw_obras_legado
    UNION ALL SELECT 'raw_obras_saude',          COUNT(*) FROM raw_obras_saude
    UNION ALL SELECT 'raw_obras_georef',         COUNT(*) FROM raw_obras_georef
    UNION ALL SELECT 'raw_obras_paralisadas',    COUNT(*) FROM raw_obras_paralisadas
    UNION ALL SELECT 'obras (estruturada)',      COUNT(*) FROM obras
    UNION ALL SELECT 'contratos (estruturada)',  COUNT(*) FROM contratos
    UNION ALL SELECT 'fornecedores (estruturada)',COUNT(*) FROM fornecedores
    UNION ALL SELECT 'aditivos (estruturada)',   COUNT(*) FROM aditivos
)
SELECT * FROM contagens ORDER BY tabela;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 2 — raw_contratos: cobertura por fonte
-- ════════════════════════════════════════════════════════════
SELECT
    fonte,
    COUNT(*)                                                          AS total,
    COUNT(*) FILTER (WHERE situacao IS NULL OR situacao = '')         AS sem_situacao,
    COUNT(*) FILTER (WHERE valor_inicial IS NULL)                     AS sem_valor,
    COUNT(*) FILTER (WHERE data_assinatura IS NULL)                   AS sem_data_assinatura,
    COUNT(*) FILTER (WHERE data_fim_vigencia IS NULL)                 AS sem_data_fim,
    COUNT(*) FILTER (WHERE cnpj_fornecedor IS NULL)                   AS sem_cnpj,
    COUNT(*) FILTER (WHERE situacao = 'Vigente')                      AS vigentes,
    COUNT(*) FILTER (WHERE situacao = 'Expirado')                     AS expirados,
    COUNT(*) FILTER (WHERE situacao = 'Indefinido')                   AS indefinidos
FROM raw_contratos
GROUP BY fonte
ORDER BY total DESC;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 3 — raw_licitacoes: cobertura por fonte
-- ════════════════════════════════════════════════════════════
SELECT
    fonte,
    COUNT(*)                                                                        AS total,
    COUNT(*) FILTER (WHERE situacao IS NULL)                                        AS sem_situacao,
    COUNT(*) FILTER (WHERE valor_estimado IS NULL)                                  AS sem_valor,
    COUNT(*) FILTER (WHERE data_publicacao IS NULL AND data_abertura IS NULL)          AS sem_data,
    COUNT(*) FILTER (WHERE cnpj_vencedor IS NULL)                                   AS sem_vencedor
FROM raw_licitacoes
GROUP BY fonte
ORDER BY total DESC;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 4 — raw_obras_saude (SISMOB): cobertura
-- ════════════════════════════════════════════════════════════
SELECT
    fonte,
    COUNT(*)                                                         AS total,
    COUNT(*) FILTER (WHERE situacao IS NULL)                         AS sem_situacao,
    COUNT(*) FILTER (WHERE percentual_executado IS NULL)             AS sem_percentual,
    COUNT(*) FILTER (WHERE valor_proposta IS NULL)                   AS sem_valor_proposta,
    COUNT(*) FILTER (WHERE valor_total_contrato IS NULL)             AS sem_valor_contrato,
    COUNT(*) FILTER (WHERE cnes IS NULL)                             AS sem_cnes,
    COUNT(*) FILTER (WHERE dt_inicio_obra IS NULL)                   AS sem_dt_inicio_obra,
    COUNT(*) FILTER (WHERE latitude IS NULL)                         AS sem_lat
FROM raw_obras_saude
GROUP BY fonte;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 5 — raw_obras_georef (EGIM): cobertura
-- ════════════════════════════════════════════════════════════
SELECT
    fonte,
    COUNT(*)                                              AS total,
    COUNT(*) FILTER (WHERE situacao IS NULL)              AS sem_situacao,
    COUNT(*) FILTER (WHERE percentual IS NULL)            AS sem_percentual,
    COUNT(*) FILTER (WHERE valor IS NULL)                 AS sem_valor,
    COUNT(*) FILTER (WHERE previsao_termino IS NULL)      AS sem_previsao_termino,
    COUNT(*) FILTER (WHERE bairro IS NULL)                AS sem_bairro,
    COUNT(*) FILTER (WHERE latitude IS NULL)              AS sem_lat
FROM raw_obras_georef
GROUP BY fonte;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 6 — raw_obras_atual: cobertura
-- ════════════════════════════════════════════════════════════
SELECT
    fonte,
    COUNT(*)                                                  AS total,
    COUNT(*) FILTER (WHERE situacao IS NULL)                  AS sem_situacao,
    COUNT(*) FILTER (WHERE percentual_executado IS NULL)      AS sem_percentual,
    COUNT(*) FILTER (WHERE valor_contrato IS NULL)            AS sem_valor,
    COUNT(*) FILTER (WHERE data_inicio IS NULL)               AS sem_data_inicio,
    COUNT(*) FILTER (WHERE data_prevista_fim IS NULL)         AS sem_data_fim,
    COUNT(*) FILTER (WHERE secretaria IS NULL)                AS sem_secretaria,
    COUNT(*) FILTER (WHERE latitude IS NULL)                  AS sem_lat
FROM raw_obras_atual
GROUP BY fonte;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 7 — raw_obras_legado: cobertura
-- ════════════════════════════════════════════════════════════
SELECT
    fonte,
    COUNT(*)                                                  AS total,
    COUNT(*) FILTER (WHERE situacao IS NULL)                  AS sem_situacao,
    COUNT(*) FILTER (WHERE percentual_executado IS NULL)      AS sem_percentual,
    COUNT(*) FILTER (WHERE valor_contrato IS NULL)            AS sem_valor,
    COUNT(*) FILTER (WHERE data_inicio IS NULL)               AS sem_data_inicio,
    COUNT(*) FILTER (WHERE secretaria IS NULL)                AS sem_secretaria,
    COUNT(*) FILTER (WHERE latitude IS NULL)                  AS sem_lat
FROM raw_obras_legado
GROUP BY fonte;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 8 — raw_obras_paralisadas: cobertura
-- ════════════════════════════════════════════════════════════
SELECT
    fonte,
    COUNT(*)                                                  AS total,
    COUNT(*) FILTER (WHERE situacao IS NULL)                  AS sem_situacao,
    COUNT(*) FILTER (WHERE valor_contrato IS NULL)            AS sem_valor,
    COUNT(*) FILTER (WHERE motivo_paralisacao IS NULL)        AS sem_motivo,
    COUNT(*) FILTER (WHERE data_inicio IS NULL)               AS sem_data_inicio
FROM raw_obras_paralisadas
GROUP BY fonte;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 9 — obras (estruturada): cobertura e médias
-- ════════════════════════════════════════════════════════════
SELECT
    fonte_origem,
    COUNT(*)                                                      AS total,
    COUNT(*) FILTER (WHERE situacao IS NULL)                      AS sem_situacao,
    COUNT(*) FILTER (WHERE percentual_executado IS NULL)          AS sem_percentual,
    COUNT(*) FILTER (WHERE valor_contrato IS NULL)                AS sem_valor,
    COUNT(*) FILTER (WHERE latitude IS NULL)                      AS sem_lat,
    COUNT(*) FILTER (WHERE data_inicio IS NULL)                   AS sem_data_inicio,
    COUNT(*) FILTER (WHERE data_prevista_fim IS NULL)             AS sem_data_fim,
    ROUND(AVG(percentual_executado), 1)                           AS media_percentual
FROM obras
GROUP BY fonte_origem
ORDER BY total DESC;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 10 — obras: distribuição de situação
-- ════════════════════════════════════════════════════════════
SELECT
    situacao,
    COUNT(*)                                                  AS qtd,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)       AS pct,
    ROUND(AVG(percentual_executado), 1)                       AS media_percentual
FROM obras
GROUP BY situacao
ORDER BY qtd DESC;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 11 — contratos (estruturada): situação por fonte
-- ════════════════════════════════════════════════════════════
SELECT
    fonte_origem,
    situacao,
    COUNT(*) AS qtd
FROM contratos
GROUP BY fonte_origem, situacao
ORDER BY fonte_origem, qtd DESC;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 12 — obras SEM percentual (deve retornar vazio)
-- ════════════════════════════════════════════════════════════
SELECT
    fonte_origem,
    id_origem,
    nome,
    situacao,
    percentual_executado
FROM obras
WHERE percentual_executado IS NULL
ORDER BY fonte_origem, nome;


-- ════════════════════════════════════════════════════════════
-- SEÇÃO 13 — contratos SEM situação (deve retornar vazio)
-- ════════════════════════════════════════════════════════════
SELECT
    fonte_origem,
    numero,
    LEFT(objeto, 60) AS objeto,
    situacao,
    data_inicio,
    data_fim
FROM contratos
WHERE situacao IS NULL OR situacao = ''
ORDER BY fonte_origem;
