-- ============================================================
-- QUERY DE VALIDAÇÃO — Dados após ajustes de mapeamento
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql
-- ============================================================


-- ── 1. VISÃO GERAL POR FONTE ─────────────────────────────────────────────────
-- Conta registros e campos cruciais nulos por fonte no raw layer

SELECT
    fonte,
    COUNT(*)                                                    AS total,
    COUNT(*) FILTER (WHERE situacao IS NULL OR situacao = '')   AS sem_situacao,
    COUNT(*) FILTER (WHERE percentual_executado IS NULL)        AS sem_percentual,
    COUNT(*) FILTER (WHERE valor_inicial IS NULL
                        AND valor_global IS NULL)               AS sem_valor,
    COUNT(*) FILTER (WHERE data_assinatura IS NULL)             AS sem_data_assinatura,
    ROUND(
        COUNT(*) FILTER (WHERE situacao IS NOT NULL)::numeric
        / COUNT(*) * 100, 1
    )                                                           AS pct_situacao_ok
FROM raw_contratos
GROUP BY fonte
ORDER BY total DESC;


-- ── 2. COBERTURA raw_obras_georef (EGIM) ────────────────────────────────────

SELECT
    fonte,
    COUNT(*)                                                     AS total,
    COUNT(*) FILTER (WHERE situacao    IS NULL)                  AS sem_situacao,
    COUNT(*) FILTER (WHERE percentual  IS NULL)                  AS sem_percentual,
    COUNT(*) FILTER (WHERE valor       IS NULL)                  AS sem_valor,
    COUNT(*) FILTER (WHERE bairro      IS NULL)                  AS sem_bairro,
    COUNT(*) FILTER (WHERE data_inicio IS NULL)                  AS sem_data_inicio,
    COUNT(*) FILTER (WHERE setor_administrativo IS NULL)         AS sem_setor,
    COUNT(*) FILTER (WHERE latitude    IS NULL)                  AS sem_lat
FROM raw_obras_georef
GROUP BY fonte;


-- ── 3. COBERTURA raw_obras_saude (SISMOB) ───────────────────────────────────

SELECT
    fonte,
    COUNT(*)                                                          AS total,
    COUNT(*) FILTER (WHERE situacao            IS NULL)               AS sem_situacao,
    COUNT(*) FILTER (WHERE percentual_executado IS NULL)              AS sem_percentual,
    COUNT(*) FILTER (WHERE valor_proposta      IS NULL)               AS sem_valor_proposta,
    COUNT(*) FILTER (WHERE valor_total_contrato IS NULL)              AS sem_valor_contrato,
    COUNT(*) FILTER (WHERE cnes                IS NULL)               AS sem_cnes,
    COUNT(*) FILTER (WHERE tipo_recurso_filtro IS NULL)               AS sem_tipo_recurso,
    COUNT(*) FILTER (WHERE porte_programa      IS NULL)               AS sem_porte,
    COUNT(*) FILTER (WHERE dt_inicio_obra      IS NULL)               AS sem_dt_inicio_obra,
    COUNT(*) FILTER (WHERE latitude            IS NULL)               AS sem_lat
FROM raw_obras_saude
GROUP BY fonte;


-- ── 4. COBERTURA raw_contratos por fonte ────────────────────────────────────

SELECT
    fonte,
    COUNT(*)                                                            AS total,
    COUNT(*) FILTER (WHERE situacao       IS NULL OR situacao = '')     AS sem_situacao,
    COUNT(*) FILTER (WHERE num_processo   IS NULL)                      AS sem_num_processo,
    COUNT(*) FILTER (WHERE prazo_dias     IS NULL)                      AS sem_prazo,
    COUNT(*) FILTER (WHERE valor_inicial  IS NULL)                      AS sem_valor,
    COUNT(*) FILTER (WHERE data_assinatura IS NULL)                     AS sem_data_assin,
    COUNT(*) FILTER (WHERE data_fim_vigencia IS NULL)                   AS sem_data_fim
FROM raw_contratos
GROUP BY fonte
ORDER BY total DESC;


-- ── 5. COBERTURA raw_licitacoes ──────────────────────────────────────────────

SELECT
    fonte,
    COUNT(*)                                                       AS total,
    COUNT(*) FILTER (WHERE situacao         IS NULL)               AS sem_situacao,
    COUNT(*) FILTER (WHERE num_processo     IS NULL)               AS sem_num_processo,
    COUNT(*) FILTER (WHERE valor_estimado   IS NULL)               AS sem_valor,
    COUNT(*) FILTER (WHERE data_abertura    IS NULL)               AS sem_data_abertura,
    COUNT(*) FILTER (WHERE cnpj_vencedor    IS NULL)               AS sem_vencedor
FROM raw_licitacoes
GROUP BY fonte
ORDER BY total DESC;


-- ── 6. COBERTURA camada ESTRUTURADA — obras ──────────────────────────────────

SELECT
    fonte_origem,
    COUNT(*)                                                           AS total,
    COUNT(*) FILTER (WHERE situacao            IS NULL)                AS sem_situacao,
    COUNT(*) FILTER (WHERE percentual_executado IS NULL)               AS sem_percentual,
    COUNT(*) FILTER (WHERE valor_contrato       IS NULL)               AS sem_valor,
    COUNT(*) FILTER (WHERE latitude             IS NULL)               AS sem_lat,
    COUNT(*) FILTER (WHERE data_inicio          IS NULL)               AS sem_data_inicio,
    COUNT(*) FILTER (WHERE data_prevista_fim    IS NULL)               AS sem_data_fim,
    ROUND(AVG(percentual_executado), 1)                                AS media_percentual
FROM obras
GROUP BY fonte_origem
ORDER BY total DESC;


-- ── 7. DISTRIBUIÇÃO DE SITUAÇÃO nas obras (camada estruturada) ───────────────

SELECT
    situacao,
    COUNT(*)                          AS qtd,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM obras
GROUP BY situacao
ORDER BY qtd DESC;


-- ── 8. DISTRIBUIÇÃO DE SITUAÇÃO nos contratos (camada estruturada) ───────────

SELECT
    situacao,
    fonte_origem,
    COUNT(*) AS qtd
FROM contratos
GROUP BY situacao, fonte_origem
ORDER BY qtd DESC;


-- ── 9. OBRAS SEM PERCENTUAL após os fallbacks ────────────────────────────────
-- Após as correções, idealmente esse resultado deve ser vazio

SELECT
    fonte_origem,
    id_origem,
    nome,
    situacao,
    percentual_executado
FROM obras
WHERE percentual_executado IS NULL
ORDER BY fonte_origem, nome;


-- ── 10. CONTRATOS SEM SITUAÇÃO após os fallbacks ─────────────────────────────

SELECT
    fonte_origem,
    numero,
    objeto,
    situacao,
    data_inicio,
    data_fim
FROM contratos
WHERE situacao IS NULL OR situacao = ''
ORDER BY fonte_origem;


-- ── 11. RESUMO GERAL DE COBERTURA — todas as tabelas raw ─────────────────────

SELECT 'raw_contratos'         AS tabela, COUNT(*) AS registros FROM raw_contratos
UNION ALL
SELECT 'raw_licitacoes',                  COUNT(*) FROM raw_licitacoes
UNION ALL
SELECT 'raw_obras_atual',                 COUNT(*) FROM raw_obras_atual
UNION ALL
SELECT 'raw_obras_legado',                COUNT(*) FROM raw_obras_legado
UNION ALL
SELECT 'raw_obras_saude',                 COUNT(*) FROM raw_obras_saude
UNION ALL
SELECT 'raw_obras_georef',                COUNT(*) FROM raw_obras_georef
UNION ALL
SELECT 'raw_obras_paralisadas',           COUNT(*) FROM raw_obras_paralisadas
UNION ALL
SELECT 'obras (estruturada)',             COUNT(*) FROM obras
UNION ALL
SELECT 'contratos (estruturada)',         COUNT(*) FROM contratos
UNION ALL
SELECT 'fornecedores (estruturada)',      COUNT(*) FROM fornecedores
ORDER BY tabela;
