-- Migração: criar tabela features_obras (camada analítica)
--
-- Responsabilidade: armazenar métricas e indicadores derivados por obra,
-- calculados a partir das tabelas estruturadas (obras, contratos, fornecedores)
-- e de dados contextuais (IBGE). Cada coluna tem nullabilidade explicitamente
-- justificada — zero nunca substitui ausência de dado.
--
-- Rodar no SQL Editor do Supabase:
--   https://supabase.com/dashboard/project/sxckuxuwfwrhjmwprucr/sql

CREATE TABLE IF NOT EXISTS features_obras (

    -- ── Identificação ────────────────────────────────────────────────────────
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_obra     UUID NOT NULL REFERENCES obras(id) ON DELETE CASCADE,
    calculado_em TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- ── Atributos herdados da obra (desnormalizados para performance) ────────
    -- NOT NULL: sempre preenchidos pelo transformer antes de chegar aqui
    situacao    TEXT    NOT NULL,
    tipo        TEXT    NOT NULL,
    municipio   TEXT    NOT NULL,
    uf          CHAR(2) NOT NULL,

    -- NULL: percentual não é informado por todas as fontes (contratos, georef)
    percentual_executado    NUMERIC(5, 2)   NULL,

    -- ── Métricas financeiras ─────────────────────────────────────────────────
    -- NULL: obras extraídas de contratos sem valor explícito (ex: tce_rj_contratos)
    valor_contrato          NUMERIC(15, 2)  NULL,
    valor_aditivos          NUMERIC(15, 2)  NULL,
    valor_total             NUMERIC(15, 2)  NULL, -- valor_contrato + valor_aditivos

    -- NULL: requer valor_contrato != 0 para o denominador
    percentual_aditivo      NUMERIC(8, 4)   NULL,

    -- NULL: requer área física da obra — dado não disponível nas fontes atuais;
    -- coluna reservada para quando fonte de área for integrada
    area_m2                 NUMERIC(12, 2)  NULL,
    custo_m2                NUMERIC(12, 2)  NULL,

    -- NULL: requer populacao_estimada do IBGE (raw_geodados)
    valor_por_habitante     NUMERIC(14, 4)  NULL,

    -- ── Métricas de prazo ────────────────────────────────────────────────────
    -- NULL: requer data_inicio E data_prevista_fim — fontes de contrato não informam
    duracao_prevista_dias   INTEGER         NULL,

    -- NULL: requer data_inicio E data_conclusao — só obras concluídas têm ambas
    duracao_real_dias       INTEGER         NULL,

    -- NULL: requer data_prevista_fim E data_conclusao (ou dias_atraso da fonte)
    atraso_dias             INTEGER         NULL,

    -- NULL: requer atraso_dias calculável
    flag_atrasada           BOOLEAN         NULL,

    -- ── Métricas de contratação ──────────────────────────────────────────────
    -- NULL: obras sem contratos linkados (georef, sismob sem contrato)
    n_contratos             INTEGER         NULL,
    n_aditivos              INTEGER         NULL,
    n_fornecedores          INTEGER         NULL,

    -- NULL: requer n_contratos >= 1 com fornecedor identificado
    flag_fornecedor_unico   BOOLEAN         NULL,

    -- ── Score de risco composto ──────────────────────────────────────────────
    -- NULL: calculado apenas quando há dados suficientes (mínimo: valor + prazo)
    -- Escala 0–100: quanto maior, maior o risco de irregularidade
    score_risco             NUMERIC(5, 2)   NULL,

    -- NULL: requer custo_m2 calculado — aguarda integração de área física
    flag_custo_elevado      BOOLEAN         NULL,

    -- ── Controle ─────────────────────────────────────────────────────────────
    CONSTRAINT features_obras_id_obra_key UNIQUE (id_obra)
);

-- Índices para queries frequentes no front-end/BI
CREATE INDEX IF NOT EXISTS idx_features_obras_situacao
    ON features_obras (situacao);

CREATE INDEX IF NOT EXISTS idx_features_obras_score_risco
    ON features_obras (score_risco)
    WHERE score_risco IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_features_obras_flag_atrasada
    ON features_obras (flag_atrasada)
    WHERE flag_atrasada IS NOT NULL;

COMMENT ON TABLE features_obras IS
    'Camada analítica: métricas e indicadores derivados por obra. '
    'Colunas NULL indicam ausência natural de dado, nunca zero artificial.';

COMMENT ON COLUMN features_obras.custo_m2 IS
    'NULL até integração de fonte com área física da obra (m²)';
COMMENT ON COLUMN features_obras.valor_por_habitante IS
    'NULL quando raw_geodados (IBGE) não estiver disponível para o município';
COMMENT ON COLUMN features_obras.score_risco IS
    'NULL quando dados insuficientes para cálculo (mínimo: valor_contrato + alguma data)';
COMMENT ON COLUMN features_obras.percentual_aditivo IS
    'NULL quando valor_contrato = NULL ou zero — evita divisão por zero artificial';
