# Mudanças de schema da camada `obras` — guia de adaptação

**Origem:** duopen-coleta · **Público-alvo:** **backend (API)** e **duopen-ml**
**Data:** 30/05/2026 · **PRs:** #5, #6, #7, #9

Este documento lista as **novas colunas** produzidas pelo ETL de coleta e como
**backend** e **ML** devem se adaptar. Nada foi removido nem renomeado — só
**adições** (retrocompatível).

> ⚠️ As colunas só passam a ser preenchidas **depois** de (1) aplicar as migrations
> no Supabase e (2) rodar o `pipeline.py`/`transformer`. Até lá, elas existem mas
> vêm `NULL`. Trate todas como **nullable**.

---

## 1. Novas colunas na tabela estruturada `obras`

| Coluna | Tipo | PR | Migration | Origem / semântica | Cobertura |
|---|---|---|---|---|---|
| `cnpj_executora` | TEXT | #5 | 010 | CNPJ da executora (painel atual/legado) | legado ~7/47; atual variável |
| `num_contrato` | TEXT | #5 | 010 | nº de contrato da fonte da obra | legado 42/47 |
| `num_licitacao` | TEXT | #5 | 010 | nº de licitação/convênio da fonte | legado 47/47 |
| `percentual_executado_financeiro` | NUMERIC | #6 | 011 | % executado financeiro (componente E do IEOP) | legado (já existia a coluna; agora populada) |
| `ano_conclusao` | SMALLINT | #9 | 012 | **ano** de conclusão (sem data exata) | legado ~19/47 |

> `percentual_executado_financeiro` **já existia** no schema de `obras`; o que mudou é
> que agora ela é **preenchida** para o grupo legado (antes ficava sempre nula).

---

## 2. Novas colunas nas tabelas Raw (consumidas direto pelo ML, se aplicável)

| Tabela | Coluna | PR | Migration |
|---|---|---|---|
| `raw_obras_legado` | `percentual_executado_financeiro` (NUMERIC) | #6 | 011 |
| `raw_obras_legado` | `ano_conclusao` (SMALLINT) | #9 | 012 |

(A tabela de referência `raw_sinapi`, criada anteriormente, segue documentada em
`docs/INSTRUCOES_sinapi.md`.)

---

## 3. Como o **backend (API)** deve se adaptar

1. **Expor os novos campos** de `obras` na serialização (DTO/response) — todos
   **nullable**: `cnpj_executora`, `num_contrato`, `num_licitacao`, `ano_conclusao`,
   e garantir que `percentual_executado_financeiro` seja exposto (pode já estar).
2. **Não assumir preenchimento**: a maioria só existe no grupo legado. Em telas/filtros,
   tratar `NULL` como "não informado".
3. **Tipos**: `ano_conclusao` é inteiro (ano, ex.: `2014`), não data. `cnpj_executora`,
   `num_contrato`, `num_licitacao` são texto livre (sem máscara garantida).
4. Nenhuma migração de leitura quebra — são adições retrocompatíveis.

---

## 4. Como o **duopen-ml** deve se adaptar

### 4.1 Join obra↔contrato (enriquecimento de CNPJ/aditivos)
- Priorizar **`num_contrato`/`num_licitacao`** (cobertura 42/47 e 47/47). **Não** usar
  `cnpj_executora` como chave primária no legado (só 7/47).
- Ressalva: `num_contrato` (de `codigo_transacao_obras`) e `num_licitacao`
  (`nr_convenio_obras`) podem precisar de **normalização** para casar com
  `raw_contratos.id_contrato` (formato `010/2025SEMINF`). Validar a taxa de match.

### 4.2 Rótulo de atraso
- **Saúde (SISMOB):** `data_conclusao` + `data_prevista_fim` agora preenchidos (#7) →
  `dias_atraso` **diário** já calculado pelo transformer.
- **Legado:** não há data exata, mas há `ano_conclusao` (#9). Derivar atraso de
  **granularidade anual** (ex.: `ano_conclusao − ano(data_prevista_fim)`), tratando-o
  como sinal coarse, separado do diário da saúde.

### 4.3 Componente E do IEOP — `percentual_executado_financeiro`
- Agora populado no legado. **Atenção:** é uma aproximação — a fonte Qlik mistura
  execução física/financeira com rótulos enganosos. A coleta usa
  `valor_executado_financeiro` quando > 0, senão `valor_final` (execucao_fisica).
  Se o sinal parecer ruidoso, sinalizar para revisitarmos na coleta.

---

## 5. Ordem de aplicação (operação)

1. Aplicar no Supabase as migrations **010** (#5), **011** (#6) e **012** (#9) **antes**
   do próximo `pipeline.py` — o upsert do transformer não filtra colunas, então coluna
   inexistente quebra a carga com `PGRST204`.
2. Rodar o `transformer`/`pipeline.py` para popular os campos.
3. Backend e ML podem então ler os novos campos (até lá vêm `NULL`).
