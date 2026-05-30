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

### 2.1 Tabela de referência `raw_sinapi` (consumida pelo **duopen-ml**)

`raw_sinapi` (custo de referência R$/m² por `tipo_obra`, criada anteriormente —
ver `docs/INSTRUCOES_sinapi.md`) é **produzida pela coleta** (`sinapi.py` →
`pipeline.py`) e **consumida exclusivamente pelo duopen-ml** — o ETL da coleta
(`etl/transformer.py`) **não** a lê.

- **Uso (ML):** insumo do **componente C (Custo) do IEOP** —
  `razao = custo_real_m2 / sinapi_referencia_m2`. O ML mapeia o tipo da obra para a
  categoria SINAPI (helpers `mapear_tipo_sinapi`/`custo_referencia` em
  `scrappers/federal/sinapi.py`) e grava de volta em `obras.tipo_sinapi`,
  `obras.ieop_custo` e `features_obras.custo_m2` — colunas **calculadas pelo ML**,
  não pela coleta.
- **Backend:** não consome `raw_sinapi` diretamente; vê o resultado apenas via os
  campos `ieop_*`/`tipo_sinapi` que o ML grava em `obras`.

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

### 4.1 Join obra↔contrato (enriquecimento de CNPJ/aditivos) — match 0% no legado 🔴
- **Validação empírica já feita:** `raw_obras_legado` × `raw_contratos` = **0 match**.
  Legado são **convênios federais** (TransfereGov); `raw_contratos` são **contratos
  municipais** (SEMINF) — universos distintos. O #5 **não** destrava aditivos/CNPJ do
  legado via `raw_contratos`.
- **Legado:** usar o `cnpj_executora` direto (7/47, único vínculo); aditivos só de uma
  fonte **federal** (TransfereGov/SIMEC), não de de-para municipal.
- **Atual/municipal:** o join por `num_contrato`/`num_licitacao` continua válido —
  priorizar esses campos (não o `cnpj`); normalizar formato vs `raw_contratos.id_contrato`.

### 4.2 Rótulo de atraso
- **Saúde (SISMOB):** `data_conclusao` + `data_prevista_fim` preenchidos (#7) →
  `dias_atraso` **diário** já calculado pelo transformer (~17 obras). ✅ ativo.
- **Legado:** não há data exata, só `ano_conclusao` (#9). ⚠️ **Ainda 0/490** — o #9 se
  perdeu no merge, restaurado no **#11**; popula (~19/47) após mergear #11 + re-rodar
  `coleta.yml`. Aí: derivar atraso de **granularidade anual**
  (`ano_conclusao − ano(data_prevista_fim)`), como sinal coarse.

### 4.3 Componente E do IEOP — `percentual_executado_financeiro`
- Agora populado no legado. **Atenção:** é uma aproximação — a fonte Qlik mistura
  execução física/financeira com rótulos enganosos. A coleta usa
  `valor_executado_financeiro` quando > 0, senão `valor_final` (execucao_fisica).
  Se o sinal parecer ruidoso, sinalizar para revisitarmos na coleta.

---

## 5. Status de operação

- Migrations **010/011/012** já aplicadas no Supabase.
- `coleta.yml` já rodado → `obras` populada (ver cobertura na §1 e em
  `ENTREGA_ML_resposta_ausencia_dados.md` §4). Campos com baixa cobertura são limitação
  de fonte, **não** falta de carga.
- **Exceção:** `ano_conclusao` = 0/490 até mergear o **#11** e re-rodar `coleta.yml`.
- O upsert do transformer não filtra colunas: aplicar migrations **antes** do
  `pipeline.py` (coluna inexistente quebra com `PGRST204`).
