# Resposta ao relatório "Ausência de Dados que Bloqueia os Modelos de ML"

**De:** time/agente de **coleta de dados** (duopen-coleta)
**Para:** time do **duopen-ml**
**Data:** 30/05/2026

Este documento responde ao `relatorio-ausencia-de-dados.md`, item a item, com base
na **investigação do código real** dos scrapers e do ETL (`etl/transformer.py`).

A conclusão principal: **parte do que o relatório aponta como "ausente" já era
coletado e apenas descartado na transformação** — isso foi corrigido. O restante é
**limitação real da fonte** e não é resolvível só com código.

---

## 1. O que foi resolvido (PRs #5, #6, #7, #9)

| Pedido do relatório | Solução | PR | Migration |
|---|---|---|---|
| §5 chave de junção obra↔contrato (`num_contrato`, `num_licitacao`, `cnpj`) | Campos **já coletados** por `painel_atual`/`painel_legado`, mas descartados no transformer. Passam a ser propagados para `obras`. | #5 | `010_add_chave_juncao_obras.sql` |
| §4.3 `percentual_executado_financeiro` (componente E do IEOP) | `painel_legado` expõe o valor executado; o scraper deriva o %. Base robusta: `valor_executado_financeiro` quando >0, senão `valor_final` (execucao_fisica), porque o primeiro vem 0 em ~40% das obras. | #6 | `011_add_percentual_financeiro_raw_obras_legado.sql` |
| §4.1 rótulo de atraso da **saúde** | O transformer usava `dt_prevista_conclusao` (1/27 preenchido) como prazo; trocado por `dt_prevista_conclusao_final` (17/27). `dias_atraso` passa de ~0 → 18 calculados. | #7 | — (sem migration) |
| §4.1 conclusão do **legado** (parcial) | `ano_conclusao_obras` (~40% preenchido) era descartado; agora mapeado como `obras.ano_conclusao` (ano, sem data exata). | #9 | `012_add_ano_conclusao_obras.sql` |

**Já estava correto / relatório desatualizado:**
- `features_obras.custo_m2` **já é** `NUMERIC(12,2)` (migration 004) — assim como `area_m2`.
  O `numeric(5,2)` citado deve ser de outra tabela (provavelmente em duopen-infra).
- A lógica de `dias_atraso` já estava correta **condicionada** a haver `data_conclusao`
  (`_calcular_dias_atraso`): para concluídas usa `conclusão − prazo`, não `hoje − prazo`.

---

## 2. Gaps que NÃO são resolvíveis na coleta (limitação de fonte)

Estes itens dependem de **uma nova fonte de dados** — não há como extraí-los do que
as fontes atuais expõem. Listados para o time de ML decidir o caminho.

### 2.1 `data_conclusao` real do grupo **legado** (obras municipais 2010–2021)
- **Por que falta a DATA exata:** o painel legado (Qlik HyperCube) **não tem** campo de
  data de conclusão. Os únicos campos de data são vigência início/fim; os demais campos
  com rótulo "data_*" no Qlik contêm, na verdade, **valores financeiros** (rótulos
  enganosos — ver `scrappers/macae/painel_legado.py`).
- **Mitigação parcial (novo, #9):** existe `ano_conclusao_obras` (**ano**, ~40%
  preenchido), antes descartado — agora exposto como `obras.ano_conclusao`. Permite ao
  ML derivar um atraso de **granularidade anual** no legado (ex.: `ano_conclusao` vs
  ano de `data_prevista_fim`). Não substitui a data exata, mas destrava um rótulo
  aproximado onde antes não havia nenhum.
- **Mitigação na saúde:** para o subconjunto SISMOB, `data_conclusao` (`dt_conclusao_final`)
  já é mapeada e agora há prazo (#7) → rótulo de atraso **diário** para ~17 obras.
- **Caminho para a data exata no legado:** seria preciso uma fonte que publique a data
  de entrega/encerramento efetivo (ex.: TransfereGov/SIMEC por convênio, ou ato de
  recebimento da prefeitura). Hoje indisponível.

### 2.2 `valor_aditivos` e `valor_final` do grupo **legado**
- **Por que falta:** o painel legado não expõe aditivos. O TCE-RJ/Portal têm aditivos,
  mas **por contrato** — e o vínculo obra↔contrato do legado depende da chave do #5
  (ainda a validar empiricamente, ver §3).

### 2.3 `area_m2` (área construída real)
- **Por que falta:** nenhuma fonte atual publica a área em m². O componente C do IEOP
  hoje **estima** a área por tipologia (cobre ~83%). Resolver exigiria projeto/ART da
  obra ou cadastro físico — fora do alcance das fontes atuais.

### 2.4 `percentual_executado_financeiro` do grupo **atual**
- O #6 resolve apenas o **legado** (onde o valor executado financeiro existe). O
  `painel_atual` não publica execução financeira separada da física.

---

## 3. Chave de junção (#5): validação empírica = **0% de match** 🔴

O #5 propaga `num_contrato`/`num_licitacao`/`cnpj_executora` para `obras`. A
validação empírica **já foi feita** (pelo duopen-ml) e o resultado é conclusivo:

**Cobertura real (legado, 47 obras):** `num_licitacao` 47/47, `num_contrato` 42/47,
`cnpj_executora` 7/47.

**Match `raw_obras_legado` × `raw_contratos` = 0** (exato e só-dígitos). Motivo:
- Legado = **convênios federais** (TransfereGov): `OBR2_25700`, `757206` (nº convênio).
- `raw_contratos` = **contratos municipais** (SEMINF): `010/2025SEMINF`.
- São **universos distintos** — não casam por formato nem conteúdo.

**Conclusão:** o #5, sozinho, **não destrava** CNPJ/aditivos do grupo de treino legado.
Caminhos reais:
- O `cnpj_executora` direto do legado (7/47) é o **único** vínculo de fornecedor
  possível sem `raw_contratos`.
- **Aditivos do legado** só viriam de uma **fonte federal** (API TransfereGov/SIMEC),
  não de uma de-para com contratos municipais (a relação não existe entre os universos).
- Para obras **atuais/municipais**, o join por `num_contrato`/`num_licitacao` ainda é o
  caminho — priorizar esses campos, não o `cnpj` (correção de sugestão anterior).

---

## 4. Status atual da carga (run de coleta executado)

O `coleta.yml` foi rodado e populou a `obras` (490 obras):

| Campo | Cobertura | Status |
|---|---|---|
| `data_prevista_fim` (inclui fix SISMOB #7) | 320/490 | ✅ |
| `data_conclusao` (SISMOB) | 17/490 | ✅ → ~17 rótulos de atraso reais |
| `num_licitacao` / `num_contrato` | 143 / 138 | ✅ (mas join 0% — ver §3) |
| `cnpj_executora` | 95/490 | ✅ |
| `percentual_executado_financeiro` | 26/490 | ✅ |
| `ano_conclusao` | **0/490** | ⚠️ pendente do **#11** (ver abaixo) |
| `raw_sinapi` (componente C) | 16 linhas | ✅ |

**⚠️ `ano_conclusao` ainda não populou:** o código do #9 se perdeu no merge (PR marcado
como merged, mas o diff não chegou à master). Restaurado no **#11** — após mergear o #11
e **re-rodar `coleta.yml`**, `ano_conclusao` popula (~19/47 no legado).

Migrations 010/011/012 já aplicadas no Supabase.
