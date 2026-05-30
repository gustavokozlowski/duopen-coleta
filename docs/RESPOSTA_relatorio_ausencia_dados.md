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

## 1. O que foi resolvido (3 PRs)

| Pedido do relatório | Solução | PR | Migration |
|---|---|---|---|
| §5 chave de junção obra↔contrato (`num_contrato`, `num_licitacao`, `cnpj`) | Campos **já coletados** por `painel_atual`/`painel_legado`, mas descartados no transformer. Passam a ser propagados para `obras`. | #5 | `010_add_chave_juncao_obras.sql` |
| §4.3 `percentual_executado_financeiro` (componente E do IEOP) | `painel_legado` já expõe o valor executado financeiro; o scraper passa a derivar o %. | #6 | `011_add_percentual_financeiro_raw_obras_legado.sql` |
| §4.1 rótulo de atraso da **saúde** | O transformer usava `dt_prevista_conclusao` (1/27 preenchido) como prazo; trocado por `dt_prevista_conclusao_final` (17/27). `dias_atraso` passa de ~0 → 18 calculados. | #7 | — (sem migration) |

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
- **Por que falta:** o painel legado (Qlik HyperCube) **não tem** campo de data de
  conclusão. Os únicos campos de data são vigência início/fim; os demais campos com
  rótulo "data_*" no Qlik contêm, na verdade, **valores financeiros** (rótulos
  enganosos — ver `scrappers/macae/painel_legado.py`).
- **Mitigação já feita:** para o subconjunto de **saúde** (SISMOB), `data_conclusao`
  (`dt_conclusao_final`) já é mapeada e agora há prazo (#7) → rótulo de atraso real
  para ~17 obras de saúde.
- **Caminho para destravar o legado:** seria preciso uma fonte que publique a data de
  entrega/encerramento efetivo das obras municipais antigas (ex.: TransfereGov/SIMEC
  por convênio, ou ato de recebimento da prefeitura). Hoje indisponível.

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

## 3. Ponto de atenção sobre a chave de junção (#5)

O #5 propaga `num_contrato`/`num_licitacao`/`cnpj_executora` para `obras`, mas **a
compatibilidade de formato ainda precisa ser validada empiricamente** depois que os
dados forem carregados:

- Legado: `num_contrato` vem de `codigo_transacao_obras` (sistema federal) e
  `num_licitacao` de `nr_convenio_obras` — podem **não casar diretamente** com
  `raw_contratos.id_contrato` (formato `010/2025SEMINF`).
- Sugestão para o ML: tentar o join primeiro por `cnpj_executora` (mais robusto) e,
  em paralelo, por `num_contrato` normalizado. Se a taxa de match for baixa, retornamos
  ao tema com uma rotina de-para na coleta.

---

## 4. Ações de operação pendentes

1. Aplicar no Supabase as migrations **010** (#5) e **011** (#6) antes do próximo
   `pipeline.py` (o upsert do transformer não filtra colunas — coluna inexistente
   quebra com `PGRST204`).
2. Mergear os PRs empilhados na ordem **#5 → #6 → #7**.
