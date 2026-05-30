# Enriquecimento do legado via fontes federais — entrega + roadmap

**Origem:** duopen-coleta · **Público-alvo:** duopen-ml · **Data:** 30/05/2026
**Relacionado:** issue #14, PRs #13, #16, #17, #18, #19

Fechamento da investigação de fontes federais para preencher os dados que faltavam
no grupo de **treino (legado)**: `data_conclusao`, `valor_aditivos` e `cnpj` por obra.

---

## 1. O que foi entregue

| Entrega | Fonte | Campo no `obras` | PR |
|---|---|---|---|
| Aditivos por convênio | TransfereGov/SICONV (dump CSV) | `qtd_aditivos`, `valor_aditivos` | #13, #16 |
| Atraso diário (proxy) | SICONV (fim de vigência) | `data_conclusao` / `data_prevista_fim` | #16, #17 |
| **Data de conclusão REAL** | **Portal da Transparência** (`/convenios`) | `data_conclusao` (precedência) | #18 |
| Chave de junção (já anterior) | painel atual/legado | `num_contrato`, `num_licitacao`, `cnpj_executora` | #5 |
| Ano de conclusão | painel legado | `ano_conclusao` | #9, #11 |
| % financeiro | painel legado | `percentual_executado_financeiro` | #6 |
| Prazo/conclusão saúde | SISMOB | `data_conclusao` (diário) | #7 |

Tabelas raw novas: `raw_aditivos_federais` (SICONV), `raw_convenios_federais` (Portal).

---

## 2. Cobertura resultante (legado)

- **Rótulo de atraso DIÁRIO:** 5 convênios SICONV concluídos (data real do Portal) + SISMOB (~17).
- **Rótulo de atraso ANUAL:** 19 obras do legado (`ano_conclusao`).
- **Aditivos:** 5 convênios (todos de vigência → `valor_aditivos = 0` = negativo confiável).

---

## 3. Fontes investigadas — veredito (qual serve para quê)

| Fonte | Acessível | data_conclusao | CNPJ executor | Veredito |
|---|---|---|---|---|
| **Portal da Transparência** `/convenios` | ✅ API (chave) | ✅ **real** | ❌ (só proponente/Município) | **Adotado (#18)** |
| **TransfereGov/SICONV** dump CSV | ✅ | 🟡 proxy (fim-vigência) | ❌ | **Adotado (#13)** |
| **Obrasgov.br** API | ✅ | — | ✅ executor | ❌ **não cobre Macaé** (10 obras RJ no país, 0 Macaé) |
| **SIMEC/FNDE** painel | 🟡 sessão (driblável) | ❌ (só vigência/%/vistorias) | ❌ | ❌ ROI baixo — não publica conclusão limpa |

---

## 4. Limitações reais de fonte (sem caminho de bom retorno hoje)

1. **`data_conclusao` do grosso do legado (16 obras SIMEC/PAR):** o SIMEC não publica
   data de conclusão efetiva (só vigência, % e vistorias). Exigiria proxy via última
   vistoria (especulativo) + scraper de sessão frágil.
2. **CNPJ da EXECUTORA (construtora):** nenhuma fonte de convênio entrega — o CNPJ
   disponível é o do proponente (Município). A executora está na licitação/contrato
   do convênio (camada mais profunda). Bloqueia features de fornecedor no treino.
3. **`area_m2` real:** nenhuma fonte pública publica; segue estimada por tipologia.
4. **Ingerir convênios do Portal como novas obras:** avaliado — dos 118 convênios de
   Macaé, só ~7-8 são obras de construção e a maioria já está no legado. Net de obras
   novas com conclusão real ≈ 3-5 (sem geolocalização). **ROI baixo — não implementado.**

---

## 5. Roadmap (se/quando priorizado)

| Item | Esforço | Desbloqueia |
|---|---|---|
| Scraper de sessão SIMEC + proxy de conclusão por última vistoria | Alto | data_conclusao de ~16 obras (aproximada) |
| CNPJ da executora via licitações/contratos do convênio (Portal `/licitacoes`) | Alto | features de fornecedor no treino |
| Ingestão de área (`area_m2`) — fonte a definir | Alto | componente C do IEOP (hoje estimado) |

---

## 6. Conclusão

Foi extraído tudo que as fontes federais atuais permitem com bom retorno. O rótulo de
atraso do treino subiu de "proxy de atraso corrente" para **desfecho real** onde a fonte
existe (SICONV/Portal + SISMOB + ano do legado). Os gaps restantes são **limitação de
fonte**, não de pipeline — listados no §4/§5 para decisão conjunta com o ML.
