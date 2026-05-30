# Instruções: `scrappers/federal/transferegov.py`
# DUOPEN 2026 — duopen-coleta
# Spec + protótipo · 2026-05-30

---

## 1. Contexto e objetivo

O duopen-ml está bloqueado no **modelo de estouro** e nas **features de fornecedor**
do grupo de **treino (legado)** por falta de **aditivos + CNPJ por obra**. O join com
`raw_contratos` (municipal) deu **0% de match** — porque o legado é **convênio federal
(TransfereGov)**, não contrato municipal (ver `ENTREGA_ML_resposta_ausencia_dados.md` §3).

Este scraper resolve isso buscando os aditivos e o CNPJ **na fonte federal**, chaveado
pelo **número do convênio** que já coletamos.

**Chave de junção (já disponível):**
- `raw_obras_legado.num_licitacao` = `nr_convenio_obras` → **47/47 preenchido**
  (ex.: `757206`, `3766`, `25700`).
- `payload_bruto` do legado também traz o `idProposta` do TransfereGov (no link
  `...ConsultarProposta...idProposta=NNNN`), útil como chave alternativa.

---

## 2. Fonte de dados

**Dados abertos SICONV / TransfereGov** (Sistema de Gestão de Convênios e Contratos de
Repasse). Entidades relevantes, todas chaveadas por `NR_CONVENIO`:

| Entidade | Campos-chave que queremos |
|---|---|
| `convenio` | `NR_CONVENIO`, `IDENTIF_PROPONENTE` (CNPJ), `VL_GLOBAL_CONV`, `VL_REPASSE_CONV`, `SIT_CONVENIO` |
| `termo_aditivo` | `NR_CONVENIO`, `NUMERO_TA`, `TIPO_TA`, `VL_GLOBAL_TA`/valor, `DATA_ASSINATURA_TA` |
| `proponente` | `IDENTIF_PROPONENTE` (CNPJ), `NM_PROPONENTE` |

Dois modos de acesso (o protótipo usa REST; o dump CSV é alternativa robusta):

1. **API REST (PostgREST)** — `https://api.transferegov.gestao.gov.br/` (módulo de
   convênios/discricionárias). ⚠️ **Verificar o caminho exato do recurso** — a base
   migrou SICONV → +Brasil → TransfereGov. Filtro por `nr_convenio` via querystring
   PostgREST (`?nr_convenio=eq.757206`).
2. **Dump CSV** — repositório de dados abertos (`siconv_convenio.csv`,
   `siconv_termo_aditivo.csv`, `siconv_proponentes.csv`). Mais estável, porém são
   arquivos nacionais grandes → baixar e filtrar pelos ~47 convênios do legado.

> Como em `sinapi.py`/IBGE SIDRA: o endpoint exato deve ser validado antes de produção;
> o protótipo isola isso em `TRANSFEREGOV_BASE_URL` e degrada com cache/`[]`.

---

## 3. Contrato de `run()`

`run() -> pd.DataFrame`, **uma linha por convênio**, e grava
`cache/transferegov_aditivos.json` (lista de registros — padrão do pipeline).

Schema do DataFrame (= `raw_aditivos_federais`):

```
nr_convenio        : str   — chave de junção (= obras.num_licitacao)
id_proposta        : str   — chave alternativa (TransfereGov)
cnpj_proponente    : str   — CNPJ da executora/proponente (14 díg.)
nome_proponente    : str
valor_global       : float — valor global do convênio
valor_aditivos     : float — soma dos termos aditivos
qtd_aditivos       : int   — nº de termos aditivos
situacao           : str
coletado_em        : str   — ISO 8601 UTC
```

A lista de convênios a consultar vem de `raw_obras_legado.num_licitacao` (ou do cache
`painel_legado_obras.json`), evitando varrer a base nacional inteira.

---

## 4. Integração no ETL

### 4.1 Migration `013_create_raw_aditivos_federais.sql`
Tabela `raw_aditivos_federais`, `UNIQUE (nr_convenio, fonte)`.

### 4.2 Routing (`etl/routing.py`)
```python
# RAW_TABLE_COLUMNS
"raw_aditivos_federais": frozenset({
    "id", "nr_convenio", "id_proposta", "cnpj_proponente", "nome_proponente",
    "valor_global", "valor_aditivos", "qtd_aditivos", "situacao",
    "fonte", "coletado_em", "payload_bruto",
}),
# RAW_LAYER_ROUTING
"transferegov_aditivos": {
    "tabela": "raw_aditivos_federais",
    "fonte": "transferegov",
    "conflict": ("nr_convenio", "fonte"),
    "required": ("nr_convenio", "fonte"),
},
```

### 4.3 Enriquecimento do treino (transformer) — payoff do ML
No `_obras_de_legado` (ou pós-merge em `transformar_obras`), fazer um **merge por
`num_licitacao == nr_convenio`** para preencher, no legado:
- `valor_aditivos`, `valor_final` (= valor_global + aditivos), `cnpj_executora` (quando
  o legado vier nulo, completar com `cnpj_proponente`).

Isso destrava o alvo de estouro (`percentual_aditivo > 20%`) e as features de fornecedor
no grupo de treino — o que o ML pediu.

---

## 5. Variáveis de ambiente

```env
# Verificar/definir o endpoint após confirmar o recurso do TransfereGov
TRANSFEREGOV_BASE_URL=https://api.transferegov.gestao.gov.br
```

---

## 6. Notas para o agente

1. **Seguir `sismob.py`** — `_get` com retry/backoff, `CACHE_DIR`, `run() -> DataFrame`,
   cache lista-de-registros, fallback para cache local em falha.
2. **Sem varredura nacional** — consultar só os convênios do legado (≤ ~50).
3. **CNPJ**: normalizar para 14 dígitos (só números).
4. **Validar o endpoint** antes de produção; o protótipo já funciona em estrutura e cai
   para cache/`[]` se a fonte não responder.
