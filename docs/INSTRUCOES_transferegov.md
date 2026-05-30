# `scrappers/federal/transferegov.py` — aditivos por convênio federal
# DUOPEN 2026 — duopen-coleta · implementado e validado · 2026-05-30

---

## 1. Objetivo

Trazer, da **fonte federal**, os **termos aditivos por convênio** das obras do grupo de
treino (legado) — o que o duopen-ml pediu para destravar o modelo de estouro / features
de fornecedor. O join municipal (`raw_contratos`) deu **0% de match** porque o legado é
**convênio federal**, não contrato municipal.

**Chave de junção (já coletada):** `raw_obras_legado.num_licitacao` = `nr_convenio_obras`.

---

## 2. Fonte (verificada e funcionando)

**Dump CSV de dados abertos do SICONV** (`;`-separado, **UTF-8 com BOM** → ler com
`utf-8-sig`):

| Arquivo | Tamanho | Uso |
|---|---|---|
| `siconv_convenio.csv.zip` | ~16 MB | `NR_CONVENIO`, `ID_PROPOSTA`, `VL_GLOBAL_CONV`, `SIT_CONVENIO`, `QTD_TA` |
| `siconv_termo_aditivo.csv.zip` | ~57 MB | soma de `VL_GLOBAL_TA` por `NR_CONVENIO` |

Base: `https://repositorio.dados.gov.br/seges/detru/` (env `TRANSFEREGOV_REPO_URL`).
São arquivos nacionais → baixados e **filtrados pelos ~35 convênios do legado**.

> **CNPJ do proponente** não está nesses 2 arquivos — exigiria `siconv_proposta.csv`
> (~199 MB) para ligar `ID_PROPOSTA → ID_PROPONENTE → CNPJ`. Como o legado já traz
> `cnpj_executora` direto, `cnpj_proponente` fica **nulo** aqui (coluna preservada para
> evolução futura). Não vale o download de 199 MB por enquanto.

---

## 3. Resultado real (validado contra a fonte)

Dos **35 convênios** do legado, **5 são SICONV** (os demais são PAC/SIMEC/Avançar, fora
deste dump). Exemplo real coletado:

| nr_convenio | qtd_aditivos | valor_aditivos | situacao |
|---|---|---|---|
| 775661 | 4 | 0,00 | Prestação de Contas Concluída |
| 757206 | 2 | 0,00 | Prestação de Contas Aprovada c/ Ressalvas |
| 767980 | 1 | 0,00 | Prestação de Contas Aprovada c/ Ressalvas |
| 913439 | 0 | (nulo) | Convênio Anulado |

⚠️ **Achado importante:** nesses convênios os aditivos são todos **"Alteração de
Vigência"** → `valor_aditivos = R$ 0`. Isso é um **verdadeiro-negativo de estouro**
(não houve aditivo de valor), não dado faltante. A `qtd_aditivos` é um sinal real e útil.

---

## 4. Contrato e integração (implementado)

- `run() -> pd.DataFrame`, uma linha por convênio; grava `cache/transferegov_aditivos.json`.
- Schema = tabela `raw_aditivos_federais`: `nr_convenio`, `id_proposta`, `cnpj_proponente`
  (nulo), `nome_proponente` (nulo), `valor_global`, `valor_aditivos`, `qtd_aditivos`,
  `situacao`, `coletado_em`.
- **Migration** `013_create_raw_aditivos_federais.sql` (`UNIQUE (nr_convenio, fonte)`).
- **Routing** `etl/routing.py`: rota `transferegov_aditivos` + `RAW_TABLE_COLUMNS`.
- **Enriquecimento (feito):** `transformar_obras` faz merge `num_licitacao == nr_convenio`
  e preenche `obras.valor_aditivos` do legado (`_enriquecer_aditivos_federais`). Valor 0
  (vigência) é preenchido como 0 informativo; sem convênio federal, permanece nulo.

---

## 5. Variáveis de ambiente

```env
TRANSFEREGOV_REPO_URL=https://repositorio.dados.gov.br/seges/detru
```

---

## 6. Próximos passos (opcionais)

1. **CNPJ federal**: incluir `siconv_proposta.csv` para `nr_convenio → cnpj_proponente`
   (custa ~199 MB de download) — só se o `cnpj_executora` do legado for insuficiente.
2. **Cobertura PAC/SIMEC/Avançar**: os ~30 convênios não-SICONV exigem outras fontes
   federais (cada sistema tem seu portal).
