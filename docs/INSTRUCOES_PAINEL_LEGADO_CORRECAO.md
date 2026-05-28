# Instruções para correção: `scrapers/macae/painel_legado.py`
# DUOPEN 2026 — duopen-coleta
# Documento de instrução para AI Agent

---

## 1. Contexto e motivação da correção

A análise do `payload_bruto` na tabela `raw_obras_legado` revelou
que os dados estão sendo coletados corretamente da fonte, mas
**não estão sendo mapeados para as colunas corretas** da Raw table.

Os campos ficam presos dentro do `payload_bruto` em vez de
serem extraídos para as colunas estruturadas da tabela.

### Evidência do problema

Exemplo de registro atual em `raw_obras_legado`:

```
nome_obra:         "CONSTRUCAO"           ← genérico, sem objeto real
valor_contrato:    null                   ← existe no payload
latitude:          null                   ← existe no payload
longitude:         null                   ← existe no payload
data_inicio:       null                   ← existe no payload
data_prevista_fim: null                   ← existe no payload
percentual_executado: null                ← existe no payload
```

O que existe dentro do `payload_bruto` (dados reais da fonte):

```json
"titulo_obras":               "CONSTRUCAO"
"objeto_proposta_obras":      "UBS"
"nome_tipo_obras":            "R$773.000,00"
"nome_modalidade_obras":      "40,00%"
"dia_inic_vigenc_conv_obras": "09/07/2013"
"dia_fim_vigenc_conv_obras":  "22/08/2023"
"latitude_obras":             "-22.30345040975075"
"longitude_obras":            "-41.70458436012268"
"desc_orgao_obras":           "DIRETORIA-EXECUTIVA DO FUNDO NACIONAL DE SAUDE"
"situacao_agrupada_obras":    "Cancelada"
"cnpj_executor_obras":        null
"execucao_fisica":            "R$618.400,00"
"ano_obras":                  "2013"
"dia_fim_vigenc_conv_obras":  "27/12/2013"
```

---

## 2. Mapeamento correto dos campos

### 2.1 Campos do CSV legado → colunas da Raw table

O scraper deve mapear estes campos da fonte para as colunas corretas:

```python
CAMPO_MAP = {
    # Identificação
    "id_obra_obras":              "id_obra",
    "codigo_transacao_obras":     "num_contrato",
    "nr_convenio_obras":          "num_licitacao",

    # Nome e objeto
    "titulo_obras":               "nome_obra",
    "objeto_proposta_obras":      "objeto",    # campo auxiliar

    # Situação
    "situacao_agrupada_obras":    "situacao",

    # Órgão / secretaria
    "desc_orgao_obras":           "secretaria",
    "desc_orgao_sup_obras":       "orgao_superior",   # campo auxiliar

    # Localização
    "endereco_obras":             "endereco",
    "latitude_obras":             "latitude",
    "longitude_obras":            "longitude",
    "munic_proponente_obras":     "municipio",
    "uf_proponente_obras":        "uf",

    # Financeiro
    # ⚠️ nome_tipo_obras contém o valor do contrato como string "R$773.000,00"
    "nome_tipo_obras":            "valor_contrato_str",    # campo auxiliar — converter depois
    # ⚠️ execucao_fisica contém o valor executado como string "R$618.400,00"
    "execucao_fisica":            "execucao_fisica_str",   # campo auxiliar — converter depois

    # Percentual executado
    # ⚠️ nome_modalidade_obras contém o percentual como string "40,00%"
    "nome_modalidade_obras":      "percentual_executado_str",  # campo auxiliar — converter depois

    # Datas
    # ⚠️ "NaT" deve ser tratado como None
    "dia_inic_vigenc_conv_obras": "data_inicio_str",       # campo auxiliar — converter depois
    "dia_fim_vigenc_conv_obras":  "data_prevista_fim_str", # campo auxiliar — converter depois
    "ano_obras":                  "ano_referencia",

    # Fornecedor
    "cnpj_executor_obras":        "cnpj_executora",
}
```

### 2.2 Por que campos "auxiliares"?

Os valores vêm como strings mal formatadas da fonte:
- `"R$773.000,00"` → precisa ser convertido para `float`
- `"40,00%"` → precisa remover `%` e converter para `float`
- `"09/07/2013"` → precisa ser convertido para `datetime UTC`
- `"NaT"` → precisa ser convertido para `None`

O scraper extrai para campos auxiliares e uma função de
normalização converte para os tipos corretos antes de gravar.

---

## 3. Funções a implementar

### 3.1 `_extrair_campos(row: dict) -> dict`

```python
def _extrair_campos(row: dict) -> dict:
    """
    Recebe uma linha do CSV/JSON legado e extrai os campos
    mapeados para um dicionário com as colunas da Raw table.

    Regras:
        - Aplicar CAMPO_MAP para renomear os campos
        - Campos não listados no CAMPO_MAP: ignorar
        - Campos ausentes no row: None
        - Logar DEBUG para cada campo não mapeado encontrado
    """
```

### 3.2 `_converter_valor_monetario(texto: str) -> Optional[float]`

```python
def _converter_valor_monetario(texto: str) -> Optional[float]:
    """
    Converte string monetária brasileira para float.

    Exemplos:
        "R$773.000,00"  → 773000.00
        "R$618.400,00"  → 618400.00
        "R$266.666,67"  → 266666.67
        "R$0,00"        → 0.0
        null / None     → None
        "NaT"           → None

    Passos:
        1. Verificar se é None ou string nula/inválida → None
        2. Remover "R$", espaços
        3. Remover pontos de milhar
        4. Substituir vírgula decimal por ponto
        5. Converter para float
        6. Se ValueError: logar WARNING e retornar None
    """
```

### 3.3 `_converter_percentual(texto: str) -> Optional[float]`

```python
def _converter_percentual(texto: str) -> Optional[float]:
    """
    Converte string de percentual para float.

    Exemplos:
        "40,00%"  → 40.00
        "100,00%" → 100.00
        "60,00%"  → 60.00
        null      → None
        "NaT"     → None

    Passos:
        1. Verificar se é None ou string nula/inválida → None
        2. Remover "%", espaços
        3. Substituir vírgula por ponto
        4. Converter para float
        5. Se ValueError: logar WARNING e retornar None
    """
```

### 3.4 `_converter_data(texto: str) -> Optional[str]`

```python
def _converter_data(texto: str) -> Optional[str]:
    """
    Converte string de data para ISO 8601 UTC.

    Exemplos:
        "09/07/2013"  → "2013-07-09T00:00:00+00:00"
        "27/12/2013"  → "2013-12-27T00:00:00+00:00"
        "NaT"         → None   ← CRÍTICO: tratar como None
        "nan"         → None
        None          → None
        ""            → None

    Valores que devem retornar None:
        VALORES_NULOS = {
            "NaT", "nat", "nan", "None", "none",
            "null", "NULL", "", "NaN"
        }

    Formatos a tentar:
        "%d/%m/%Y"
        "%Y-%m-%d"
        "%d-%m-%Y"
    """
```

### 3.5 `_normalizar_linha(row: dict) -> dict`

```python
def _normalizar_linha(row: dict) -> dict:
    """
    Aplica todas as conversões em uma linha já mapeada.

    Converte os campos auxiliares para os tipos corretos:
        valor_contrato_str      → valor_contrato (float)
        execucao_fisica_str     → valor_final (float)
        percentual_executado_str → percentual_executado (float)
        data_inicio_str         → data_inicio (ISO 8601)
        data_prevista_fim_str   → data_prevista_fim (ISO 8601)

    Remove os campos auxiliares do dicionário após a conversão.

    Gera nome_obra mais descritivo:
        Se nome_obra == "CONSTRUCAO" e objeto não é None:
            nome_obra = f"{objeto} — {nome_obra}"
        Ex: "UBS — CONSTRUCAO"

    Preenche fonte:
        fonte = "painel_obras_legado_macae"

    Preenche municipio:
        municipio = "Macaé"
        uf = "RJ"
    """
```

---

## 4. Estrutura do arquivo corrigido

```python
# ── Configuração ──────────────────────────────────────────────────────────────
# ── Constantes ────────────────────────────────────────────────────────────────
# ── Leitura do arquivo CSV/JSON legado ────────────────────────────────────────
# ── _extrair_campos() ─────────────────────────────────────────────────────────
# ── _converter_valor_monetario() ──────────────────────────────────────────────
# ── _converter_percentual() ───────────────────────────────────────────────────
# ── _converter_data() ─────────────────────────────────────────────────────────
# ── _normalizar_linha() ───────────────────────────────────────────────────────
# ── run() ─────────────────────────────────────────────────────────────────────
```

---

## 5. Leitura do arquivo fonte

O painel legado pode vir como CSV ou JSON dependendo da
estratégia de coleta implementada. Tratar os dois casos:

```python
def _ler_fonte(caminho: str) -> list[dict]:
    """
    Lê o arquivo do painel legado.

    Se .csv ou .xlsx:
        pd.read_csv() ou pd.read_excel()
        Converter para lista de dicts com .to_dict(orient='records')

    Se .json:
        json.load()

    Retornar lista de dicts com os dados brutos da fonte.
    Logar em INFO: total de registros lidos.
    """
```

---

## 6. Pipeline principal `run()`

```python
def run() -> pd.DataFrame:
    """
    Pipeline completo do scraper painel_legado.

    1. Ler arquivo da fonte (_ler_fonte)
    2. Para cada linha:
           a. _extrair_campos(row)    → renomear campos
           b. _normalizar_linha(row)  → converter tipos
    3. Criar DataFrame com os registros normalizados
    4. Salvar cache via fallback.py
    5. Retornar DataFrame

    Logar em INFO ao final:
        Total de registros processados
        Total com valor_contrato preenchido
        Total com latitude preenchida
        Total com data_inicio preenchida

    Retornar DataFrame vazio (não None) se falhar.
    """
```

---

## 7. Schema esperado na saída

O DataFrame retornado por `run()` deve ter estas colunas
preenchidas sempre que disponíveis na fonte:

| Coluna | Tipo esperado | Fonte no CSV |
|---|---|---|
| `id_obra` | str | `id_obra_obras` |
| `nome_obra` | str | `titulo_obras` + `objeto_proposta_obras` |
| `situacao` | str | `situacao_agrupada_obras` |
| `secretaria` | str | `desc_orgao_obras` |
| `endereco` | str | `endereco_obras` |
| `latitude` | float | `latitude_obras` |
| `longitude` | float | `longitude_obras` |
| `valor_contrato` | float | `nome_tipo_obras` (converter de "R$X") |
| `valor_final` | float | `execucao_fisica` (converter de "R$X") |
| `percentual_executado` | float | `nome_modalidade_obras` (converter de "X%") |
| `data_inicio` | str ISO 8601 | `dia_inic_vigenc_conv_obras` |
| `data_prevista_fim` | str ISO 8601 | `dia_fim_vigenc_conv_obras` |
| `ano_referencia` | int | `ano_obras` |
| `cnpj_executora` | str | `cnpj_executor_obras` |
| `num_contrato` | str | `codigo_transacao_obras` |
| `num_licitacao` | str | `nr_convenio_obras` |
| `fonte` | str | sempre `"painel_obras_legado_macae"` |
| `municipio` | str | sempre `"Macaé"` |
| `payload_bruto` | str JSON | registro original completo |

---

## 8. Correção no `cleaner.py`

Adicionar `"NaT"` na lista de valores nulos do cleaner:

```python
# etl/cleaner.py — adicionar na constante VALORES_NULOS
VALORES_NULOS = {
    # Existentes
    None, "", "null", "none", "undefined",
    "n/a", "na", "não informado", "nao informado",
    "-", "--", "s/i", "sem informação",

    # NOVO — valores pandas/numpy que chegam como string
    "NaT", "nat", "NaN", "nan", "inf", "-inf",
}
```

Esta correção impacta todos os scrapers — não apenas o legado.

---

## 9. Testes unitários (`tests/unit/test_scrapers_macae.py`)

```python
def test_converter_valor_monetario_formato_br():
    """'R$773.000,00' → 773000.00"""

def test_converter_valor_monetario_zero():
    """'R$0,00' → 0.0"""

def test_converter_valor_monetario_nat_retorna_none():
    """'NaT' → None"""

def test_converter_valor_monetario_none_retorna_none():
    """None → None"""

def test_converter_percentual_formato_br():
    """'40,00%' → 40.00"""

def test_converter_percentual_cem_porcento():
    """'100,00%' → 100.00"""

def test_converter_data_formato_br():
    """'09/07/2013' → '2013-07-09T00:00:00+00:00'"""

def test_converter_data_nat_retorna_none():
    """'NaT' → None"""

def test_converter_data_nan_retorna_none():
    """'nan' → None"""

def test_extrair_campos_mapeia_latitude():
    """'latitude_obras' → coluna 'latitude'"""

def test_extrair_campos_mapeia_valor_contrato():
    """'nome_tipo_obras' com 'R$773.000,00' → valor_contrato_str"""

def test_normalizar_linha_gera_nome_descritivo():
    """'UBS' + 'CONSTRUCAO' → 'UBS — CONSTRUCAO'"""

def test_normalizar_linha_remove_campos_auxiliares():
    """Campos _str devem ser removidos após conversão"""

def test_run_retorna_dataframe_nao_vazio():
    """run() retorna DataFrame com registros ao ler arquivo válido"""

def test_run_latitude_preenchida():
    """Após run(), latitude não deve ser nula para registros com latitude_obras"""

def test_run_valor_contrato_preenchido():
    """Após run(), valor_contrato não deve ser nulo para registros com nome_tipo_obras"""
```

---

## 10. Validação após a correção

Após reimplementar e rodar o scraper, verificar no Supabase:

```sql
SELECT
    COUNT(*)                        AS total,
    COUNT(valor_contrato)           AS com_valor_contrato,
    COUNT(percentual_executado)     AS com_pct_executado,
    COUNT(data_inicio)              AS com_data_inicio,
    COUNT(data_prevista_fim)        AS com_data_prevista,
    COUNT(latitude)                 AS com_latitude,
    COUNT(cnpj_executora)           AS com_cnpj
FROM raw_obras_legado;
```

**Resultado esperado após a correção:**

| Campo | Antes | Depois |
|---|---|---|
| `valor_contrato` | 0 | > 30 registros |
| `percentual_executado` | 0 | > 30 registros |
| `data_inicio` | 0 | > 20 registros |
| `latitude` | 0 | > 30 registros |

---

## 11. Notas finais para o agente

1. **`"NaT"` é o principal vilão** — o pandas representa datas
   nulas como `NaT` (Not a Time). Quando serializado para JSON
   e depois lido como string, vira `"NaT"`. O cleaner e o scraper
   devem tratar `"NaT"` como `None` em qualquer campo.

2. **`nome_tipo_obras` é o valor do contrato** — o nome do campo
   é enganoso. Na fonte legada, ele contém o valor monetário da
   obra no formato `"R$773.000,00"`, não um tipo de obra.

3. **`nome_modalidade_obras` é o percentual executado** — outro
   campo com nome enganoso. Contém `"40,00%"` que representa
   o percentual de execução física da obra.

4. **`execucao_fisica` é o valor executado** — representa quanto
   foi efetivamente gasto, não o percentual. Mapear para
   `valor_final` na Raw table.

5. **Não apagar registros existentes** — após a correção, rodar
   o scraper com upsert para atualizar os campos nulos dos
   registros já existentes em `raw_obras_legado`. A chave
   de upsert é `id_obra`.