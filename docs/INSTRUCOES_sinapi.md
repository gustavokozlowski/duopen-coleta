# Instrucoes: `scrappers/federal/sinapi.py`
# DUOPEN 2026 - duopen-coleta
# Documento de instrucao para AI Agent
# Revisado e validado com base no codigo real do projeto - 2026-05-29

---

## 1. Contexto

O SINAPI (Sistema Nacional de Pesquisa de Custos e Indices da
Construcao Civil) fornece os custos de referencia por m2 para
cada tipo de obra. Essencial para calcular o componente C
(Custo por m2) do IEOP:

```
razao = custo_real_m2 / sinapi_referencia_m2
```

---

## 2. Divergencias corrigidas em relacao as versoes anteriores

| Item | Versao errada | Corrigido para |
|---|---|---|
| Caminho do arquivo | `scrapers/federal/sinapi.py` | `scrappers/federal/sinapi.py` (dois "p") |
| Retorno de `run()` | `dict` | `pd.DataFrame` (uma linha por tipo_obra) |
| Fonte de dados | IBGE SIDRA + Portal Transparencia | Tabela embutida (unica fonte confiavel) |
| Cache | JSON com dict aninhado | JSON lista-de-registros (formato padrao do pipeline) |
| Migration | Nao mencionada | Requer `009_create_raw_sinapi.sql` |
| Routing | Nao mencionado | Requer entrada em `etl/routing.py` |
| Nome do dict de rotas | `DATASET_ROUTES` | **`RAW_LAYER_ROUTING`** (nome real no codigo) |
| Chave da rota | `"chave": [lista]` | **`"conflict": (tupla)`** + **`"fonte"` obrigatorio** |
| Local de `RAW_TABLE_COLUMNS` | "routing.py ou loader.py" | Confirmado: `etl/routing.py`, entradas sao `frozenset` |
| Coluna `fonte` no DataFrame | Definida no `run()` | Sobrescrita pelo routing (`.assign(fonte=...)`) |

---

## 3. Localizacao no projeto

```
duopen-coleta/
  scrappers/
    federal/
      sismob.py        <- referencia de implementacao (modelo real)
      sinapi.py        <- NOVO arquivo a criar
```

> Nota: `transparencia.py` so existe como `.pyc` compilado (sem fonte
> versionado). Usar **`sismob.py` como unico modelo de referencia**.

---

## 4. Contrato de `run()` - alinhar com o pipeline

O `pipeline.py` espera que todo scraper retorne `pd.DataFrame` e
grave o cache como lista de registros. O `etl/routing.py` roteia
pelo stem do arquivo de cache (`sinapi.json` -> chave `"sinapi"`).

```python
def run() -> pd.DataFrame:
    """
    Retorna DataFrame com uma linha por tipo_obra - NAO um dict.

    Schema do DataFrame (deve bater com raw_sinapi):
        uf          : str  - sempre 'RJ'
        competencia : str  - formato YYYY-MM (ex: '2026-05')
        tipo_obra   : str  - chave da SINAPI_REFERENCIA_RJ
        custo_m2    : float
        coletado_em : str  - ISO 8601 UTC (o loader sobrescreve em UTC)

    NAO incluir a coluna 'fonte' no DataFrame: o pipeline a
    sobrescreve via routing (`.assign(fonte=rota["fonte"])`),
    entao defini-la aqui nao tem efeito.

    Salva cache em cache/sinapi.json como lista de registros
    (mesmo formato de todos os outros scrapers do projeto).
    """
```

---

## 5. Fonte de dados - tabela embutida (unica para o prototipo)

Nao usar IBGE SIDRA nem Portal da Transparencia nesta versao.
Motivos:
- `TRANSPARENCIA_API_KEY` nao esta no `.env.example` do projeto
- Endpoints de SINAPI do IBGE SIDRA precisam de verificacao
- A tabela embutida e suficiente e confiavel para o hackathon

```python
# scrappers/federal/sinapi.py

# Valores aproximados para Rio de Janeiro
# Fonte: SINAPI/CUB RJ - referencia 2026
# Atualizar manualmente a cada trimestre
SINAPI_REFERENCIA_RJ = {
    # tipo_obra            custo_m2 (R$)
    "residencial_popular":      1850.00,
    "residencial_normal":       2400.00,
    "residencial_alto_padrao":  3800.00,
    "comercial_salas_lojas":    2200.00,
    "galpao_industrial":        1200.00,
    "escola":                   2800.00,
    "ubs":                      3200.00,
    "upa":                      3500.00,
    "caps":                     3000.00,
    "quadra_esportiva":          900.00,
    "pavimentacao_asfalto":      450.00,
    "pavimentacao_concreto":     650.00,
    "drenagem":                  380.00,
    "calcamento":                350.00,
    "praca_urbanizacao":         800.00,
    "padrao":                   2000.00,  # fallback generico
}
```

---

## 6. Mapeamento tipo_obra -> categoria SINAPI

`mapear_tipo_sinapi()` e funcao auxiliar usada pelo duopen-ml ao
calcular o componente C: dado o `tipo_obra` cru de uma obra,
retorna a chave correspondente em `SINAPI_REFERENCIA_RJ`.

```python
import unicodedata

TIPO_PARA_SINAPI = {
    # chaves mais especificas primeiro (a ordem importa na busca parcial)
    "ubs":            "ubs",
    "upa":            "upa",
    "caps":           "caps",
    "hospital":       "ubs",
    "creche":         "escola",
    "colegio":        "escola",
    "escola":         "escola",
    "cras":           "comercial_salas_lojas",
    "creas":          "comercial_salas_lojas",
    "recapeamento":   "pavimentacao_asfalto",
    "pavimentacao":   "pavimentacao_asfalto",
    "drenagem":       "drenagem",
    "galeria":        "drenagem",
    "calcamento":     "calcamento",
    "praca":          "praca_urbanizacao",
    "parque":         "praca_urbanizacao",
    "quadra":         "quadra_esportiva",
    "construcao":     "residencial_normal",
    "reforma":        "residencial_normal",
    "ampliacao":      "residencial_normal",
}


def _sem_acento(texto: str) -> str:
    """Remove acentos e baixa para minusculas (busca robusta)."""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def mapear_tipo_sinapi(tipo_obra: str) -> str:
    """
    Busca parcial case-insensitive e sem acentos.
    Retorna 'padrao' se nao encontrar.

    A ordem das chaves em TIPO_PARA_SINAPI importa para casos
    ambiguos - chaves mais especificas vem antes das genericas.
    """
    if not tipo_obra:
        return "padrao"
    t = _sem_acento(tipo_obra)
    for chave, categoria in TIPO_PARA_SINAPI.items():
        if chave in t:
            return categoria
    return "padrao"
```

---

## 7. Implementacao de `run()`

Seguir o padrao de `sismob.py`: logging configurado, `CACHE_DIR`
relativo a raiz do projeto, salvar cache como lista de registros.

```python
def run() -> pd.DataFrame:
    competencia = datetime.now(timezone.utc).strftime("%Y-%m")
    coletado_em = datetime.now(timezone.utc).isoformat()

    rows = [
        {
            "uf":          "RJ",
            "competencia": competencia,
            "tipo_obra":   tipo_obra,
            "custo_m2":    custo_m2,
            "coletado_em": coletado_em,
        }
        for tipo_obra, custo_m2 in SINAPI_REFERENCIA_RJ.items()
    ]

    df = pd.DataFrame(rows)
    _salvar_cache(rows)
    log.info(f"SINAPI: {len(df)} referencias de custo carregadas (fonte: embutida)")
    return df
```

---

## 8. Integracao no ETL - 3 passos obrigatorios

### 8.1 Migration `009_create_raw_sinapi.sql`

Ultima migration existente: `008_create_raw_convenios.sql` -> usar `009`.
`raw_sinapi` e uma tabela de referencia pequena; nao usa `payload_bruto`
(seria apenas uma copia redundante das 5 colunas). Por isso a coluna e
omitida tanto da tabela quanto de `RAW_TABLE_COLUMNS` (o loader so envia
`payload_bruto` se ele estiver entre as colunas permitidas).

```sql
-- migrations/009_create_raw_sinapi.sql
CREATE TABLE IF NOT EXISTS raw_sinapi (
    id           UUID        DEFAULT gen_random_uuid() PRIMARY KEY,
    uf           TEXT        NOT NULL DEFAULT 'RJ',
    competencia  TEXT        NOT NULL,
    tipo_obra    TEXT        NOT NULL,
    custo_m2     NUMERIC(10, 2) NOT NULL,
    fonte        TEXT        NOT NULL DEFAULT 'sinapi_embutida',
    coletado_em  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_raw_sinapi UNIQUE (uf, competencia, tipo_obra)
);
```

### 8.2 Registrar colunas em `RAW_TABLE_COLUMNS` (`etl/routing.py`)

`RAW_TABLE_COLUMNS` esta em `etl/routing.py`; as entradas sao
`frozenset` declarados dentro do literal. O loader filtra os
campos do cache por este conjunto antes do upsert.

```python
"raw_sinapi": frozenset({
    "id", "uf", "competencia", "tipo_obra", "custo_m2",
    "fonte", "coletado_em",
}),
```

### 8.3 Adicionar rota em `RAW_LAYER_ROUTING` (`etl/routing.py`)

ATENCAO: o dict real chama-se `RAW_LAYER_ROUTING` (nao
`DATASET_ROUTES`). O pipeline le `rota["fonte"]` e `rota["conflict"]`
(pipeline.py) - ambos sao obrigatorios; `conflict` e **tupla**.
A chave do dict e o stem do arquivo de cache (`sinapi.json` -> `"sinapi"`).

```python
# etl/routing.py - dentro do literal RAW_LAYER_ROUTING
"sinapi": {
    "tabela":   "raw_sinapi",
    "fonte":    "sinapi_embutida",
    "conflict": ("uf", "competencia", "tipo_obra"),
    # invariante (test_routing): `required` precisa conter todas as chaves de conflict
    "required": ("uf", "competencia", "tipo_obra", "custo_m2"),
},
```

---

## 9. Variaveis de ambiente

Nenhuma variavel nova necessaria. A tabela embutida nao requer
autenticacao nem dependencias externas alem de `pandas` e
`python-dotenv` (ja no projeto).

---

## 10. Testes

```python
def test_run_retorna_dataframe():
    """run() retorna pd.DataFrame nao-vazio."""

def test_run_uma_linha_por_tipo():
    """run() tem uma linha por entrada de SINAPI_REFERENCIA_RJ."""

def test_schema_dataframe():
    """Colunas: uf, competencia, tipo_obra, custo_m2, coletado_em."""

def test_mapear_tipo_sinapi_ubs():
    """mapear_tipo_sinapi('UBS Lagomar') -> 'ubs'."""

def test_mapear_tipo_sinapi_com_acento():
    """mapear_tipo_sinapi('Pavimentacao da Rua X') -> 'pavimentacao_asfalto'."""

def test_mapear_tipo_sinapi_desconhecido():
    """mapear_tipo_sinapi('xpto') -> 'padrao'."""

def test_cache_lista_de_registros():
    """cache/sinapi.json e uma lista de dicts (formato do pipeline)."""
```

---

## 11. Notas para o agente

1. **Referenciar `sismob.py` como modelo** - mesmo padrao de
   estrutura, logging e retorno `pd.DataFrame`.

2. **Nao adicionar dependencias novas** - so `pandas`,
   `python-dotenv` e a stdlib (`datetime`, `unicodedata`, `json`,
   `logging`, `pathlib`).

3. **Migration**: ultima e `008` -> usar `009`. Conferir o diretorio
   `migrations/` antes para garantir que `009` ainda esta livre.

4. **Routing**: abrir `etl/routing.py` antes de editar. O dict de
   rotas e `RAW_LAYER_ROUTING` e o de colunas e `RAW_TABLE_COLUMNS`,
   ambos no mesmo arquivo. `conflict` e tupla; `fonte` e obrigatorio.

5. **`fonte` sobrescrita**: o pipeline aplica `.assign(fonte=rota["fonte"])`,
   entao o valor final no banco vem da rota, nao do `run()`.
