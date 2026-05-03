# duopen-coleta

> Pipeline de coleta, tratamento e compressão de dados de obras públicas · Hackathon DUOPEN 2026

---

## Visão geral

Responsável por toda a ingestão de dados externos para o projeto **Plataforma Inteligente de Análise de Eficiência de Obras Públicas – RJ**.

Coleta dados de múltiplas fontes, aplica limpeza e normalização via ETL, comprime campos texto com `zlib` (Huffman) e grava no **Supabase** (PostgreSQL) em lote. Executa automaticamente todo dia às **3h BRT** via GitHub Actions — sem servidor dedicado.

```
GitHub Actions (cron 3h BRT)
  → scrapers/        coleta por fonte
  → etl/cleaner      limpeza e normalização
  → etl/compressor   zlib/Huffman em campos texto
  → etl/loader       upsert em lote no Supabase (Raw)
```

---

## Estrutura

```
duopen-coleta/
│
├── scrapers/
│   ├── tce_rj.py              # TCE-RJ — contratos e aditivos (JSON paginado)
│   ├── transparencia.py       # Portal de Transparência Federal (REST)
│   ├── ibge.py                # IBGE SIDRA + GeoJSON de Macaé
│   └── xd_software.py         # XD Software — exportações XLS/XLS2xd
│
├── etl/
│   ├── cleaner.py             # Limpeza e normalização do DataFrame bruto
│   ├── compressor.py          # Compressão zlib em campos texto pesados
│   └── loader.py              # Upsert em lote no Supabase via supabase-py
│
├── tests/
│   ├── unit/
│   │   └── federal/
│   │       └── test_transparencia_unit.py
│   └── integration/
│       └── federal/
│           └── test_transparencia_integration.py
│
├── .github/
│   └── workflows/
│       └── coleta.yml         # Cron diário 3h BRT + disparo manual
│
├── .env.example
├── requirements.txt
└── README.md
```

---

## Fontes de dados

| Fonte | Arquivo | Formato | Auth |
|---|---|---|---|
| TCE-RJ | `scrapers/tce_rj.py` | JSON paginado | Token Bearer |
| Portal de Transparência | `scrapers/transparencia.py` | REST JSON | API Key (opcional) |
| IBGE SIDRA + Malhas | `scrapers/ibge.py` | JSON + GeoJSON | Sem auth |
| XD Software | `scrapers/xd_software.py` | XLS / XLS2xd (.zip) | Arquivo local |

---

## Configuração

### 1. Clonar e instalar dependências

```bash
git clone https://github.com/seu-org/duopen-coleta.git
cd duopen-coleta
pip install -r requirements.txt
```

### 2. Configurar variáveis de ambiente

```bash
cp .env.example .env
```

Edite o `.env` com suas credenciais:

```env
# Supabase
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=sua_service_role_key

# APIs públicas
TCE_RJ_TOKEN=seu_token_tce_rj
TRANSPARENCIA_API_KEY=sua_chave_opcional
IBGE_MUNICIPIO_CODE=3302403

# XD Software
XD_EXPORT_PATH=/caminho/para/exportacao.xls

# Log
LOG_LEVEL=INFO
```

> ⚠️ **Nunca commite o arquivo `.env`** — apenas o `.env.example`.  
> Use `SUPABASE_KEY` com a chave `service_role`, nunca a `anon`.

---

## Como executar

### Executar pipeline completo

```bash
python scrapers/tce_rj.py
python scrapers/transparencia.py
python scrapers/ibge.py
python scrapers/xd_software.py
python etl/cleaner.py
python etl/compressor.py
python etl/loader.py
```

### Executar apenas um scraper

```bash
python scrapers/tce_rj.py
```

---

## ETL — detalhamento

### `etl/cleaner.py`

Recebe o DataFrame bruto de qualquer scraper e devolve um DataFrame limpo e padronizado.

- Padroniza datas para `datetime64[UTC]`
- Normaliza CNPJ (remove pontuação, valida dígitos verificadores)
- Remove linhas completamente duplicadas por `id_contrato`
- Converte valores monetários de string (`R$ 1.234,56`) para `float`
- Preenche campos obrigatórios ausentes com valores padrão documentados

### `etl/compressor.py`

Aplica compressão `zlib` (que usa Huffman internamente) nos campos texto mais pesados.

Campos comprimidos:
- `objeto_contrato`
- `historico_obra`
- `razao_social_fornecedor`
- `descricao_item`

> Campos com menos de **64 bytes** são ignorados — o overhead do cabeçalho `zlib` não compensaria.  
> Nível de compressão padrão: **6** (equilíbrio entre velocidade e tamanho).  
> Taxa de compressão esperada: **40–60%** nos campos texto.

```python
import zlib

# Comprimir
blob = zlib.compress(texto.encode("utf-8"), level=6)

# Descomprimir
texto = zlib.decompress(blob).decode("utf-8")
```

### `etl/loader.py`

Responsável exclusivamente por gravar os dados no Supabase.

- Destino: tabela `raw_contratos` (TOAST ativo automaticamente no Supabase)
- Lote: **500 registros por chamada** (respeita limites da API PostgREST)
- Upsert por `id_contrato` — sem duplicatas, sem precisar apagar e reinserir
- Registra `coletado_em` (timestamp UTC) em cada registro para auditoria
- Em erro parcial, registra os IDs com falha em log sem abortar o lote

---

## Cache e fallback (etl/fallback.py)

O modulo `etl/fallback.py` centraliza a persistencia de cache para evitar que
falhas temporarias derrubem o pipeline. Use nos scrapers como ultima opcao.

Exemplo rapido:

```python
from etl.fallback import salvar_cache, carregar_cache, cache_valido

def run():
  try:
    df = coletar_dados()
    salvar_cache("minha_fonte", df)
    return df
  except Exception:
    return carregar_cache("minha_fonte")

if cache_valido("minha_fonte"):
  print("Cache recente disponivel")
```

Variaveis de ambiente:

```env
CACHE_DIR=cache
CACHE_MAX_DIAS=1
```

---

## Testes

```bash
# Rodar todos os testes
pytest -v

# Rodar apenas unit
pytest -m unit -v

# Rodar apenas integration
pytest -m integration -v

# Com cobertura
pytest --cov=etl --cov-report=term-missing

# Apenas um módulo
pytest tests/unit/federal/test_transparencia_unit.py -v
```

Padrão do projeto para testes:
- Unit em `tests/unit/**`
- Integration em `tests/integration/**`
- Marcação com `@pytest.mark.unit` e `@pytest.mark.integration`
- Configuração central no `pytest.ini`

Meta de cobertura mínima: **80%** (conforme Plano de Trabalho DUOPEN 2026).

As chamadas de rede são mockadas com `pytest-mock` — os testes rodam sem depender de APIs externas ou do Supabase.

---

## GitHub Actions

O workflow `coleta.yml` roda automaticamente todo dia às **3h BRT** (06:00 UTC) e pode ser disparado manualmente pelo painel do GitHub.

```yaml
on:
  schedule:
    - cron: "0 6 * * *"   # 3h BRT
  workflow_dispatch:        # disparo manual

jobs:
  coleta:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements.txt
      - run: python scrapers/tce_rj.py
      - run: python scrapers/transparencia.py
      - run: python scrapers/ibge.py
      - run: python scrapers/xd_software.py
      - run: python etl/cleaner.py
      - run: python etl/compressor.py
      - run: python etl/loader.py
```

Configure os **Secrets** em `Settings > Secrets and variables > Actions`:

| Secret | Descrição |
|---|---|
| `SUPABASE_URL` | URL do projeto Supabase |
| `SUPABASE_KEY` | Chave service_role |
| `TCE_RJ_TOKEN` | Token do TCE-RJ |
| `TRANSPARENCIA_API_KEY` | Chave do Portal de Transparência |

---

## Dependências

```
supabase          # cliente oficial Python para o Supabase
pandas            # manipulação de DataFrames no ETL
numpy             # operações numéricas auxiliares
requests          # chamadas HTTP para as APIs públicas
openpyxl          # leitura de arquivos XLSX
xlrd              # leitura de arquivos XLS legados
python-dotenv     # carregamento do arquivo .env
pytest            # suite de testes
pytest-mock       # mock de APIs externas nos testes
pytest-cov        # cobertura de testes
```

## Para atualizar todas as dependências no futuro:
```
pip install pip-tools
pip-compile requirements.in  # gera requirements.txt com hashes
```

---

## Integração com o projeto

Este repositório é uma das 5 partes do projeto DUOPEN 2026:

| Repositório | Responsável | Descrição |
|---|---|---|
| **duopen-coleta** | Ambos | Este repositório — coleta, ETL e compressão |
| duopen-backend | Renato | API REST + agentes IA (Laravel Cloud) |
| duopen-ml | Ambos | Modelos XGBoost + feature engineering |
| duopen-frontend | Gustavo | React + Streamlit (dashboards) |
| duopen-infra | Ambos | Migrations SQL + Supabase + pg_cron |

> ⚠️ Alterações no schema do banco devem ser feitas em **duopen-infra** e revisadas pelos dois membros antes de qualquer deploy.

---

## Gestão de riscos

| Risco | Mitigação |
|---|---|
| API pública indisponível | Retry com backoff exponencial + cache do último resultado bem-sucedido |
| Schema de resposta alterado | Validação de campos obrigatórios antes do ETL com log de alerta |
| Limite de requisições atingido | Sleep entre chamadas + rotação de chaves quando disponível |
| Falha parcial no upsert | Log dos IDs com falha sem abortar o lote completo |
| Credenciais expostas | .env no .gitignore + secrets via GitHub Actions |

---

## Autores

**Renato Lemos Limongi de Aguiar Moraes**  
**Gustavo Kozlowiski**

Hackathon DUOPEN 2026 · Período: 15/03/2026 a 29/05/2026