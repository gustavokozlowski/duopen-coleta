# duopen-coleta

> Pipeline de coleta, tratamento e ingestão de dados de obras públicas · Hackathon DUOPEN 2026

---

## Visão geral

Responsável por toda a ingestão de dados externos para o projeto **Plataforma Inteligente de Análise de Eficiência de Obras Públicas — RJ**.

Coleta dados de **8 fontes públicas**, aplica limpeza/normalização via ETL, comprime campos texto pesados com `zlib` e grava na **camada Raw do Supabase** (PostgreSQL) usando upsert em lote. Executa automaticamente todo dia às **3h BRT** via GitHub Actions — sem servidor dedicado.

```
GitHub Actions (cron 3h BRT)
  → scrappers/        coleta por fonte → cache local
  → pipeline.py       orquestra cleaner → compressor → loader
       ├─ etl/cleaner    limpeza, normalização, validação
       ├─ etl/compressor zlib em campos texto pesados
       ├─ etl/routing    mapeia dataset → tabela Raw
       └─ etl/loader     upsert em lote no Supabase + log em ingestoes
```

---

## Estrutura

```
duopen-coleta/
│
├── pipeline.py                # Orquestrador: lê cache → ETL → carga no Supabase
│
├── scrappers/
│   ├── macae/
│   │   ├── portal_macae.py    # Portal de Transparência de Macaé (Selenium)
│   │   ├── painel_atual.py    # Painel de obras em andamento (Selenium)
│   │   ├── painel_legado.py   # Histórico de obras (Selenium)
│   │   └── egim.py            # EGIM — KML do Google My Maps
│   ├── tce/
│   │   ├── tce_rj.py          # TCE-RJ — contratos, aditivos, obras paralisadas
│   │   └── tce_licitacoes.py  # TCE-RJ — licitações e perfil de fornecedores
│   ├── federal/
│   │   └── sismob.py          # SISMOB Cidadão — obras de saúde do MS
│   └── ibge/
│       └── ibge.py            # IBGE SIDRA + GeoJSON do município
│
├── etl/
│   ├── cleaner.py             # Limpeza e normalização do DataFrame bruto
│   ├── compressor.py          # Compressão zlib em campos texto pesados
│   ├── routing.py             # Mapa dataset → tabela Raw (fonte, conflict, rename, defaults)
│   ├── loader.py              # Upsert em lote no Supabase + log em ingestoes
│   └── fallback.py            # Cache local com expiração (resiliência a falhas)
│
├── tests/
│   ├── unit/                  # Testes unitários por módulo
│   └── integration/           # Testes de integração (Supabase / APIs externas)
│
├── cache/                     # Cache local dos scrapers (gitignored)
├── logs/                      # Logs de execução (gitignored)
│
├── .github/workflows/coleta.yml   # CI: cron 3h BRT + disparo manual por fonte
│
├── .env.example
├── pytest.ini
├── requirements.txt
└── README.md
```

---

## Fontes de dados

Cada scraper produz um ou mais arquivos em `cache/`. O `pipeline.py` resolve cada cache via `etl/routing.py` e grava na tabela Raw correspondente.

| Scraper | Cache produzido | Tabela Raw | Fonte gravada | Tecnologia |
|---|---|---|---|---|
| `scrappers/macae/portal_macae.py` | `portal_macae_contratos.json` | `raw_contratos` | `portal_transparencia_macae_contratos` | Selenium |
| `scrappers/macae/portal_macae.py` | `portal_macae_licitacoes.json` | `raw_licitacoes` | `portal_transparencia_macae_licitacoes` | Selenium |
| `scrappers/macae/painel_atual.py` | `painel_atual.json` | `raw_obras_atual` | `painel_obras_atual_macae` | Selenium |
| `scrappers/macae/painel_legado.py` | `painel_legado_obras.json` | `raw_obras_legado` | `painel_obras_legado_macae` | Selenium |
| `scrappers/macae/egim.py` | `egim.json` | `raw_obras_georef` | `egim_google_mymaps` | KML público |
| `scrappers/tce/tce_rj.py` | `tce_rj_contratos.json` | `raw_contratos` | `tce_rj_contratos` | REST JSON |
| `scrappers/tce/tce_rj.py` | `tce_rj_obras.json` | `raw_obras_paralisadas` | `tce_rj_obras_paralisadas` | REST JSON |
| `scrappers/tce/tce_licitacoes.py` | `tce_contratos.json` | `raw_contratos` | `tce_rj_compras_diretas` | REST JSON |
| `scrappers/tce/tce_licitacoes.py` | `tce_licitacoes.json` | `raw_licitacoes` | `tce_rj_licitacoes` | REST JSON |
| `scrappers/federal/sismob.py` | `sismob.json` | `raw_obras_saude` | `sismob_cidadao` | REST JSON |
| `scrappers/ibge/ibge.py` | `ibge_metadados.json` | `raw_geodados` | `ibge` | REST JSON |

Caches sem rota cadastrada (`tce_rj_aditivos.json`, `tce_perfil_fornecedores.json`, `ibge_macae.geojson`, etc.) são ignorados pelo `pipeline.py` com aviso — destinam-se ao pipeline de features (duopen-ml).

Todos os dados são restritos ao município de **Macaé / RJ** (IBGE `3302403`).

---

## Configuração

### 1. Clonar e instalar dependências

```bash
git clone https://github.com/seu-org/duopen-coleta.git
cd duopen-coleta
pip install -r requirements.txt
```

> Os scrapers que usam Selenium (`portal_macae`, `painel_atual`, `painel_legado`) requerem **Google Chrome** instalado no host. No CI o workflow já provisiona Chrome automaticamente.

### 2. Configurar variáveis de ambiente

```bash
cp .env.example .env
```

Edite o `.env` com suas credenciais. Variáveis essenciais:

```env
# Supabase (obrigatório)
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=sua_service_role_key

# Códigos do município de Macaé
IBGE_MUNICIPIO_CODE=3302403   # 7 dígitos (IBGE)
IBGE_UF_CODE=33
SISMOB_MUNICIPIO_CODE=330240  # 6 dígitos (SISMOB)

# Cache
CACHE_DIR=cache
CACHE_MAX_DIAS=1

# Log
LOG_LEVEL=INFO
```

Veja `.env.example` para a lista completa (incluindo overrides do TCE-RJ, Selenium e painel legado).

> ⚠️ **Nunca commite o arquivo `.env`** — apenas o `.env.example`.
> Use `SUPABASE_KEY` com a chave `service_role`, nunca a `anon` (o loader rejeita chaves anon explicitamente).

---

## Como executar

### Pipeline completo (todos os scrapers + ETL)

```bash
# 1. Roda os scrapers (geram arquivos em cache/)
python scrappers/macae/portal_macae.py
python scrappers/macae/painel_atual.py
python scrappers/macae/painel_legado.py
python scrappers/macae/egim.py
python scrappers/tce/tce_rj.py
python scrappers/tce/tce_licitacoes.py
python scrappers/federal/sismob.py
python scrappers/ibge/ibge.py

# 2. Roda o ETL: lê o cache, transforma e carrega no Supabase
python pipeline.py
```

### Apenas o ETL (com cache pré-existente)

```bash
python pipeline.py
```

O `pipeline.py` descobre todos os arquivos válidos em `cache/`, consulta `etl/routing.py` para resolver a tabela alvo e a chave de conflito de cada dataset, e executa upsert em lote. Datasets sem rota cadastrada são ignorados com aviso. Cada execução grava um registro em `ingestoes` (fonte, status, qtd_registros, duracao).

---

## ETL — detalhamento

### `etl/cleaner.py`

Recebe um DataFrame bruto e devolve um limpo, padronizado e validado.

- Padroniza datas para `datetime64[UTC]` (formatos BR/ISO/epoch)
- Normaliza CNPJ (remove pontuação, valida dígitos verificadores)
- Remove duplicatas (linhas idênticas e por `id_contrato` quando existir)
- Converte valores monetários (`R$ 1.234,56`, `1234.56`) para `float`
- Preenche defaults (`fonte`, `coletado_em`)
- Valida schema mínimo (configurável por dataset via `required_columns`)

### `etl/compressor.py`

Aplica compressão `zlib` (nível 6) nos campos texto pesados quando passa de **64 bytes**.

Campos comprimidos: `objeto_contrato`, `historico_obra`, `razao_social_fornecedor`, `descricao_item`. Taxa esperada: **40–60%** de redução.

### `etl/routing.py`

Mapeia cada dataset (stem do arquivo de cache) para sua configuração de carga:

| Campo | Descrição |
|---|---|
| `tabela` | Tabela Raw de destino no Supabase |
| `fonte` | Valor gravado na coluna `fonte` para identificar a origem |
| `conflict` | Tupla com a chave de upsert (`ON CONFLICT`) — pode ser composta |
| `rename` | Renomeia colunas do cache para o schema da tabela alvo |
| `defaults` | Valores constantes (ex: `municipio_ibge=3302403`) |
| `required` | Schema mínimo para validação no cleaner |

Também publica `RAW_TABLE_COLUMNS` — o conjunto de colunas válidas de cada tabela, usado pelo loader para descartar campos não-mapeados (que seguem preservados em `payload_bruto`).

### `etl/loader.py`

Persiste o DataFrame na tabela alvo. Implementa as 7 regras documentadas no Plano DUOPEN:

| # | Regra |
|---|---|
| 1 | Upsert (`ON CONFLICT DO UPDATE`) — composto quando aplicável (`id_contrato + fonte`, `nome_obra + lat + lng`, etc.) |
| 2 | Batch de **500 registros** por chamada (PostgREST) |
| 3 | `coletado_em` em UTC (ISO 8601) |
| 4 | `payload_bruto` serializado como string JSON (`json.dumps(record, default=str)`) |
| 5 | Falha parcial não aborta — registra IDs com erro e continua |
| 6 | Retry exponencial em erros 5xx (2s, 4s, 8s); 4xx não retenta |
| 7 | Log da execução em `ingestoes` (fonte, status, qtd_registros, duracao_segundos) |

Antes do upsert, o loader também:
- Filtra colunas para o schema da tabela (`allowed_columns` via `routing.colunas_alvo`)
- Deduplica registros pela chave de conflito dentro do mesmo batch

### `etl/fallback.py`

Cache local com expiração para resiliência a falhas temporárias dos scrapers.

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
    print("Cache recente disponível")
```

Variáveis: `CACHE_DIR=cache`, `CACHE_MAX_DIAS=1`.

---

## Camada Raw — tabelas de destino

| Tabela | Conteúdo | Chave de upsert |
|---|---|---|
| `raw_contratos` | Contratos de obras (Macaé, Federal, TCE-RJ) | `id_contrato + fonte` |
| `raw_licitacoes` | Licitações (Macaé, Federal, TCE-RJ) | `id_licitacao + fonte` |
| `raw_obras_paralisadas` | Obras paralisadas no e-TCERJ | `id_obra + fonte` |
| `raw_obras_saude` | Obras de saúde do SISMOB | `proposta_id` |
| `raw_obras_georef` | Obras georreferenciadas (EGIM) | `nome_obra + latitude + longitude` |
| `raw_obras_atual` | Obras em andamento (Macaé) | `id_obra` |
| `raw_obras_legado` | Histórico de obras (Macaé) | `id_obra` |
| `raw_geodados` | Geodados do município (IBGE) | `municipio_id` |
| `ingestoes` | Log de execução do pipeline | (auto) |

> O DDL completo da camada Raw vive em **duopen-infra**. Toda alteração de schema deve ser feita lá e validada pelos dois membros antes de qualquer deploy.

### Campos notáveis por tabela

**`raw_contratos`**
| Campo | Tipo | Origem | Descrição |
|---|---|---|---|
| `num_processo` | TEXT | Portal Macaé | Processo administrativo de origem (ex: `76403/2023`). Chave de cruzamento contrato ↔ licitação. |
| `prazo_dias` | INTEGER | Portal Macaé | Prazo do contrato em dias (convertido de `"300 DIAS"`, `"12 MESES"`). |

**`raw_licitacoes`**
| Campo | Tipo | Origem | Descrição |
|---|---|---|---|
| `num_processo` | TEXT | Portal Macaé | Número do processo licitatório. |

**`raw_obras_saude`** (SISMOB)
| Campo | Tipo | Origem | Descrição |
|---|---|---|---|
| `tipo_recurso_filtro` | TEXT | SISMOB | `"programa"` (ministerial) ou `"emenda"` (emenda parlamentar). |
| `porte_programa` | TEXT | SISMOB | Porte da UBS: `Porte I/II/III`, `Fixa`, `Auditiva e Física`. |
| `possui_etapa_funcionamento` | BOOLEAN | SISMOB | Se a obra tem fase de operação registrada. |
| `forma_execucao_projeto` | TEXT | SISMOB | Modalidade de execução do projeto. |
| `dt_prevista_inauguracao` | TIMESTAMPTZ | SISMOB | Data prevista de inauguração. |
| `fotos_grupos` | TEXT (JSON) | SISMOB | Lista plana de fotos por grupo: `[{grupo, foto_id, dt_atualizacao}]`. URL: `/api/public/fotos/{foto_id}`. |

**`raw_obras_georef`** (EGIM)
| Campo | Tipo | Origem | Descrição |
|---|---|---|---|
| `data_inicio` | TIMESTAMPTZ | EGIM | Data de início da obra (convertida de `"Abril/2022"` → ISO 8601). |
| `setor_administrativo` | TEXT | EGIM | Zona administrativa de Macaé: `SETOR VERDE`, `AZUL`, `VERMELHO`, etc. |
| `objectid` | TEXT | EGIM | ID interno único do Google My Maps. |

---

## Migrations

As migrations ficam em `migrations/` e devem ser rodadas no **SQL Editor do Supabase** em ordem numérica. Cada arquivo é idempotente (`IF NOT EXISTS`).

| Arquivo | O que faz | Status |
|---|---|---|
| `001_unique_constraints_camada_estruturada.sql` | Cria constraints UNIQUE nas tabelas estruturadas (obras, contratos, aditivos) para o upsert do transformer. | Aplicada |
| `002_rename_status_to_situacao_georef.sql` | Renomeia coluna `status` → `situacao` em `raw_obras_georef`. | Aplicada |
| `003_rename_situacao_obra_to_situacao_saude.sql` | Renomeia coluna `situacao_obra` → `situacao` em `raw_obras_saude`. | Aplicada |
| `004_create_features_obras.sql` | Cria tabela `features_obras` (camada analítica de métricas por obra). | Aplicada |
| `005_add_processo_prazo_raw_contratos.sql` | Adiciona `num_processo` e `prazo_dias` a `raw_contratos`; `num_processo` a `raw_licitacoes`. | **Pendente** |
| `006_add_campos_egim_raw_obras_georef.sql` | Adiciona `data_inicio`, `setor_administrativo` e `objectid` a `raw_obras_georef`. | **Pendente** |

---

## Testes

```bash
# Todos os testes
pytest -v

# Apenas unit
pytest -m unit -v

# Apenas integration (precisa do .env configurado)
pytest -m integration -v

# Cobertura
pytest --cov=etl --cov=scrappers --cov-report=term-missing

# Apenas um módulo
pytest tests/test_loader.py -v
```

Padrões:
- Unit em `tests/unit/**` ou `tests/test_<modulo>.py`, com `@pytest.mark.unit`
- Integration em `tests/integration/**`, com `@pytest.mark.integration`
- Configuração central em `pytest.ini`
- Meta de cobertura: **80%** (Plano DUOPEN 2026) — bloqueante no CI

Chamadas externas são mockadas com `pytest-mock`. Os testes unitários rodam offline.

---

## GitHub Actions (`coleta.yml`)

Roda automaticamente todo dia às **3h BRT (06:00 UTC)** e pode ser disparado manualmente — opcionalmente filtrando uma única fonte (`workflow_dispatch.inputs.fonte`).

Estrutura em **3 jobs sequenciais**:

1. **testes** — instala Chrome + dependências, roda unit + integration com `--cov-fail-under=80`. Falha aqui bloqueia a coleta.
2. **coleta** — roda os 8 scrapers (cada um com `continue-on-error: true` para não derrubar os demais) e em seguida `python pipeline.py`. Faz upload do cache e logs como artefatos.
3. **notificar-falha** — escreve um resumo no `GITHUB_STEP_SUMMARY` quando algum job falha.

Notas de CI:
- Actions JavaScript rodam fixadas no Node.js LTS (24) via `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24`.
- Os uploads de artefatos (coverage, logs, caches) estao desativados temporariamente porque `actions/upload-artifact` ainda declara runtime Node 20 e gera warning com Node 24. Reativar quando a action migrar para Node 24.

Secrets necessários (`Settings > Secrets and variables > Actions`):

| Secret | Descrição |
|---|---|
| `SUPABASE_URL` | URL do projeto Supabase |
| `SUPABASE_KEY` | Chave `service_role` |
| `TCE_RJ_TOKEN` | Token do TCE-RJ (opcional — API pública aceita anon) |
| `IBGE_MUNICIPIO_CODE` | `3302403` |
| `SISMOB_MUNICIPIO_CODE` | `330240` |

---

## Dependências principais

```
supabase==2.10.0       # cliente oficial Python para o Supabase
pandas==2.2.2          # manipulação de DataFrames no ETL
numpy==1.26.4
requests==2.32.3       # chamadas HTTP às APIs públicas
beautifulsoup4==4.12.3 # parsing HTML/KML
lxml==5.2.2
selenium==4.25.0       # scrapers do Portal Macaé / Painel
webdriver-manager==4.0.2
openpyxl==3.1.2        # leitura de XLSX
python-dotenv==1.0.1   # carregamento do .env
pytest==8.2.2
pytest-mock==3.14.0
pytest-cov==5.0.0
```

---

## Integração com o projeto

Este repositório é uma das 5 partes do projeto DUOPEN 2026:

| Repositório | Responsável | Descrição |
|---|---|---|
| **duopen-coleta** | Ambos | Este repositório — coleta, ETL e carga na Raw |
| duopen-backend | Renato | API REST + agentes IA (Laravel Cloud) |
| duopen-ml | Ambos | Modelos XGBoost + feature engineering |
| duopen-frontend | Gustavo | React + Streamlit (dashboards) |
| duopen-infra | Ambos | Migrations SQL + Supabase + pg_cron |

> ⚠️ Alterações no schema do banco devem ser feitas em **duopen-infra** e revisadas pelos dois membros antes de qualquer deploy.

---

## Gestão de riscos

| Risco | Mitigação |
|---|---|
| API pública indisponível | Retry com backoff exponencial + cache local via `etl/fallback.py` |
| Schema de resposta alterado | Validação por dataset em `cleaner.clean(required_columns=...)` + `RAW_TABLE_COLUMNS` filtra campos desconhecidos |
| Limite de requisições atingido | Sleep entre páginas + `TCE_RJ_PAGE_SIZE`/`MAX_PAGES` configuráveis |
| Falha parcial no upsert | Loader registra IDs com erro e continua o próximo batch sem abortar |
| Falha em uma fonte derrubar a coleta | `continue-on-error: true` por scraper no workflow + isolamento por dataset no `pipeline.py` |
| Credenciais expostas | `.env` no `.gitignore` + secrets via GitHub Actions; `SUPABASE_KEY` anon é rejeitada pelo loader |

---

## Autores

**Renato Lemos Limongi de Aguiar Moraes**
**Gustavo Kozlowiski**

Hackathon DUOPEN 2026 · Período: 15/03/2026 a 29/05/2026
