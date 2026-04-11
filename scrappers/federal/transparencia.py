# """
# scrapers/federal/transparencia.py
# Scraper do Portal de Transparência Federal — duopen-coleta
# DUOPEN 2026

# Coleta contratos de obras e licitações vinculadas ao município de Macaé (RJ)
# via API REST do Portal de Transparência do Governo Federal.

# Documentação da API:
#     https://api.portaldatransparencia.gov.br/swagger-ui/index.html

# Endpoints utilizados:
#     GET /api-de-dados/contratos          — contratos por município
#     GET /api-de-dados/licitacoes         — licitações por município

# Autenticação:
#     Header: chave-api-dados: <TRANSPARENCIA_API_KEY>
#     Cadastro gratuito em: https://api.portaldatransparencia.gov.br/

# Variáveis de ambiente (.env):
#     TRANSPARENCIA_API_KEY   chave de acesso à API (obrigatória em produção)
#     IBGE_MUNICIPIO_CODE     código IBGE de Macaé (padrão: 3302403)
#     LOG_LEVEL               nível de log (padrão: INFO)
# """

# import os
# import json
# import time
# import logging
# from datetime import datetime, timezone
# from pathlib import Path
# from typing import Any, Optional, TypeAlias

# import requests
# import pandas as pd
# from dotenv import load_dotenv

# load_dotenv()

# # ── Configuração ──────────────────────────────────────────────────────────────

# logging.basicConfig(
#     level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
#     format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
# )
# log = logging.getLogger("scraper.transparencia")

# BASE_URL        = os.getenv("TRANSPARENCIA_BASE_URL", "https://api.portaldatransparencia.gov.br")
# API_PREFIX      = os.getenv("TRANSPARENCIA_API_PREFIX", "/api-de-dados")
# API_KEY         = os.getenv("TRANSPARENCIA_API_KEY", "")
# ORGAO_CODE      = os.getenv("TRANSPARENCIA_CODIGO_ORGAO", "")
# MUNICIPIO_CODE  = os.getenv("IBGE_MUNICIPIO_CODE", "")   # Macaé
# PAGE_SIZE       = 500        # máximo permitido pela API
# MAX_PAGES       = 50         # limite de segurança (25.000 registros)
# RETRY_ATTEMPTS  = 3
# RETRY_BACKOFF   = 2.0        # segundos base para backoff exponencial
# REQUEST_TIMEOUT = 30         # segundos
# CACHE_DIR       = Path(__file__).parent.parent.parent / "cache"

# # Tipos auxiliares para payload JSON retornado pela API.
# JSONScalar: TypeAlias = str | int | float | bool | None
# JSONValue: TypeAlias = JSONScalar | dict[str, "JSONValue"] | list["JSONValue"]
# JSONDict: TypeAlias = dict[str, JSONValue]
# Record: TypeAlias = dict[str, Any]

# # ── Headers ───────────────────────────────────────────────────────────────────

# def _headers() -> dict[str, str]:
#     h = {
#         "Accept":       "application/json",
#         "Content-Type": "application/json",
#     }
#     if API_KEY:
#         # Header oficial no OpenAPI é "chave-api-dados".
#         # Mantemos "chave-api" por compatibilidade com ambientes legados.
#         h["chave-api-dados"] = API_KEY
#         h["chave-api"] = API_KEY
#     else:
#         log.warning(
#             "TRANSPARENCIA_API_KEY não configurada. "
#             "Requisições sem chave têm limite reduzido de taxa."
#         )
#     return h


# def _build_url(endpoint: str) -> str:
#     """Monta URL final suportando base com ou sem prefixo /api-de-dados."""
#     base = BASE_URL.rstrip("/")
#     if "api.transparencia.gov.br" in base:
#         fixed = base.replace("api.transparencia.gov.br", "api.portaldatransparencia.gov.br")
#         log.warning(
#             "Host legado detectado em TRANSPARENCIA_BASE_URL. "
#             f"Usando host atualizado em runtime: {fixed}"
#         )
#         base = fixed

#     prefix = API_PREFIX.strip("/")
#     path = endpoint.lstrip("/")

#     if path.startswith("api-de-dados/"):
#         return f"{base}/{path}"

#     if base.endswith("/api-de-dados"):
#         return f"{base}/{path}"

#     return f"{base}/{prefix}/{path}"


# # ── Cliente HTTP com retry ────────────────────────────────────────────────────

# def _get(endpoint: str, params: dict[str, Any]) -> JSONValue:
#     """
#     Faz GET com retry e backoff exponencial.
#     Lança RuntimeError após esgotar as tentativas.
#     """
#     url = _build_url(endpoint)
#     for attempt in range(1, RETRY_ATTEMPTS + 1):
#         try:
#             resp = requests.get(
#                 url,
#                 headers=_headers(),
#                 params=params,
#                 timeout=REQUEST_TIMEOUT,
#             )
#             if resp.status_code == 429:
#                 wait = RETRY_BACKOFF * (2 ** attempt)
#                 log.warning(f"Rate limit atingido. Aguardando {wait}s...")
#                 time.sleep(wait)
#                 continue

#             resp.raise_for_status()
#             return resp.json()

#         except requests.exceptions.Timeout:
#             log.warning(
#                 f"Timeout na tentativa {attempt}/{RETRY_ATTEMPTS}: {url} "
#                 f"params={params}"
#             )
#         except requests.exceptions.ConnectionError as e:
#             log.warning(
#                 f"Erro de conexão na tentativa {attempt}/{RETRY_ATTEMPTS}: {url} "
#                 f"params={params} erro={e}"
#             )
#         except requests.exceptions.HTTPError as e:
#             body_text = getattr(resp, "text", "")
#             body_preview = body_text[:400].replace("\n", " ") if body_text else ""
#             log.error(
#                 f"HTTP {resp.status_code} em {url} params={params}: {e}. "
#                 f"body={body_preview}"
#             )

#             if resp.status_code in (401, 403):
#                 log.error(
#                     "Falha de autenticação/autorização. "
#                     "Verifique TRANSPARENCIA_API_KEY e o header chave-api-dados."
#                 )
#             elif resp.status_code == 400:
#                 log.error(
#                     "Parâmetros inválidos para o endpoint. "
#                     "No OpenAPI, contratos/licitacoes exigem 'codigoOrgao'."
#                 )
#             raise
#         except requests.exceptions.RequestException as e:
#             log.warning(
#                 f"Erro HTTP genérico na tentativa {attempt}/{RETRY_ATTEMPTS}: "
#                 f"url={url} params={params} erro={e}"
#             )

#         if attempt < RETRY_ATTEMPTS:
#             wait = RETRY_BACKOFF * (2 ** attempt)
#             log.info(f"Aguardando {wait}s antes da próxima tentativa...")
#             time.sleep(wait)

#     raise RuntimeError(
#         f"Falha após {RETRY_ATTEMPTS} tentativas: {url}"
#     )


# def _params_base() -> dict[str, str]:
#     """
#     Constrói parâmetros base para busca.

#     OpenAPI atual exige codigoOrgao para /contratos e /licitacoes.
#     Para retrocompatibilidade, usa municipioCodigoIbge quando codigoOrgao não está definido.
#     """
#     if ORGAO_CODE:
#         return {"codigoOrgao": ORGAO_CODE}

#     if MUNICIPIO_CODE:
#         log.warning(
#             "TRANSPARENCIA_CODIGO_ORGAO não configurado. "
#             "Usando municipioCodigoIbge para compatibilidade; a API pode retornar 400."
#         )
#         return {"municipioCodigoIbge": MUNICIPIO_CODE}

#     log.warning(
#         "Nenhum filtro principal configurado. Defina TRANSPARENCIA_CODIGO_ORGAO "
#         "(preferencial) ou IBGE_MUNICIPIO_CODE."
#     )
#     return {}


# # ── Paginação ─────────────────────────────────────────────────────────────────

# def _paginar(endpoint: str, params_base: dict[str, Any]) -> list[Record]:
#     """
#     Itera todas as páginas de um endpoint e retorna lista completa de registros.
#     Para quando a página retorna vazia ou atinge MAX_PAGES.
#     """
#     registros: list[Record] = []
#     pagina = 1

#     while pagina <= MAX_PAGES:
#         params = {**params_base, "pagina": pagina, "tamanhoDaPagina": PAGE_SIZE}
#         log.info(f"[{endpoint}] Página {pagina}...")

#         try:
#             data = _get(endpoint, params)
#         except RuntimeError as e:
#             log.error(f"Abandonando paginação em {endpoint} na página {pagina}: {e}")
#             break

#         # API retorna lista diretamente ou dict com chave 'data'
#         if isinstance(data, list):
#             itens = [item for item in data if isinstance(item, dict)]
#         elif isinstance(data, dict):
#             raw_items = data.get("data", data.get("content", []))
#             if isinstance(raw_items, list):
#                 itens = [item for item in raw_items if isinstance(item, dict)]
#             else:
#                 itens = []
#         else:
#             itens = []

#         if not itens:
#             log.info(f"[{endpoint}] Fim dos resultados na página {pagina}.")
#             break

#         registros.extend(itens)
#         log.info(f"[{endpoint}] +{len(itens)} registros (total: {len(registros)})")

#         if len(itens) < PAGE_SIZE:
#             break

#         pagina += 1
#         time.sleep(0.3)  # respeita rate limit entre páginas

#     return registros


# # ── Scrapers por endpoint ─────────────────────────────────────────────────────

# def fetch_contratos() -> list[Record]:
#     """
#     Coleta contratos de obras públicas vinculados ao município de Macaé.

#     Filtros aplicados:
#         - municipioCodigoIbge: 3302403 (Macaé)
#         - Filtragem posterior por modalidade/objeto relacionado a obras
#     """
#     log.info("Iniciando coleta de contratos...")
#     params = _params_base()
#     registros = _paginar("contratos", params)
#     log.info(f"Contratos coletados: {len(registros)}")
#     return registros


# def fetch_licitacoes() -> list[Record]:
#     """
#     Coleta licitações de obras públicas vinculadas ao município de Macaé.

#     Filtros aplicados:
#         - municipioCodigoIbge: 3302403 (Macaé)
#         - modalidadeCompra relacionada a obras (filtrado no normalizar)
#     """
#     log.info("Iniciando coleta de licitações...")
#     params = _params_base()
#     registros = _paginar("licitacoes", params)
#     log.info(f"Licitações coletadas: {len(registros)}")
#     return registros


# # ── Normalização ──────────────────────────────────────────────────────────────

# # Modalidades que indicam obras de infraestrutura
# MODALIDADES_OBRA = {
#     "Concorrência",
#     "Tomada de Preços",
#     "Convite",
#     "Pregão",
#     "Dispensa de Licitação",
#     "Inexigibilidade",
#     "RDC",           # Regime Diferenciado de Contratações
#     "Concurso",
# }

# # Palavras-chave no objeto do contrato que indicam obras
# KEYWORDS_OBRA = {
#     "obra", "construção", "reforma", "ampliação", "pavimentação",
#     "drenagem", "saneamento", "urbanização", "infraestrutura",
#     "edificação", "restauração", "revitalização", "implantação",
# }


# def _e_obra(texto: Optional[str]) -> bool:
#     """Verifica se o objeto do contrato/licitação é relacionado a obras."""
#     if not texto:
#         return False
#     texto_lower = texto.lower()
#     return any(kw in texto_lower for kw in KEYWORDS_OBRA)


# def _normalizar_valor(valor) -> Optional[float]:
#     """Converte valor monetário de qualquer formato para float."""
#     if valor is None:
#         return None
#     if isinstance(valor, (int, float)):
#         return float(valor)
#     # Remove R$, pontos de milhar e substitui vírgula decimal
#     s = str(valor).replace("R$", "").replace(".", "").replace(",", ".").strip()
#     try:
#         return float(s)
#     except ValueError:
#         return None


# def _normalizar_data(data_str) -> Optional[str]:
#     """Normaliza datas para ISO 8601 UTC."""
#     if not data_str:
#         return None
#     formatos = [
#         "%d/%m/%Y",
#         "%Y-%m-%d",
#         "%d/%m/%Y %H:%M:%S",
#         "%Y-%m-%dT%H:%M:%S",
#         "%Y-%m-%dT%H:%M:%SZ",
#     ]
#     for fmt in formatos:
#         try:
#             dt = datetime.strptime(str(data_str), fmt)
#             return dt.replace(tzinfo=timezone.utc).isoformat()
#         except ValueError:
#             continue
#     log.debug(f"Formato de data não reconhecido: {data_str}")
#     return None


# def normalizar_contratos(registros: list[Record]) -> pd.DataFrame:
#     """
#     Transforma a lista bruta de contratos em DataFrame normalizado.
#     Filtra apenas registros relacionados a obras.
#     """
#     if not registros:
#         return pd.DataFrame()

#     rows = []
#     for r in registros:
#         objeto = r.get("objetoContrato") or r.get("objeto") or ""
#         if not _e_obra(objeto):
#             continue

#         rows.append({
#             "id_contrato":          r.get("numero") or r.get("id"),
#             "fonte":                "portal_transparencia_federal",
#             "municipio_ibge":       MUNICIPIO_CODE,
#             "objeto":               objeto,
#             "modalidade":           r.get("modalidadeCompra", {}).get("descricao") if isinstance(r.get("modalidadeCompra"), dict) else r.get("modalidade"),
#             "valor_inicial":        _normalizar_valor(r.get("valorInicialCompra") or r.get("valorContrato")),
#             "valor_global":         _normalizar_valor(r.get("valorGlobalContrato") or r.get("valorGlobal")),
#             "data_assinatura":      _normalizar_data(r.get("dataAssinatura")),
#             "data_inicio_vigencia": _normalizar_data(r.get("dataInicioVigencia")),
#             "data_fim_vigencia":    _normalizar_data(r.get("dataFimVigencia")),
#             "cnpj_fornecedor":      r.get("fornecedor", {}).get("cnpjFormatado") if isinstance(r.get("fornecedor"), dict) else r.get("cnpjFornecedor"),
#             "nome_fornecedor":      r.get("fornecedor", {}).get("nome") if isinstance(r.get("fornecedor"), dict) else r.get("nomeFornecedor"),
#             "unidade_gestora":      r.get("unidadeGestora", {}).get("nome") if isinstance(r.get("unidadeGestora"), dict) else r.get("unidadeGestora"),
#             "coletado_em":          datetime.now(timezone.utc).isoformat(),
#             "payload_bruto":        json.dumps(r, ensure_ascii=False),
#         })

#     df = pd.DataFrame(rows)
#     log.info(f"Contratos normalizados: {len(df)} de obras (de {len(registros)} total)")
#     return df


# def normalizar_licitacoes(registros: list[Record]) -> pd.DataFrame:
#     """
#     Transforma a lista bruta de licitações em DataFrame normalizado.
#     Filtra apenas registros relacionados a obras.
#     """
#     if not registros:
#         return pd.DataFrame()

#     rows = []
#     for r in registros:
#         objeto = r.get("objeto") or r.get("descricaoObjeto") or ""
#         if not _e_obra(objeto):
#             continue

#         rows.append({
#             "id_licitacao":    r.get("numero") or r.get("id"),
#             "fonte":           "portal_transparencia_federal",
#             "municipio_ibge":  MUNICIPIO_CODE,
#             "objeto":          objeto,
#             "modalidade":      r.get("modalidadeCompra", {}).get("descricao") if isinstance(r.get("modalidadeCompra"), dict) else r.get("modalidade"),
#             "situacao":        r.get("situacaoCompra", {}).get("descricao") if isinstance(r.get("situacaoCompra"), dict) else r.get("situacao"),
#             "valor_estimado":  _normalizar_valor(r.get("valorEstimadoTotal") or r.get("valorTotal")),
#             "data_abertura":   _normalizar_data(r.get("dataAbertura") or r.get("dataAberturaPropostas")),
#             "data_publicacao": _normalizar_data(r.get("dataPublicacao")),
#             "unidade_gestora": r.get("unidadeGestora", {}).get("nome") if isinstance(r.get("unidadeGestora"), dict) else r.get("unidadeGestora"),
#             "coletado_em":     datetime.now(timezone.utc).isoformat(),
#             "payload_bruto":   json.dumps(r, ensure_ascii=False),
#         })

#     df = pd.DataFrame(rows)
#     log.info(f"Licitações normalizadas: {len(df)} de obras (de {len(registros)} total)")
#     return df


# # ── Fallback ──────────────────────────────────────────────────────────────────

# def _salvar_cache(nome: str, dados: list[Record]) -> None:
#     """Salva resultado em cache local para uso como fallback."""
#     CACHE_DIR.mkdir(parents=True, exist_ok=True)
#     path = CACHE_DIR / f"{nome}.json"
#     with open(path, "w", encoding="utf-8") as f:
#         json.dump(dados, f, ensure_ascii=False, indent=2)
#     log.info(f"Cache salvo: {path}")


# def _carregar_cache(nome: str) -> list[Record]:
#     """Carrega cache local. Retorna lista vazia se não existir."""
#     path = CACHE_DIR / f"{nome}.json"
#     if not path.exists():
#         log.warning(f"Cache não encontrado: {path}")
#         return []
#     with open(path, encoding="utf-8") as f:
#         dados: Any = json.load(f)
#     if not isinstance(dados, list):
#         log.warning(f"Formato de cache inválido em {path}. Retornando lista vazia.")
#         return []
#     registros = [item for item in dados if isinstance(item, dict)]
#     log.warning(f"Usando cache local: {path} ({len(registros)} registros)")
#     return registros


# # ── Pipeline principal ────────────────────────────────────────────────────────

# def run() -> dict[str, pd.DataFrame]:
#     """
#     Executa o pipeline completo de coleta do Portal de Transparência Federal.

#     Retorna:
#         dict com DataFrames normalizados:
#             "contratos"   — contratos de obras
#             "licitacoes"  — licitações de obras

#     Fallback:
#         Se a API falhar, usa cache local da última execução bem-sucedida.
#     """
#     log.info("=" * 55)
#     log.info("Portal de Transparência Federal — início da coleta")
#     log.info(f"Município: Macaé (IBGE {MUNICIPIO_CODE})")
#     log.info("=" * 55)

#     resultados = {}

#     # ── Contratos ──
#     try:
#         raw_contratos = fetch_contratos()
#         _salvar_cache("transparencia_contratos", raw_contratos)
#     except Exception as e:
#         log.error(f"Falha ao coletar contratos: {e}. Usando cache.")
#         raw_contratos = _carregar_cache("transparencia_contratos")

#     resultados["contratos"] = normalizar_contratos(raw_contratos)

#     # ── Licitações ──
#     try:
#         raw_licitacoes = fetch_licitacoes()
#         _salvar_cache("transparencia_licitacoes", raw_licitacoes)
#     except Exception as e:
#         log.error(f"Falha ao coletar licitações: {e}. Usando cache.")
#         raw_licitacoes = _carregar_cache("transparencia_licitacoes")

#     resultados["licitacoes"] = normalizar_licitacoes(raw_licitacoes)

#     # ── Resumo ──
#     log.info("=" * 55)
#     for nome, df in resultados.items():
#         log.info(f"  {nome}: {len(df)} registros")
#     log.info("=" * 55)
#     log.info("Coleta finalizada.")

#     return resultados


# if __name__ == "__main__":
#     dataframes = run()

#     # Preview rápido para validação manual
#     for nome, df in dataframes.items():
#         if not df.empty:
#             print(f"\n── {nome.upper()} ({len(df)} registros) ──")
#             print(df[["id_contrato" if "id_contrato" in df.columns else "id_licitacao",
#                        "objeto", "valor_inicial" if "valor_inicial" in df.columns else "valor_estimado",
#                        "data_assinatura" if "data_assinatura" in df.columns else "data_abertura"]].head())