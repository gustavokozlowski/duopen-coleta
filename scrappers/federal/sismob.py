"""
scrapers/federal/sismob.py
Scraper do SISMOB Cidadão — Ministério da Saúde / FNS
duopen-coleta · DUOPEN 2026

Coleta obras de infraestrutura de saúde (UBS, UPA, Academias da Saúde, etc.)
financiadas pelo Ministério da Saúde no município de Macaé (RJ).

─────────────────────────────────────────────────────────────
IMPORTANTE — CONTEXTO DO SISMOB
─────────────────────────────────────────────────────────────
O SISMOB tem dois ambientes:

1. SISMOB Completo (sismob.saude.gov.br)
   - Sistema interno, requer login CNPJ do Fundo Municipal de Saúde
   - Aplicação Vaadin (server-side), sem API pública documentada
   - NÃO acessível por scraper sem credenciais institucionais

2. SISMOB Cidadão (sismobcidadao.saude.gov.br)
   - Portal público, sem autenticação
   - App mobile (iOS/Android) consome API REST por baixo
   - É O CAMINHO VIÁVEL para este projeto

Este scraper usa três estratégias em ordem de prioridade:

  ESTRATÉGIA A — API do app mobile (SISMOB Cidadão)
    Endpoint inferido do tráfego do app:
    GET /api/obras?municipioId=<ibge_code>
    Se funcionar: dados ricos em JSON com geolocalização

  ESTRATÉGIA B — Scraping do portal web SISMOB Cidadão
    HTML público em sismobcidadao.saude.gov.br
    Parsear lista de obras por município via BeautifulSoup

  ESTRATÉGIA C — Portal de Transferências (fallback federal)
    API pública do Ministério da Saúde via Transferegov/SIOPS
    GET transferegov.gov.br/api/... filtrado por área saúde + Macaé

Variáveis de ambiente (.env):
    IBGE_MUNICIPIO_CODE     código IBGE de Macaé (padrão: 3302403)
    LOG_LEVEL               nível de log (padrão: INFO)
─────────────────────────────────────────────────────────────
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Configuração ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper.sismob")

MUNICIPIO_CODE   = os.getenv("IBGE_MUNICIPIO_CODE", "3302403")  # Macaé
REQUEST_TIMEOUT  = 30
RETRY_ATTEMPTS   = 3
RETRY_BACKOFF    = 2.0
CACHE_DIR        = Path(__file__).parent.parent.parent / "cache"

# Endpoints a tentar (estratégia A)
SISMOB_API_CANDIDATES = [
    f"https://sismobcidadao.saude.gov.br/api/obras",
    f"https://sismobcidadao.saude.gov.br/rest/obras",
    f"https://sismobcidadao.saude.gov.br/api/v1/obras",
    f"https://sismob.saude.gov.br/sismob2/api/obras",
]

SISMOB_WEB_BASE = "https://sismobcidadao.saude.gov.br"

TRANSFEREGOV_API = (
    "https://api.transferegov.gestao.gov.br/api-de-dados/transferencias"
)

# Tipos de estabelecimento de saúde que nos interessam
TIPOS_SAUDE = {
    "UBS", "UPA", "Academia da Saúde", "CER", "CAPS",
    "Banco de Leite", "UTI Neonatal", "Centro de Reabilitação",
    "Unidade de Saúde", "Posto de Saúde", "Unidade Básica",
}

# ── Utilitários ───────────────────────────────────────────────────────────────

HEADERS_WEB = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; DUOPEN-coleta/1.0; "
        "+https://github.com/seu-org/duopen-coleta)"
    ),
    "Accept": "application/json, text/html, */*",
}


def _get(url: str, params: dict = None, accept_html: bool = False) -> requests.Response:
    """GET com retry e backoff exponencial."""
    headers = dict(HEADERS_WEB)
    if not accept_html:
        headers["Accept"] = "application/json"

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(
                url,
                headers=headers,
                params=params or {},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 429:
                wait = RETRY_BACKOFF * (2 ** attempt)
                log.warning(f"Rate limit. Aguardando {wait}s...")
                time.sleep(wait)
                continue
            return resp

        except requests.exceptions.Timeout:
            log.warning(f"Timeout tentativa {attempt}/{RETRY_ATTEMPTS}: {url}")
        except requests.exceptions.ConnectionError:
            log.warning(f"Conexão falhou tentativa {attempt}/{RETRY_ATTEMPTS}: {url}")

        if attempt < RETRY_ATTEMPTS:
            time.sleep(RETRY_BACKOFF * attempt)

    raise RuntimeError(f"Falha após {RETRY_ATTEMPTS} tentativas: {url}")


def _normalizar_data(val) -> Optional[str]:
    if not val:
        return None
    formatos = ["%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y"]
    for fmt in formatos:
        try:
            return datetime.strptime(str(val), fmt).replace(
                tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return str(val)


def _normalizar_valor(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).replace("R$", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except ValueError:
        return None


# ── Estratégia A — API do app mobile ─────────────────────────────────────────

def _tentar_api_mobile() -> Optional[list[dict]]:
    """
    Tenta os endpoints candidatos da API usada pelo app SISMOB Cidadão.
    O app mobile (iOS/Android) consome uma REST API não documentada publicamente.
    Testamos os endpoints mais prováveis com o código IBGE de Macaé.

    Retorna lista de registros se algum endpoint responder com JSON válido,
    ou None se todos falharem.
    """
    params_candidatos = [
        {"municipioId": MUNICIPIO_CODE},
        {"codigoIbge": MUNICIPIO_CODE},
        {"ibge": MUNICIPIO_CODE},
        {"municipio": MUNICIPIO_CODE},
    ]

    for url in SISMOB_API_CANDIDATES:
        for params in params_candidatos:
            try:
                log.info(f"Tentando API mobile: {url} params={params}")
                resp = _get(url, params=params)

                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        if isinstance(data, list) and len(data) > 0:
                            log.info(
                                f"API mobile respondeu: {url} "
                                f"({len(data)} registros)"
                            )
                            return data
                        elif isinstance(data, dict):
                            itens = data.get("content") or data.get("data") or data.get("obras")
                            if itens:
                                log.info(f"API mobile respondeu: {url} ({len(itens)} registros)")
                                return itens
                    except ValueError:
                        pass  # não é JSON, tentar próximo

                elif resp.status_code in (401, 403):
                    log.warning(f"Endpoint requer autenticação: {url}")
                    break  # não adianta tentar outros params nesse endpoint

            except RuntimeError:
                continue

    log.warning("Nenhum endpoint da API mobile respondeu com dados.")
    return None


# ── Estratégia B — Scraping do portal web ────────────────────────────────────

def _scraping_portal_web() -> list[dict]:
    """
    Scraping do portal público SISMOB Cidadão.
    Tenta extrair lista de obras do município de Macaé via HTML.

    O portal renderiza dados via JavaScript (SPA), então tentamos
    também o endpoint de dados subjacente que o browser consome.
    """
    registros = []

    # Tentar página de obras por município
    urls_web = [
        f"{SISMOB_WEB_BASE}/municipio/{MUNICIPIO_CODE}",
        f"{SISMOB_WEB_BASE}/obras?municipio={MUNICIPIO_CODE}",
        f"{SISMOB_WEB_BASE}/#/municipio/{MUNICIPIO_CODE}",
    ]

    for url in urls_web:
        try:
            log.info(f"Tentando scraping web: {url}")
            resp = _get(url, accept_html=True)

            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            # Tentar extrair dados de tags JSON embarcadas na página
            scripts = soup.find_all("script")
            for script in scripts:
                if script.string and ("obras" in script.string.lower() or
                                       "municipio" in script.string.lower()):
                    try:
                        # Procurar por JSON embarcado
                        texto = script.string
                        inicio = texto.find("[{")
                        if inicio >= 0:
                            fim = texto.rfind("}]") + 2
                            candidato = texto[inicio:fim]
                            dados = json.loads(candidato)
                            if isinstance(dados, list) and len(dados) > 0:
                                log.info(f"JSON embarcado encontrado: {len(dados)} registros")
                                registros.extend(dados)
                    except (json.JSONDecodeError, ValueError):
                        pass

            # Tentar extrair cards de obras do HTML
            cards = (
                soup.find_all(class_=lambda c: c and "obra" in c.lower()) or
                soup.find_all("article") or
                soup.find_all(class_=lambda c: c and "card" in c.lower())
            )

            for card in cards:
                texto = card.get_text(separator=" ", strip=True)
                if any(t.lower() in texto.lower() for t in TIPOS_SAUDE):
                    registros.append({
                        "fonte_extracao": "html_card",
                        "texto_bruto": texto,
                        "url_origem": url,
                    })

            if registros:
                log.info(f"Scraping web: {len(registros)} registros extraídos de {url}")
                break

        except RuntimeError:
            continue

    if not registros:
        log.warning("Scraping web não retornou registros.")

    return registros


# ── Estratégia C — Transferegov (fallback federal) ───────────────────────────

def _transferegov_saude() -> list[dict]:
    """
    Fallback: busca transferências do Ministério da Saúde para Macaé
    via API pública do Transferegov.

    Filtra por:
      - Município: Macaé (IBGE 3302403)
      - Área: Saúde
      - Tipo: obras e infraestrutura
    """
    log.info("Estratégia C: buscando via Transferegov...")
    registros = []

    params = {
        "codigoMunicipioIbge": MUNICIPIO_CODE,
        "pagina": 1,
        "tamanhoDaPagina": 500,
    }

    try:
        resp = _get(TRANSFEREGOV_API, params=params)
        if resp.status_code == 200:
            data = resp.json()
            itens = data if isinstance(data, list) else data.get("data", [])

            # Filtrar transferências relacionadas a saúde e obras
            for item in itens:
                programa = str(item.get("programa") or "").lower()
                objeto   = str(item.get("objeto") or "").lower()
                funcao   = str(item.get("funcao") or "").lower()

                eh_saude = "saúde" in funcao or "saude" in funcao or "saúde" in programa
                eh_obra  = any(kw in objeto for kw in [
                    "obra", "construção", "reforma", "ampliação", "ubs", "upa"
                ])

                if eh_saude and eh_obra:
                    registros.append(item)

            log.info(f"Transferegov: {len(registros)} transferências de saúde/obras encontradas")
        else:
            log.warning(f"Transferegov retornou status {resp.status_code}")

    except RuntimeError as e:
        log.error(f"Transferegov falhou: {e}")

    return registros


# ── Normalização ──────────────────────────────────────────────────────────────

def normalizar(registros: list[dict], estrategia: str) -> pd.DataFrame:
    """
    Normaliza registros de qualquer estratégia em DataFrame padronizado.
    Campos ausentes recebem None — sem quebrar o pipeline.
    """
    if not registros:
        return pd.DataFrame()

    rows = []
    for r in registros:
        rows.append({
            # Identificação
            "id_obra_sismob":    r.get("id") or r.get("codigoObra") or r.get("numero"),
            "fonte":             f"sismob_{estrategia}",
            "municipio_ibge":    MUNICIPIO_CODE,

            # Dados da obra
            "nome_obra":         r.get("nome") or r.get("nomeObra") or r.get("descricao"),
            "tipo_estabelecimento": r.get("tipoEstabelecimento") or r.get("tipo"),
            "situacao":          r.get("situacao") or r.get("status"),
            "percentual_execucao": r.get("percentualExecucao") or r.get("percentual"),

            # Financeiro
            "valor_repasse":     _normalizar_valor(
                                    r.get("valorRepasse") or
                                    r.get("valorTransferido") or
                                    r.get("valor")
                                 ),
            "valor_contrapartida": _normalizar_valor(r.get("valorContrapartida")),
            "valor_total":       _normalizar_valor(r.get("valorTotal")),

            # Datas
            "data_inicio_previsto": _normalizar_data(
                                        r.get("dataInicioPrevisto") or
                                        r.get("dataInicio")
                                    ),
            "data_fim_previsto": _normalizar_data(
                                    r.get("dataFimPrevisto") or
                                    r.get("dataFim") or
                                    r.get("dataConclusaoPrevista")
                                 ),
            "data_conclusao":    _normalizar_data(r.get("dataConclusao")),

            # Geolocalização (quando disponível via API mobile)
            "latitude":          r.get("latitude") or r.get("lat"),
            "longitude":         r.get("longitude") or r.get("lng") or r.get("lon"),
            "endereco":          r.get("endereco") or r.get("logradouro"),

            # Auditoria
            "coletado_em":       datetime.now(timezone.utc).isoformat(),
            "payload_bruto":     json.dumps(r, ensure_ascii=False),
        })

    df = pd.DataFrame(rows)

    # Remover registros sem identificação mínima
    df = df.dropna(subset=["nome_obra", "situacao"], how="all")

    log.info(
        f"Normalização ({estrategia}): {len(df)} registros prontos"
    )
    return df


# ── Cache ─────────────────────────────────────────────────────────────────────

def _salvar_cache(dados: list[dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / "sismob.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    log.info(f"Cache salvo: {path} ({len(dados)} registros)")


def _carregar_cache() -> list[dict]:
    path = CACHE_DIR / "sismob.json"
    if not path.exists():
        log.warning("Cache SISMOB não encontrado.")
        return []
    with open(path, encoding="utf-8") as f:
        dados = json.load(f)
    log.warning(f"Usando cache local SISMOB ({len(dados)} registros)")
    return dados


# ── Pipeline principal ────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
    """
    Executa o pipeline de coleta do SISMOB com fallback em cascata.

    Ordem de tentativas:
        A → API mobile do SISMOB Cidadão       (dados ricos + geolocalização)
        B → Scraping do portal web SISMOB      (dados parciais)
        C → API Transferegov (saúde + obras)   (dados financeiros federais)
        D → Cache local da última execução     (último recurso)

    Retorna DataFrame normalizado ou DataFrame vazio se todas falharem.
    """
    log.info("=" * 55)
    log.info("SISMOB — início da coleta")
    log.info(f"Município: Macaé (IBGE {MUNICIPIO_CODE})")
    log.info("=" * 55)

    # ── Estratégia A ──
    log.info("Tentando estratégia A: API mobile SISMOB Cidadão...")
    registros = _tentar_api_mobile()
    if registros:
        _salvar_cache(registros)
        return normalizar(registros, "api_mobile")

    # ── Estratégia B ──
    log.info("Tentando estratégia B: scraping portal web...")
    registros = _scraping_portal_web()
    if registros:
        _salvar_cache(registros)
        return normalizar(registros, "scraping_web")

    # ── Estratégia C ──
    log.info("Tentando estratégia C: Transferegov...")
    registros = _transferegov_saude()
    if registros:
        _salvar_cache(registros)
        return normalizar(registros, "transferegov")

    # ── Estratégia D — cache local ──
    log.warning("Todas as estratégias falharam. Usando cache local.")
    registros = _carregar_cache()
    if registros:
        return normalizar(registros, "cache_local")

    log.error("SISMOB: nenhuma fonte disponível. Retornando DataFrame vazio.")
    return pd.DataFrame()


if __name__ == "__main__":
    df = run()

    if df.empty:
        print("\nNenhum dado coletado do SISMOB.")
        print("Verifique os logs e tente validar manualmente em:")
        print("  https://sismobcidadao.saude.gov.br")
    else:
        print(f"\n── SISMOB ({len(df)} registros) ──")
        cols = ["nome_obra", "tipo_estabelecimento", "situacao",
                "valor_repasse", "percentual_execucao"]
        cols_disp = [c for c in cols if c in df.columns]
        print(df[cols_disp].head(10).to_string(index=False))