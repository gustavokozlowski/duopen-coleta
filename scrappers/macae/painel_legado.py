"""Scraper do painel legado de obras da SERPRO para Macaé."""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Configuração ──────────────────────────────────────────────────────────────

BASE_URL = os.getenv(
    "PAINEL_LEGADO_URL",
    "https://dd-publico.serpro.gov.br/extensions/obras/obras.html",
)
APP_ID = "c96bb022-352c-4cf4-b912-a65491271cb7"
FILTER_DOM_ID = "fltr-localidade"
FILTER_OBJECT_ID = "e6876f78-6171-439a-80c1-39437587e15b"
TABLE_DOM_ID = "tbl-detalhes-obras"
TABLE_OBJECT_ID = "90821a9f-daf7-477e-9bb6-7df517c000f3"
LOCALIDADE_PADRAO = os.getenv("PAINEL_LEGADO_LOCALIDADE", "Macaé/RJ")
LOCALIDADE_COMPLEMENTAR = os.getenv("PAINEL_LEGADO_LOCALIDADE_COMPLEMENTAR", "Macae/RJ")
WAIT_TIMEOUT = int(os.getenv("PAINEL_LEGADO_WAIT_TIMEOUT", "60"))
QUIET_WAIT_MS = int(os.getenv("PAINEL_LEGADO_QUIET_WAIT_MS", "8000"))
# Timeout do execute_async_script (Qlik). Default do Selenium é 30s, insuficiente
# para o engine Qlik + QUIET_WAIT_MS em runners de CI lentos.
SCRIPT_TIMEOUT = int(os.getenv("PAINEL_LEGADO_SCRIPT_TIMEOUT", "120"))
PAGE_SIZE = int(os.getenv("PAINEL_LEGADO_PAGE_SIZE", "200"))

CACHE_DIR = Path(__file__).resolve().parents[2] / "cache"
CACHE_FILE = CACHE_DIR / "painel_legado_obras.json"


# ── Constantes ────────────────────────────────────────────────────────────────

CAMPO_MAP: dict[str, str] = {
    # Identificação
    "id_obra_obras":              "id_obra",
    "codigo_transacao_obras":     "num_contrato",
    "nr_convenio_obras":          "num_licitacao",

    # Nome e objeto
    "titulo_obras":               "nome_obra",
    "objeto_proposta_obras":      "objeto",

    # Situação
    "situacao_agrupada_obras":    "situacao",

    # Órgão / secretaria
    "desc_orgao_obras":           "secretaria",
    "desc_orgao_sup_obras":       "orgao_superior",

    # Localização
    "endereco_obras":             "endereco",
    "latitude_obras":             "latitude",
    "longitude_obras":            "longitude",
    "munic_proponente_obras":     "municipio",
    "uf_proponente_obras":        "uf",

    # Financeiro
    # nome_tipo_obras contém o valor do contrato como string "R$773.000,00"
    "nome_tipo_obras":            "valor_contrato_str",
    # execucao_fisica contém o valor executado como string "R$618.400,00"
    "execucao_fisica":            "execucao_fisica_str",

    # Percentual executado
    # nome_modalidade_obras contém o percentual como string "40,00%"
    "nome_modalidade_obras":      "percentual_executado_str",

    # Datas — "NaT" deve ser tratado como None
    "dia_inic_vigenc_conv_obras": "data_inicio_str",
    "dia_fim_vigenc_conv_obras":  "data_prevista_fim_str",
    "ano_obras":                  "ano_referencia",
    # Única pista de conclusão do legado: ano (não há data exata). ~40% preenchido.
    "ano_conclusao_obras":        "ano_conclusao",

    # Fornecedor
    "cnpj_executor_obras":        "cnpj_executora",

    # Sistema federal de origem (ex: TRANSFEREGOV.BR, SIMEC, PAC, AVANCAR)
    "sistema_obras":              "sistema_origem",

    # Campos com rótulo enganoso no Qlik HyperCube: contêm dados financeiros, não datas.
    # data_atualizacao_obras  → valor repasse federal (= investimento_total quando não há contrapartida)
    # data_previsao_retomada  → valor contrapartida municipal (zero em projetos sem contrapartida)
    # data_criacao_obras      → valor executado financeiro (= execucao_fisica na maioria dos casos)
    "data_atualizacao_obras":                    "valor_repasse_str",
    "data_previsao_retomada_tratativa_obras":     "valor_contrapartida_str",
    "data_criacao_obras":                        "valor_executado_financeiro_str",
}

VALORES_NULOS_DATA: frozenset[str] = frozenset({
    "NaT", "nat", "nan", "None", "none", "null", "NULL", "", "NaN",
})


# ── Funções utilitárias ───────────────────────────────────────────────────────

def _texto(valor: Any) -> str | None:
    if valor is None:
        return None
    if isinstance(valor, float) and pd.isna(valor):
        return None

    texto = str(valor).strip()
    if texto.lower() in {"", "nan", "none", "null", "-"}:
        return None

    return texto


def _normalizar_texto(valor: Any) -> str:
    texto = _texto(valor) or ""
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = re.sub(r"\s+", " ", texto).strip().upper()
    return texto


def _normalizar_chave(valor: Any) -> str:
    texto = _normalizar_texto(valor)
    return re.sub(r"[^A-Z0-9]+", "", texto)


def _slugificar(valor: Any) -> str:
    texto = _normalizar_texto(valor)
    texto = re.sub(r"[^A-Z0-9]+", "_", texto)
    texto = re.sub(r"_+", "_", texto).strip("_")
    return texto.lower()


def _float(valor: Any) -> float | None:
    texto = _texto(valor)
    if texto is None:
        return None

    texto = texto.replace("R$", "").replace("%", "").replace(" ", "")
    texto = texto.replace(".", "").replace(",", ".")
    texto = re.sub(r"[^0-9\-+.]+", "", texto)
    if texto in {"", "+", "-", ".", "+.", "-."}:
        return None

    try:
        return float(texto)
    except ValueError:
        return None


def _int(valor: Any) -> int | None:
    texto = _texto(valor)
    if texto is None:
        return None

    texto = re.sub(r"[^0-9\-]+", "", texto)
    if texto in {"", "-"}:
        return None

    try:
        return int(texto)
    except ValueError:
        return None


def _campo(registro: Mapping[str, Any], *candidatos: str) -> str | None:
    candidatos_normalizados = [_normalizar_chave(c) for c in candidatos if c]
    for chave, valor in registro.items():
        chave_normalizada = _normalizar_chave(chave)
        if any(
            candidato == chave_normalizada or candidato in chave_normalizada or chave_normalizada in candidato
            for candidato in candidatos_normalizados
        ):
            return _texto(valor)
    return None


# ── Leitura do arquivo CSV/JSON legado ────────────────────────────────────────

def _ler_fonte(caminho: str) -> list[dict]:
    """Lê o arquivo do painel legado (CSV, XLSX ou JSON) e retorna lista de dicts brutos."""
    path = Path(caminho)
    sufixo = path.suffix.lower()

    if sufixo == ".json":
        with open(path, encoding="utf-8") as f:
            dados = json.load(f)
        registros: list[dict] = dados if isinstance(dados, list) else [dados]
    elif sufixo == ".csv":
        df = pd.read_csv(path, encoding="utf-8", sep=None, engine="python")
        registros = df.to_dict(orient="records")
    elif sufixo in (".xlsx", ".xls"):
        df = pd.read_excel(path)
        registros = df.to_dict(orient="records")
    else:
        raise ValueError(f"Formato não suportado: {sufixo}")

    log.info("Arquivo lido: %s (%d registros)", path.name, len(registros))
    return registros


# ── _extrair_campos() ─────────────────────────────────────────────────────────

def _extrair_campos(row: dict) -> dict:
    """Renomeia campos do CSV/JSON legado conforme CAMPO_MAP; ignora campos não mapeados."""
    extraido: dict[str, Any] = {}
    for chave_fonte, chave_destino in CAMPO_MAP.items():
        extraido[chave_destino] = row.get(chave_fonte)

    for chave in row:
        if chave not in CAMPO_MAP:
            log.debug("Campo não mapeado ignorado: %s", chave)

    return extraido


# ── _converter_valor_monetario() ──────────────────────────────────────────────

def _converter_valor_monetario(texto: str) -> Optional[float]:
    """Converte string monetária brasileira para float. Ex: 'R$773.000,00' → 773000.0."""
    if texto is None:
        return None
    if not isinstance(texto, str) or not texto.strip() or texto.strip() in VALORES_NULOS_DATA:
        return None
    try:
        limpo = texto.replace("R$", "").replace(" ", "")
        limpo = limpo.replace(".", "").replace(",", ".")
        return float(limpo)
    except ValueError:
        log.warning("Não foi possível converter valor monetário: %r", texto)
        return None


# ── _converter_percentual() ───────────────────────────────────────────────────

def _converter_percentual(texto: str) -> Optional[float]:
    """Converte string de percentual para float. Ex: '40,00%' → 40.0."""
    if texto is None:
        return None
    if not isinstance(texto, str) or not texto.strip() or texto.strip() in VALORES_NULOS_DATA:
        return None
    try:
        limpo = texto.replace("%", "").replace(" ", "").replace(",", ".")
        return float(limpo)
    except ValueError:
        log.warning("Não foi possível converter percentual: %r", texto)
        return None


def _percentual_financeiro(
    valor_executado: Optional[float], valor_contrato: Optional[float]
) -> Optional[float]:
    """
    Deriva o % executado financeiro = valor_executado / valor_contrato * 100.
    Retorna None quando o contrato é ausente ou zero (evita divisão por zero).
    """
    if not valor_executado or not valor_contrato:
        return None
    try:
        return round(valor_executado / valor_contrato * 100, 2)
    except (ZeroDivisionError, TypeError):
        return None


# ── _converter_data() ─────────────────────────────────────────────────────────

def _converter_data(texto: str) -> Optional[str]:
    """Converte string de data para ISO 8601 UTC. 'NaT'/'nan'/None → None."""
    if texto is None:
        return None
    if not isinstance(texto, str):
        return None
    texto = texto.strip()
    if not texto or texto in VALORES_NULOS_DATA:
        return None

    formatos = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]
    for fmt in formatos:
        try:
            dt = datetime.strptime(texto, fmt).replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue

    log.warning("Não foi possível converter data: %r", texto)
    return None


def _parse_coord(valor: Any) -> float | None:
    """Converte coordenada geográfica para float (decimal ou DMS com hemisfério)."""
    if valor is None:
        return None
    texto = _texto(valor)
    if texto is None or texto in VALORES_NULOS_DATA:
        return None
    texto = texto.strip().upper()

    # formato decimal simples
    if not re.search(r"[NSEW]", texto):
        try:
            return float(texto.replace(",", "."))
        except ValueError:
            return None

    # formato DMS com hemisfério (ex: 22.21.21.S, 41°46'11"W)
    direcao_match = re.search(r"[NSEW]", texto)
    if not direcao_match:
        return None
    direcao = direcao_match.group(0)

    tokens = re.split(r"[^0-9]+", texto)
    tokens = [t for t in tokens if t]
    if not tokens:
        return None

    graus = tokens[0] if len(tokens) >= 1 else "0"
    minutos = tokens[1] if len(tokens) >= 2 else "0"
    segundos = tokens[2] if len(tokens) >= 3 else "0"

    try:
        g = float(graus)
        m = float(minutos)
        s = float(segundos)
    except ValueError:
        return None

    decimal = g + (m / 60.0) + (s / 3600.0)
    if direcao in {"S", "W"}:
        decimal *= -1
    return decimal


# ── _normalizar_linha() ───────────────────────────────────────────────────────

def _normalizar_linha(row: dict) -> dict:
    """Converte campos auxiliares para tipos corretos e preenche campos fixos."""
    result = dict(row)

    result["valor_contrato"] = _converter_valor_monetario(result.pop("valor_contrato_str", None))
    result["valor_final"] = _converter_valor_monetario(result.pop("execucao_fisica_str", None))
    result["percentual_executado"] = _converter_percentual(result.pop("percentual_executado_str", None))
    result["data_inicio"] = _converter_data(result.pop("data_inicio_str", None))
    result["data_prevista_fim"] = _converter_data(result.pop("data_prevista_fim_str", None))
    result["valor_repasse"] = _converter_valor_monetario(result.pop("valor_repasse_str", None))
    result["valor_contrapartida"] = _converter_valor_monetario(result.pop("valor_contrapartida_str", None))
    result["valor_executado_financeiro"] = _converter_valor_monetario(result.pop("valor_executado_financeiro_str", None))
    # % financeiro derivado (componente E do IEOP); físico já vem em percentual_executado.
    # valor_executado_financeiro (data_criacao_obras) vem 0/nulo em ~40% das obras —
    # nesses casos cai para valor_final (execucao_fisica), que tem melhor cobertura e
    # reflete a execução real (ex.: concluída com 90% executado, mas financeiro=0).
    base_executada = result["valor_executado_financeiro"] or result["valor_final"]
    result["percentual_executado_financeiro"] = _percentual_financeiro(
        base_executada, result["valor_contrato"]
    )

    result["latitude"] = _parse_coord(result.get("latitude"))
    result["longitude"] = _parse_coord(result.get("longitude"))
    result["ano_referencia"] = _int(result.get("ano_referencia"))
    result["ano_conclusao"] = _int(result.get("ano_conclusao"))
    result["sistema_origem"] = _texto(result.get("sistema_origem"))

    nome_obra = result.get("nome_obra")
    objeto = result.get("objeto")
    if nome_obra == "CONSTRUCAO" and objeto is not None:
        result["nome_obra"] = f"{objeto} — {nome_obra}"

    result["fonte"] = "painel_obras_legado_macae"
    result["municipio"] = "Macaé"
    result["uf"] = "RJ"

    return result


# ── Selenium / Qlik ───────────────────────────────────────────────────────────

def _executar_js_json(driver: webdriver.Chrome, script: str, operacao: str) -> dict[str, Any]:
    resposta = driver.execute_async_script(script)
    if isinstance(resposta, str) and resposta.startswith("ERR:"):
        raise RuntimeError(f"{operacao}: {resposta[4:]}")

    if isinstance(resposta, dict):
        return resposta

    if not isinstance(resposta, str):
        raise RuntimeError(f"{operacao}: resposta inesperada {type(resposta)!r}")

    try:
        dados = json.loads(resposta)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{operacao}: resposta JSON inválida") from exc

    if isinstance(dados, dict):
        return dados

    raise RuntimeError(f"{operacao}: resposta JSON inesperada {type(dados)!r}")


def _inicializar_driver() -> webdriver.Chrome:
    opcoes = webdriver.ChromeOptions()
    opcoes.add_argument("--headless=new")
    opcoes.add_argument("--no-sandbox")
    opcoes.add_argument("--disable-dev-shm-usage")
    opcoes.add_argument("--disable-gpu")
    opcoes.add_argument("--window-size=1600,1200")

    preferencia_download = {
        "download.prompt_for_download": False,
        "download.default_directory": str(CACHE_DIR),
        "plugins.always_open_pdf_externally": True,
    }
    opcoes.add_experimental_option("prefs", preferencia_download)

    servico = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=servico, options=opcoes)
    driver.set_page_load_timeout(WAIT_TIMEOUT)
    # execute_async_script (Qlik) precisa de timeout maior que o padrão de 30s
    driver.set_script_timeout(SCRIPT_TIMEOUT)
    return driver


def _abrir_painel(driver: webdriver.Chrome) -> None:
    driver.get(BASE_URL)
    WebDriverWait(driver, WAIT_TIMEOUT).until(
        EC.presence_of_element_located((By.ID, FILTER_DOM_ID))
    )


def _obter_candidatos_localidade(driver: webdriver.Chrome) -> list[dict[str, Any]]:
    script = f"""
    const done = arguments[arguments.length - 1];
    require(['js/qlik'], function (qlik) {{
      (async function () {{
        try {{
          const prefix = window.location.pathname.substr(0, window.location.pathname.toLowerCase().lastIndexOf('/extensions') + 1);
          const config = {{
            host: window.location.hostname,
            prefix,
            port: window.location.port,
            isSecure: window.location.protocol === 'https:'
          }};
          const app = qlik.openApp('{APP_ID}', config);
          const model = await app.getObject('{FILTER_DOM_ID}', '{FILTER_OBJECT_ID}');
          const filhos = await model.getChildInfos();
          const child = await model.getChild(filhos[0].qId);
          const candidatos = [];
          let top = 0;
          while (true) {{
            const pages = await child.getListObjectData('/qListObjectDef', [{{ qTop: top, qLeft: 0, qHeight: {PAGE_SIZE}, qWidth: 1 }}]);
            const matrix = (pages && pages[0] && pages[0].qMatrix) ? pages[0].qMatrix : [];
            if (!matrix.length) {{
              break;
            }}
            for (const row of matrix) {{
              const cell = row[0];
              if (cell) {{
                candidatos.push({{
                  qText: cell.qText || null,
                  qElemNumber: cell.qElemNumber,
                  qState: cell.qState || null
                }});
              }}
            }}
            if (matrix.length < {PAGE_SIZE}) {{
              break;
            }}
            top += {PAGE_SIZE};
          }}
          done(JSON.stringify({{ candidatos }}));
        }} catch (error) {{
          done('ERR:' + error.message);
        }}
      }})();
    }});
    """
    resposta = _executar_js_json(driver, script, "coleta de candidatos de localidade")
    candidatos = resposta.get("candidatos", [])
    if not isinstance(candidatos, list):
        raise RuntimeError("coleta de candidatos de localidade: resposta inválida")
    return candidatos


def _normalizar_localidade(localidade: str) -> str:
    texto = _normalizar_texto(localidade).replace(" ", "")
    if "/" not in texto:
        texto = f"{texto}/RJ"
    return texto


def _chave_localidade_literal(localidade: str) -> str:
    texto = (_texto(localidade) or "").replace(" ", "").upper()
    if "/" not in texto:
        texto = f"{texto}/RJ"
    return texto


def _localidades_para_coleta(localidade: str | None) -> list[str]:
    principal = localidade or LOCALIDADE_PADRAO
    localidades = [principal]

    if (
        LOCALIDADE_COMPLEMENTAR
        and _chave_localidade_literal(principal) == _chave_localidade_literal(LOCALIDADE_PADRAO)
    ):
        localidades.append(LOCALIDADE_COMPLEMENTAR)

    deduplicadas: list[str] = []
    vistos: set[str] = set()
    for item in localidades:
        chave = _chave_localidade_literal(item)
        if chave in vistos:
            continue
        vistos.add(chave)
        deduplicadas.append(item)

    return deduplicadas


def _selecionar_localidade(
    candidatos: Sequence[Mapping[str, Any]],
    localidade: str,
    preferir_exata: bool = False,
) -> Mapping[str, Any]:
    alvo = _normalizar_localidade(localidade)
    alvo_literal = _chave_localidade_literal(localidade)
    matches = []
    matches_exatos = []

    for candidato in candidatos:
        q_text = _texto(candidato.get("qText"))
        if q_text and _normalizar_localidade(q_text) == alvo:
            matches.append(candidato)
            if _chave_localidade_literal(q_text) == alvo_literal:
                matches_exatos.append(candidato)

    if not matches:
        raise ValueError(f"Localidade não encontrada no painel legado: {localidade}")

    if preferir_exata and matches_exatos:
        return matches_exatos[0]

    def pontuar(candidato: Mapping[str, Any]) -> tuple[int, int]:
        q_text = _texto(candidato.get("qText")) or ""
        tem_acentos = 1 if any(ord(ch) > 127 for ch in q_text) else 0
        return (tem_acentos, len(q_text))

    return sorted(matches, key=pontuar, reverse=True)[0]


def _obter_metadados_obras_qlik(driver: webdriver.Chrome, q_elem_number: int) -> dict[str, Any]:
    script = f"""
    const done = arguments[arguments.length - 1];
    require(['js/qlik'], function (qlik) {{
        (async function () {{
            try {{
                const prefix = window.location.pathname.substr(0, window.location.pathname.toLowerCase().lastIndexOf('/extensions') + 1);
                const config = {{
                    host: window.location.hostname,
                    prefix,
                    port: window.location.port,
                    isSecure: window.location.protocol === 'https:'
                }};
                const app = qlik.openApp('{APP_ID}', config);
                const model = await app.getObject('{FILTER_DOM_ID}', '{FILTER_OBJECT_ID}');
                const filhos = await model.getChildInfos();
                const child = await model.getChild(filhos[0].qId);
                await app.clearAll();
                await child.selectListObjectValues('/qListObjectDef', [{int(q_elem_number)}], false);
                const table = await app.getObject('{TABLE_DOM_ID}', '{TABLE_OBJECT_ID}');
                await new Promise((resolve) => setTimeout(resolve, {QUIET_WAIT_MS}));
                const layout = await table.getLayout();
                const dimensoes = Array.isArray(layout.qHyperCube.qDimensionInfo) ? layout.qHyperCube.qDimensionInfo : [];
                const medidas = Array.isArray(layout.qHyperCube.qMeasureInfo) ? layout.qHyperCube.qMeasureInfo : [];
                const columns = dimensoes.concat(medidas).map((item) => ({{
                    name: item.qEffectiveDimensionName || item.qFallbackTitle || item.qName || item.qText || null,
                    title: item.qFallbackTitle || item.qEffectiveDimensionName || item.qName || item.qText || null,
                }}));
                const rowCount = (layout.qHyperCube && layout.qHyperCube.qSize && layout.qHyperCube.qSize.qcy) || 0;
                done(JSON.stringify({{ columns, rowCount }}));
            }} catch (error) {{
                done('ERR:' + error.message);
            }}
        }})();
    }});
    """
    resposta = _executar_js_json(driver, script, "coleta dos metadados do painel legado")
    columns = resposta.get("columns", [])
    row_count = resposta.get("rowCount", 0)
    if not isinstance(columns, list):
        raise RuntimeError("coleta dos metadados do painel legado: colunas inválidas")

    try:
        row_count_int = int(row_count)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("coleta dos metadados do painel legado: quantidade de linhas inválida") from exc

    return {"columns": columns, "row_count": max(row_count_int, 0)}


def _obter_pagina_obras_qlik(
    driver: webdriver.Chrome,
    q_top: int,
    q_height: int,
    q_width: int,
) -> list[list[Any]]:
    script = f"""
    const done = arguments[arguments.length - 1];
    require(['js/qlik'], function (qlik) {{
        (async function () {{
            try {{
                const prefix = window.location.pathname.substr(0, window.location.pathname.toLowerCase().lastIndexOf('/extensions') + 1);
                const config = {{
                    host: window.location.hostname,
                    prefix,
                    port: window.location.port,
                    isSecure: window.location.protocol === 'https:'
                }};
                const app = qlik.openApp('{APP_ID}', config);
                const table = await app.getObject('{TABLE_DOM_ID}', '{TABLE_OBJECT_ID}');
                const pages = await table.getHyperCubeData('/qHyperCubeDef', [{{
                    qTop: {int(q_top)},
                    qLeft: 0,
                    qHeight: {int(q_height)},
                    qWidth: {int(q_width)}
                }}]);
                const matrix = (pages && pages[0] && pages[0].qMatrix) ? pages[0].qMatrix : [];
                const rows = matrix.map((row) => row.map((cell) => cell ? cell.qText : null));
                done(JSON.stringify({{ rows }}));
            }} catch (error) {{
                done('ERR:' + error.message);
            }}
        }})();
    }});
    """
    resposta = _executar_js_json(driver, script, "coleta paginada das obras do painel legado")
    rows = resposta.get("rows", [])
    if not isinstance(rows, list):
        raise RuntimeError("coleta paginada das obras do painel legado: linhas inválidas")
    return rows


def _rows_para_registros(columns: Sequence[Any], rows: Sequence[Sequence[Any]]) -> list[dict[str, Any]]:
    registros: list[dict[str, Any]] = []
    nomes_colunas = []

    for coluna in columns:
        if isinstance(coluna, Mapping):
            nome = coluna.get("name") or coluna.get("title") or coluna.get("qName") or coluna.get("qText")
        else:
            nome = coluna
        nomes_colunas.append(_slugificar(nome))

    for row in rows:
        registro: dict[str, Any] = {}
        for indice, nome_coluna in enumerate(nomes_colunas):
            valor = row[indice] if indice < len(row) else None
            registro[nome_coluna] = _texto(valor)
        registros.append(registro)

    return registros


def fetch_obras(localidade: str | None = None) -> list[dict[str, Any]]:
    """Coleta as obras do painel legado usando o engine Qlik."""
    alvo = localidade or LOCALIDADE_PADRAO
    localidades_alvo = _localidades_para_coleta(alvo)
    driver = _inicializar_driver()
    try:
        _abrir_painel(driver)
        candidatos = _obter_candidatos_localidade(driver)
        registros_unicos: list[dict[str, Any]] = []
        chaves_vistas: set[str] = set()

        for localidade_alvo in localidades_alvo:
            selecionada = _selecionar_localidade(
                candidatos,
                localidade_alvo,
                preferir_exata=True,
            )
            log.info(
                "Localidade selecionada no painel legado: %s (%s)",
                selecionada.get("qText"),
                selecionada.get("qElemNumber"),
            )

            payload = _obter_metadados_obras_qlik(driver, int(selecionada["qElemNumber"]))
            columns = payload["columns"]
            total_linhas = payload["row_count"]
            q_width = len(columns)

            rows: list[list[Any]] = []
            for q_top in range(0, total_linhas, PAGE_SIZE):
                q_height = min(PAGE_SIZE, total_linhas - q_top)
                pagina = _obter_pagina_obras_qlik(
                    driver,
                    q_top=q_top,
                    q_height=q_height,
                    q_width=q_width,
                )
                rows.extend(pagina)

            registros = _rows_para_registros(columns, rows)
            for registro in registros:
                chave = (
                    _texto(registro.get("id_obra_obras"))
                    or _texto(registro.get("nr_convenio_obras"))
                    or json.dumps(registro, sort_keys=True, ensure_ascii=False)
                )
                if chave in chaves_vistas:
                    continue
                chaves_vistas.add(chave)
                registros_unicos.append(registro)

        log.info("Obras brutas coletadas: %s", len(registros_unicos))
        return registros_unicos
    finally:
        quit_fn = getattr(driver, "quit", None)
        if callable(quit_fn):
            quit_fn()


# ── Cache ─────────────────────────────────────────────────────────────────────

def _salvar_cache(df: pd.DataFrame) -> None:
    if df.empty:
        return

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_json(CACHE_FILE, orient="records", force_ascii=False, indent=2)
    log.info("Cache salvo: %s (%s registros)", CACHE_FILE.name, len(df))


def _carregar_cache() -> pd.DataFrame:
    if not CACHE_FILE.exists():
        return pd.DataFrame()

    df = pd.read_json(CACHE_FILE, orient="records")
    log.warning("Cache carregado: %s (%s registros)", CACHE_FILE.name, len(df))
    return df


# ── run() ─────────────────────────────────────────────────────────────────────

def run(localidade: str | None = None) -> pd.DataFrame:
    """Pipeline completo: coleta via Qlik → extrai campos → normaliza → salva cache."""
    alvo = localidade or LOCALIDADE_PADRAO
    log.info("Painel legado - início da coleta (localidade=%s)", alvo)

    try:
        registros_brutos = fetch_obras(alvo)
        if not registros_brutos:
            raise RuntimeError("painel legado retornou zero registros")
    except Exception as exc:
        log.error("Falha na coleta do painel legado: %s. Tentando cache local...", exc)
        return _carregar_cache()

    linhas = []
    for row in registros_brutos:
        extraido = _extrair_campos(row)
        extraido["payload_bruto"] = json.dumps(row, ensure_ascii=False, default=str)
        normalizado = _normalizar_linha(extraido)
        linhas.append(normalizado)

    df = pd.DataFrame(linhas)

    com_valor = df["valor_contrato"].notna().sum() if "valor_contrato" in df.columns else 0
    com_lat = df["latitude"].notna().sum() if "latitude" in df.columns else 0
    com_data = df["data_inicio"].notna().sum() if "data_inicio" in df.columns else 0
    log.info(
        "Processados: %d registros | valor_contrato: %d | latitude: %d | data_inicio: %d",
        len(df), com_valor, com_lat, com_data,
    )

    _salvar_cache(df)
    return df


if __name__ == "__main__":
    resultado = run()
    if resultado.empty:
        print("Nenhuma obra coletada.")
    else:
        print(resultado.head(10).to_string(index=False))
