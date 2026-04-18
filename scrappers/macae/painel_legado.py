"""Scraper do painel legado de obras da SERPRO para Macaé."""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

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
WAIT_TIMEOUT = int(os.getenv("PAINEL_LEGADO_WAIT_TIMEOUT", "60"))
QUIET_WAIT_MS = int(os.getenv("PAINEL_LEGADO_QUIET_WAIT_MS", "8000"))
PAGE_SIZE = int(os.getenv("PAINEL_LEGADO_PAGE_SIZE", "1000"))

CACHE_DIR = Path(__file__).resolve().parents[2] / "cache"
CACHE_FILE = CACHE_DIR / "painel_legado_obras.json"

NORMALIZED_COLUMNS = [
    "id_obra",
    "codigo_transacao_obra",
    "origem",
    "numero_instrumento_obra",
    "orgao_superior",
    "orgao",
    "objeto",
    "titulo",
    "situacao_atual",
    "ano_inicio_obra",
    "data_inicio_obra",
    "ano_conclusao_obra",
    "data_fim_obra",
    "uf_proponente",
    "municipio_proponente",
    "endereco",
    "latitude",
    "longitude",
    "link",
    "modalidade",
    "instrumento",
    "tipo",
    "subtipo",
    "cnpj_executor",
    "funcional_programatica",
    "numero_emenda",
    "plurianual_prioritario",
    "pro_brasil",
    "restos_a_pagar",
    "motivo_paralisacao",
    "causa_paralisacao",
    "justificativa_tratativa",
    "data_previsao_retomada",
    "data_criacao_obra",
    "data_atualizacao_obra",
    "execucao_fisica",
    "execucao_financeira",
    "investimento_total",
    "valor_conclusao",
    "valor_empenhado",
    "valor_repasse",
    "valor_desembolsado",
    "fonte",
    "coletado_em",
    "payload_bruto",
]


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


def _data(valor: Any) -> str | None:
    texto = _texto(valor)
    if texto is None:
        return None

    formatos = [
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    ]
    for formato in formatos:
        try:
            data = datetime.strptime(texto, formato).replace(tzinfo=timezone.utc)
            return data.isoformat()
        except ValueError:
            continue

    return texto


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


def _selecionar_localidade(candidatos: Sequence[Mapping[str, Any]], localidade: str) -> Mapping[str, Any]:
    alvo = _normalizar_localidade(localidade)
    matches = []
    for candidato in candidatos:
        q_text = _texto(candidato.get("qText"))
        if q_text and _normalizar_localidade(q_text) == alvo:
            matches.append(candidato)

    if not matches:
        raise ValueError(f"Localidade não encontrada no painel legado: {localidade}")

    def pontuar(candidato: Mapping[str, Any]) -> tuple[int, int]:
        q_text = _texto(candidato.get("qText")) or ""
        tem_acentos = 1 if any(ord(ch) > 127 for ch in q_text) else 0
        return (tem_acentos, len(q_text))

    return sorted(matches, key=pontuar, reverse=True)[0]


def _obter_obras_qlik(driver: webdriver.Chrome, q_elem_number: int) -> dict[str, Any]:
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
          const rows = [];
          let top = 0;
          while (true) {{
            const pages = await table.getHyperCubeData('/qHyperCubeDef', [{{ qTop: top, qLeft: 0, qHeight: {PAGE_SIZE}, qWidth: columns.length }}]);
            const matrix = (pages && pages[0] && pages[0].qMatrix) ? pages[0].qMatrix : [];
            if (!matrix.length) {{
              break;
            }}
            for (const row of matrix) {{
              rows.push(row.map((cell) => cell ? cell.qText : null));
            }}
            if (matrix.length < {PAGE_SIZE}) {{
              break;
            }}
            top += {PAGE_SIZE};
          }}
          done(JSON.stringify({{ columns, rows }}));
        }} catch (error) {{
          done('ERR:' + error.message);
        }}
      }})();
    }});
    """
    resposta = _executar_js_json(driver, script, "coleta das obras do painel legado")
    columns = resposta.get("columns", [])
    rows = resposta.get("rows", [])
    if not isinstance(columns, list) or not isinstance(rows, list):
        raise RuntimeError("coleta das obras do painel legado: resposta inválida")
    return {"columns": columns, "rows": rows}


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
    driver = _inicializar_driver()
    try:
        _abrir_painel(driver)
        candidatos = _obter_candidatos_localidade(driver)
        selecionada = _selecionar_localidade(candidatos, alvo)
        log.info(
            "Localidade selecionada no painel legado: %s (%s)",
            selecionada.get("qText"),
            selecionada.get("qElemNumber"),
        )
        payload = _obter_obras_qlik(driver, int(selecionada["qElemNumber"]))
        registros = _rows_para_registros(payload["columns"], payload["rows"])
        log.info("Obras brutas coletadas: %s", len(registros))
        return registros
    finally:
        quit_fn = getattr(driver, "quit", None)
        if callable(quit_fn):
            quit_fn()


def _normalizar_registro(registro: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id_obra": _campo(registro, "id_obra_obras", "id_obra"),
        "codigo_transacao_obra": _campo(registro, "cod_transacao_obras", "codigo_transacao_obra"),
        "origem": _campo(registro, "origem_obras", "origem"),
        "numero_instrumento_obra": _campo(registro, "numero_instrumento_obras", "numero_instrumento_obra"),
        "orgao_superior": _campo(registro, "orgao_superior_obras", "orgao_superior"),
        "orgao": _campo(registro, "orgao_obras", "orgao"),
        "objeto": _campo(registro, "objeto_obras", "objeto"),
        "titulo": _campo(registro, "titulo_obras", "titulo"),
        "situacao_atual": _campo(registro, "situacao_atual_obras", "situacao_atual"),
        "ano_inicio_obra": _int(_campo(registro, "ano_inicio_obras", "ano_inicio_obra")),
        "data_inicio_obra": _data(_campo(registro, "data_inicio_obras", "data_inicio_obra")),
        "ano_conclusao_obra": _int(_campo(registro, "ano_conclusao_obras", "ano_conclusao_obra")),
        "data_fim_obra": _data(_campo(registro, "data_fim_obras", "data_fim_obra")),
        "uf_proponente": _campo(registro, "uf_proponente_obras", "uf_proponente"),
        "municipio_proponente": _campo(
            registro,
            "munic_proponente_obras",
            "municipio_proponente_obras",
            "municipio_proponente",
        ),
        "endereco": _campo(registro, "endereco_obras", "endereco"),
        "latitude": _float(_campo(registro, "latitude_obras", "latitude")),
        "longitude": _float(_campo(registro, "longitude_obras", "longitude")),
        "link": _campo(registro, "link_obras", "link"),
        "modalidade": _campo(registro, "nome_modalidade_obras", "modalidade"),
        "instrumento": _campo(registro, "nome_instrumento_obras", "instrumento"),
        "tipo": _campo(registro, "nome_tipo_obras", "tipo"),
        "subtipo": _campo(registro, "nome_subtipo_obras", "subtipo"),
        "cnpj_executor": _campo(registro, "cnpj_executor_obras", "cnpj_executor"),
        "funcional_programatica": _campo(registro, "funcional_programatica_obras", "funcional_programatica"),
        "numero_emenda": _campo(registro, "numero_emenda_obras", "numero_emenda"),
        "plurianual_prioritario": _campo(registro, "plurianual_prioritario_obras", "plurianual_prioritario"),
        "pro_brasil": _campo(registro, "pro_brasil_obras", "pro_brasil"),
        "restos_a_pagar": _campo(registro, "restos_a_pagar_obras", "restos_a_pagar"),
        "motivo_paralisacao": _campo(registro, "motivo_paralisacao_obras", "motivo_paralisacao"),
        "causa_paralisacao": _campo(registro, "causa_paralisacao_obras", "causa_paralisacao"),
        "justificativa_tratativa": _campo(registro, "justificativa_tratativa_obras", "justificativa_tratativa"),
        "data_previsao_retomada": _data(
            _campo(
                registro,
                "data_previsao_retomada_tratativa_obras",
                "data_previsao_retomada",
            )
        ),
        "data_criacao_obra": _data(_campo(registro, "data_criacao_obras", "data_criacao_obra")),
        "data_atualizacao_obra": _data(_campo(registro, "data_atualizacao_obras", "data_atualizacao_obra")),
        "execucao_fisica": _campo(registro, "execucao_fisica"),
        "execucao_financeira": _campo(registro, "execucao_financeira"),
        "investimento_total": _campo(registro, "investimento_total"),
        "valor_conclusao": _campo(registro, "valor_conclusao"),
        "valor_empenhado": _campo(registro, "valor_empenhado"),
        "valor_repasse": _campo(registro, "valor_repasse"),
        "valor_desembolsado": _campo(registro, "valor_desembolsado"),
        "fonte": "painel_legado_obras_serpro",
        "coletado_em": datetime.now(timezone.utc).isoformat(),
        "payload_bruto": json.dumps(registro, ensure_ascii=False, default=str),
    }


def normalizar_obras(registros: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    """Normaliza os registros brutos do painel legado para DataFrame."""
    if not registros:
        return pd.DataFrame(columns=NORMALIZED_COLUMNS)

    linhas = [_normalizar_registro(registro) for registro in registros]
    df = pd.DataFrame(linhas, columns=NORMALIZED_COLUMNS)
    log.info("Obras normalizadas: %s registros", len(df))
    return df


def _salvar_cache(df: pd.DataFrame) -> None:
    if df.empty:
        return

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_json(CACHE_FILE, orient="records", force_ascii=False, indent=2)
    log.info("Cache salvo: %s (%s registros)", CACHE_FILE.name, len(df))


def _carregar_cache() -> pd.DataFrame:
    if not CACHE_FILE.exists():
        return pd.DataFrame(columns=NORMALIZED_COLUMNS)

    df = pd.read_json(CACHE_FILE, orient="records")
    log.warning("Cache carregado: %s (%s registros)", CACHE_FILE.name, len(df))
    return df


def run(localidade: str | None = None) -> pd.DataFrame:
    """Executa a coleta completa do painel legado e usa cache como fallback."""
    alvo = localidade or LOCALIDADE_PADRAO
    log.info("Painel legado - início da coleta (localidade=%s)", alvo)

    try:
        registros = fetch_obras(alvo)
        df = normalizar_obras(registros)
        if df.empty:
            raise RuntimeError("painel legado retornou zero registros")
        _salvar_cache(df)
        return df
    except Exception as exc:
        log.error("Falha na coleta do painel legado: %s. Tentando cache local...", exc)
        return _carregar_cache()


if __name__ == "__main__":
    resultado = run()
    if resultado.empty:
        print("Nenhuma obra coletada.")
    else:
        print(resultado.head(10).to_string(index=False))