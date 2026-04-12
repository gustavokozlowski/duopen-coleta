"""
scrapers/macae/portal_macae.py
Scraper do Portal de Transparência de Macaé
duopen-coleta · DUOPEN 2026

Coleta contratos e licitações de obras e serviços de engenharia
firmados pela Prefeitura Municipal de Macaé e suas autarquias/fundações.

Portal: https://transparencia.macae.rj.gov.br

Estratégia de coleta (Selenium + WebDriver):
    Como o portal é uma SPA complexa com renderização JavaScript,
    usamos Selenium para:
    
    CONTRATOS:
        1. Navegar para /contratacoes/contratos
        2. Preencher Tipo de Contrato = "Obras e Serviços de Engenharia"
        3. Clicar em "Buscar"
        4. Aguardar carregamento dos resultados
        5. Clicar em "Exportar CSV"
        6. Capturar o arquivo baixado
    
    LICITAÇÕES:
        1. Navegar para /contratacoes/licitacoespesquisa
        2. Para cada palavra-chave relacionada a obras:
            a. Preencher Palavra-Chave
            b. Clicar em "Buscar"
            c. Aguardar carregamento
            d. Clicar em "Exportar CSV"
        3. Consolidar resultados e remover duplicatas

Variáveis de ambiente (.env):
    MACAE_TRANSPARENCIA_URL   URL base do portal
                              (padrão: https://transparencia.macae.rj.gov.br)
    LOG_LEVEL                 nível de log (padrão: INFO)
    CHROME_HEADLESS           rodar Chrome em headless mode (padrão: True)
    CHROME_NO_SANDBOX         desabilitar sandbox (padrão: True)
"""

import os
import io
import json
import time
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


load_dotenv()

# ── Configuração ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper.portal_macae")

BASE_URL        = os.getenv(
    "MACAE_TRANSPARENCIA_URL",
    "https://transparencia.macae.rj.gov.br"
)
CACHE_DIR       = Path(__file__).parent.parent.parent / "cache"

# Selenium options
CHROME_HEADLESS = os.getenv("CHROME_HEADLESS", "True").lower() == "true"
CHROME_NO_SANDBOX = os.getenv("CHROME_NO_SANDBOX", "True").lower() == "true"
WAIT_TIMEOUT = 20  # segundos para esperar elementos aparece rem

# Tipo de contrato mapeado exatamente como aparece no formulário do portal
TIPO_CONTRATO_OBRAS = "Obras e Serviços de Engenharia"

# Palavras-chave para filtrar licitações relacionadas a obras
KEYWORDS_OBRAS = [
    "obra", "construção", "reforma", "ampliação", "pavimentação",
    "drenagem", "saneamento", "urbanização", "infraestrutura",
    "recapeamento", "iluminação", "praça", "parque", "contenção",
]


# ── Inicialização do Webdriver ────────────────────────────────────────────────

def _inicializar_driver() -> webdriver.Chrome:
    """Cria e retorna instância do Selenium WebDriver (Chrome)."""
    options = webdriver.ChromeOptions()
    
    if CHROME_HEADLESS:
        options.add_argument("--headless")
    
    if CHROME_NO_SANDBOX:
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
    
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    # Configurar para download de arquivos em diretório temporário
    prefs = {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
    }
    options.add_experimental_option("prefs", prefs)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    log.info("WebDriver Chrome inicializado")
    return driver


# ── Utilidades para Selenium ──────────────────────────────────────────────────

def _esperar_elemento(driver: webdriver.Chrome, 
                      by: By, 
                      locator: str,
                      timeout: int = WAIT_TIMEOUT) -> bool:
    """Aguarda elemento aparecer no DOM."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, locator))
        )
        return True
    except Exception as e:
        log.warning(f"Timeout esperando elemento: {locator} — {e}")
        return False


def _ler_csv(conteudo: bytes) -> pd.DataFrame:
    """
    Lê CSV.
    Tenta múltiplos encodings e separadores comuns em portais brasileiros.
    """
    encodings  = ["utf-8-sig", "latin-1", "cp1252", "utf-8"]
    separators = [";", ",", "\t"]

    for enc in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(
                    io.BytesIO(conteudo),
                    sep=sep,
                    encoding=enc,
                    on_bad_lines="skip",
                    dtype=str,
                )
                # Validação mínima: pelo menos 2 colunas e 1 linha
                if df.shape[1] >= 2 and len(df) > 0:
                    log.info(
                        f"CSV lido: {len(df)} linhas, {df.shape[1]} colunas "
                        f"[enc={enc}, sep={repr(sep)}]"
                    )
                    return df
            except Exception:
                continue

    raise ValueError("Não foi possível ler o CSV com nenhuma combinação de encoding/separador.")





# ── FONTE 1 — Contratos de Obras e Serviços de Engenharia ────────────────────

def fetch_contratos() -> pd.DataFrame:
    """
    Exporta contratos de "Obras e Serviços de Engenharia" via CSV usando Selenium.

    Fluxo:
        1. Navega para /contratacoes/contratos
        2. Preenche "Tipo de Contrato" = "Obras e Serviços de Engenharia"
        3. Clica "Buscar"
        4. Aguarda tabela de resultados
        5. Clica "Exportar CSV" e captura o arquivo
    """
    log.info("Coletando contratos de obras...")
    
    driver = None
    download_dir = tempfile.mkdtemp(prefix="portal_macae_")
    
    try:
        driver = _inicializar_driver()
        
        # Passo 1: Navegar
        url = f"{BASE_URL}/contratacoes/contratos"
        log.info(f"Navegando para {url}")
        driver.get(url)
        time.sleep(2)
        
        # Passo 2: Preencher filtro "Tipo de Contrato"
        log.info(f"Preenchendo filtro: Tipo de Contrato = '{TIPO_CONTRATO_OBRAS}'")
        try:
            # Tentar encontrar o select por ID ou name comuns
            select_xpaths = [
                "//select[@name='tipoContrato']",
                "//select[@id='tipoContrato']",
                "//*[contains(@name, 'tipoContrato')]//select",
                "//select[contains(., 'Obras')]",
            ]
            
            select_elem = None
            for xpath in select_xpaths:
                try:
                    select_elem = driver.find_element(By.XPATH, xpath)
                    break
                except:
                    continue
            
            if select_elem:
                select = Select(select_elem)
                select.select_by_visible_text(TIPO_CONTRATO_OBRAS)
                log.info("Tipo de Contrato selecionado")
                time.sleep(0.5)
            else:
                log.warning("Não foi possível encontrar select de Tipo de Contrato")
                
        except Exception as e:
            log.warning(f"Erro ao preencher Tipo de Contrato: {e}")
        
        # Passo 3: Clicar "Buscar"
        log.info("Clicando em 'Buscar'")
        try:
            botao_buscar = None
            # Tentar diferentes seletores para o botão Buscar
            selectors = [
                "//button[contains(text(), 'Buscar')]",
                "//input[@type='button'][contains(@value, 'Buscar')]",
                "//input[@type='submit'][contains(@value, 'Buscar')]",
                "//a[contains(text(), 'Buscar')]",
            ]
            
            for selector in selectors:
                try:
                    botao_buscar = driver.find_element(By.XPATH, selector)
                    break
                except:
                    continue
            
            if botao_buscar:
                driver.execute_script("arguments[0].click();", botao_buscar)
                time.sleep(2)
                log.info("Botão 'Buscar' clicado")
            else:
                log.warning("Não foi possível encontrar botão 'Buscar'")
                
        except Exception as e:
            log.warning(f"Erro ao clicar Buscar: {e}")
        
        # Passo 4: Aguardar resultados
        log.info("Aguardando carregamento dos resultados...")
        if _esperar_elemento(driver, By.XPATH, "//table", WAIT_TIMEOUT):
            log.info("Tabela de resultados carregada")
            time.sleep(1)
        else:
            log.warning("Timeout esperando tabela de resultados")
        
        # Passo 5: Clicar "Exportar CSV"
        log.info("Clicando em 'Exportar CSV'")
        try:
            botao_exportar = None
            selectors = [
                "//button[contains(text(), 'Exportar CSV')]",
                "//input[@value='Exportar CSV']",
                "//a[contains(text(), 'Exportar CSV')]",
                "//*[contains(text(), 'Exportar CSV')]",
            ]
            
            for selector in selectors:
                try:
                    botao_exportar = driver.find_element(By.XPATH, selector)
                    break
                except:
                    continue
            
            if botao_exportar:
                driver.execute_script("arguments[0].click();", botao_exportar)
                log.info("Botão 'Exportar CSV' clicado - aguardando download...")
                
                # Aguardar arquivo ser baixado
                time.sleep(3)
                
                # Procurar pelo arquivo CSV baixado
                csv_files = list(Path(download_dir).glob("*.csv"))
                if csv_files:
                    csv_path = csv_files[0]
                    log.info(f"Arquivo CSV encontrado: {csv_path}")
                    with open(csv_path, "rb") as f:
                        conteudo = f.read()
                    df = _ler_csv(conteudo)
                    log.info(f"Contratos exportados: {len(df)} registros")
                    return df
                else:
                    log.warning("Nenhum arquivo CSV foi baixado")
                    return pd.DataFrame()
            else:
                log.warning("Não foi possível encontrar botão 'Exportar CSV'")
                return pd.DataFrame()
                
        except Exception as e:
            log.error(f"Erro ao exportar CSV: {e}")
            return pd.DataFrame()
        
    except Exception as e:
        log.error(f"Erro no fluxo de coleta de contratos: {e}")
        return pd.DataFrame()
    
    finally:
        if driver:
            driver.quit()
            log.info("WebDriver encerrado")


# ── FONTE 2 — Licitações de Obras ────────────────────────────────────────────

def fetch_licitacoes() -> pd.DataFrame:
    """
    Exporta licitações relacionadas a obras via CSV usando Selenium.

    Fluxo para cada palavra-chave:
        1. Navega para /contratacoes/licitacoespesquisa
        2. Preenche "Palavra-Chave"
        3. Clica "Buscar"
        4. Aguarda tabela de resultados
        5. Clica "Exportar CSV" e captura o arquivo
        6. Consolida resultados de múltiplas palavras-chave
    """
    log.info("Coletando licitações de obras...")
    
    driver = None
    download_dir = tempfile.mkdtemp(prefix="portal_macae_lic_")
    todos = []
    
    try:
        driver = _inicializar_driver()
        
        # URL das licitações
        url = f"{BASE_URL}/contratacoes/licitacoespesquisa"
        
        for keyword in KEYWORDS_OBRAS:
            try:
                log.info(f"Buscando licitações por palavra-chave: '{keyword}'")
                
                # Passo 1: Navegar (só na primeira iteração, depois carrega página novamente)
                log.info(f"Navegando para {url} com keyword='{keyword}'")
                driver.get(url)
                time.sleep(2)
                
                # Passo 2: Preencher "Palavra-Chave"
                log.info(f"Preenchendo Palavra-Chave = '{keyword}'")
                try:
                    input_xpath = [
                        "//input[@name='palavraChave']",
                        "//input[@id='palavraChave']",
                        "//*[contains(@name, 'palavraChave')]",
                    ]
                    
                    input_elem = None
                    for xpath in input_xpath:
                        try:
                            input_elem = driver.find_element(By.XPATH, xpath)
                            break
                        except:
                            continue
                    
                    if input_elem:
                        input_elem.clear()
                        input_elem.send_keys(keyword)
                        time.sleep(0.5)
                        log.info("Palavra-Chave preenchida")
                    else:
                        log.warning("Não foi possível encontrar input de Palavra-Chave")
                        continue
                        
                except Exception as e:
                    log.warning(f"Erro ao preencher Palavra-Chave: {e}")
                    continue
                
                # Passo 3: Clicar "Buscar"
                log.info("Clicando em 'Buscar'")
                try:
                    botao_buscar = None
                    selectors = [
                        "//button[contains(text(), 'Buscar')]",
                        "//input[@type='submit'][contains(@value, 'Buscar')]",
                        "//input[@type='button'][contains(@value, 'Buscar')]",
                    ]
                    
                    for selector in selectors:
                        try:
                            botao_buscar = driver.find_element(By.XPATH, selector)
                            break
                        except:
                            continue
                    
                    if botao_buscar:
                        driver.execute_script("arguments[0].click();", botao_buscar)
                        time.sleep(2)
                        log.info("Botão 'Buscar' clicado")
                    else:
                        log.warning("Não foi possível encontrar botão 'Buscar'")
                        continue
                        
                except Exception as e:
                    log.warning(f"Erro ao clicar Buscar: {e}")
                    continue
                
                # Passo 4: Aguardar resultados
                log.info("Aguardando carregamento dos resultados...")
                if _esperar_elemento(driver, By.XPATH, "//table", WAIT_TIMEOUT):
                    log.info("Tabela de resultados carregada")
                    time.sleep(1)
                
                # Passo 5: Clicar "Exportar CSV"
                log.info("Clicando em 'Exportar CSV'")
                try:
                    botao_exportar = None
                    selectors = [
                        "//button[contains(text(), 'Exportar CSV')]",
                        "//input[@value='Exportar CSV']",
                    ]
                    
                    for selector in selectors:
                        try:
                            botao_exportar = driver.find_element(By.XPATH, selector)
                            break
                        except:
                            continue
                    
                    if botao_exportar:
                        driver.execute_script("arguments[0].click();", botao_exportar)
                        log.info("Botão 'Exportar CSV' clicado - aguardando download...")
                        
                        # Aguardar arquivo
                        time.sleep(3)
                        
                        # Procurar novo arquivo CSV
                        csv_files = list(Path(download_dir).glob("*.csv"))
                        if csv_files:
                            # Pegar o arquivo mais recente
                            csv_path = max(csv_files, key=lambda x: x.stat().st_mtime)
                            log.info(f"Arquivo CSV encontrado: {csv_path}")
                            with open(csv_path, "rb") as f:
                                conteudo = f.read()
                            df = _ler_csv(conteudo)
                            if not df.empty:
                                todos.append(df)
                                log.info(f"Licitações '{keyword}': {len(df)} registros")
                        else:
                            log.warning(f"Nenhum arquivo foi baixado para '{keyword}'")
                    else:
                        log.warning("Não foi possível encontrar botão 'Exportar CSV'")
                        
                except Exception as e:
                    log.error(f"Erro ao exportar CSV para '{keyword}': {e}")
                
                time.sleep(1)  # delay entre keywords
                
            except Exception as e:
                log.error(f"Erro ao processar palavra-chave '{keyword}': {e}")
                continue
        
        if not todos:
            log.warning("Nenhuma licitação coletada")
            return pd.DataFrame()
        
        # Consolidar e deduplicar
        df_total = pd.concat(todos, ignore_index=True)
        
        # Tentar identificar coluna de ID para deduplicação
        col_id = next(
            (c for c in df_total.columns if any(
                kw in c.lower() for kw in ["número", "numero", "id", "código", "codigo"]
            )), None
        )
        
        if col_id:
            antes = len(df_total)
            df_total = df_total.drop_duplicates(subset=[col_id])
            log.info(
                f"Licitações consolidadas: {len(df_total)} únicas "
                f"(removidas {antes - len(df_total)} duplicatas)"
            )
        else:
            df_total = df_total.drop_duplicates()
            log.info(f"Licitações consolidadas: {len(df_total)} registros")
        
        return df_total
        
    except Exception as e:
        log.error(f"Erro no fluxo de coleta de licitações: {e}")
        return pd.DataFrame()
    
    finally:
        if driver:
            driver.quit()
            log.info("WebDriver encerrado")


# ── Normalização ──────────────────────────────────────────────────────────────

def _col(df: pd.DataFrame, *candidatos: str) -> Optional[str]:
    """Encontra a primeira coluna do DataFrame que contenha algum dos candidatos."""
    for c in candidatos:
        match = next(
            (col for col in df.columns if c.lower() in col.lower()),
            None
        )
        if match:
            return match
    return None


def _val(row: pd.Series, col: Optional[str]) -> Optional[str]:
    """Retorna valor de uma coluna ou None se a coluna não existir."""
    if col and col in row.index:
        v = row[col]
        return str(v).strip() if pd.notna(v) and str(v).strip() not in ("", "nan") else None
    return None


def _float(val: Optional[str]) -> Optional[float]:
    if not val:
        return None
    s = str(val).replace("R$", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _data(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    formatos = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y %H:%M:%S"]
    for fmt in formatos:
        try:
            dt = datetime.strptime(val.strip(), fmt).replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue
    return val


def normalizar_contratos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza o DataFrame bruto de contratos para o schema padrão do projeto.
    Tolerante a variações nos nomes de colunas do CSV.
    """
    if df.empty:
        return pd.DataFrame()

    # Mapear colunas pelo nome mais provável
    c_num      = _col(df, "número", "numero", "contrato", "id")
    c_objeto   = _col(df, "objeto", "descrição", "descricao", "assunto")
    c_valor    = _col(df, "valor", "montante", "total")
    c_empresa  = _col(df, "empresa", "fornecedor", "contratado", "razão social")
    c_cnpj     = _col(df, "cnpj", "cpf")
    c_dt_ass   = _col(df, "assinatura", "data contrato", "data inicio", "data")
    c_dt_fim   = _col(df, "vigência", "vigencia", "término", "termino", "fim")
    c_secretaria = _col(df, "secretaria", "órgão", "orgao", "unidade gestora")
    c_tipo_lic = _col(df, "modalidade", "licitação", "licitacao", "tipo lic")
    c_num_lic  = _col(df, "número licitação", "numero licitacao", "num lic")
    c_situacao = _col(df, "situação", "situacao", "status")
    c_aditivo  = _col(df, "aditivo", "acréscimo")

    rows = []
    for _, row in df.iterrows():
        rows.append({
            "id_contrato":        _val(row, c_num),
            "objeto":             _val(row, c_objeto),
            "valor":              _float(_val(row, c_valor)),
            "fornecedor":         _val(row, c_empresa),
            "cnpj_fornecedor":    _val(row, c_cnpj),
            "data_assinatura":    _data(_val(row, c_dt_ass)),
            "data_vigencia_fim":  _data(_val(row, c_dt_fim)),
            "secretaria":         _val(row, c_secretaria),
            "modalidade_licitacao": _val(row, c_tipo_lic),
            "num_licitacao":      _val(row, c_num_lic),
            "situacao":           _val(row, c_situacao),
            "possui_aditivo":     _val(row, c_aditivo),
            "tipo_contrato":      TIPO_CONTRATO_OBRAS,
            "fonte":              "portal_transparencia_macae_contratos",
            "coletado_em":        datetime.now(timezone.utc).isoformat(),
            "payload_bruto":      json.dumps(row.to_dict(), ensure_ascii=False),
        })

    resultado = pd.DataFrame(rows)
    log.info(f"Contratos normalizados: {len(resultado)} registros")
    return resultado


def normalizar_licitacoes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza o DataFrame bruto de licitações para o schema padrão do projeto.
    Tolerante a variações nos nomes de colunas do CSV.
    """
    if df.empty:
        return pd.DataFrame()

    c_num       = _col(df, "número", "numero", "licitação", "licitacao", "id")
    c_objeto    = _col(df, "objeto", "descrição", "descricao", "assunto")
    c_modalidade = _col(df, "modalidade", "tipo", "espécie")
    c_status    = _col(df, "status", "situação", "situacao")
    c_valor     = _col(df, "valor", "estimado", "montante", "total")
    c_dt_aber   = _col(df, "abertura", "data abertura", "data sessão", "data")
    c_dt_pub    = _col(df, "publicação", "publicacao", "homolog")
    c_secretaria = _col(df, "secretaria", "órgão", "orgao", "unidade")
    c_ano       = _col(df, "ano", "exercício", "exercicio")

    rows = []
    for _, row in df.iterrows():
        rows.append({
            "id_licitacao":       _val(row, c_num),
            "objeto":             _val(row, c_objeto),
            "modalidade":         _val(row, c_modalidade),
            "status":             _val(row, c_status),
            "valor_estimado":     _float(_val(row, c_valor)),
            "data_abertura":      _data(_val(row, c_dt_aber)),
            "data_publicacao":    _data(_val(row, c_dt_pub)),
            "secretaria":         _val(row, c_secretaria),
            "ano":                _val(row, c_ano),
            "fonte":              "portal_transparencia_macae_licitacoes",
            "coletado_em":        datetime.now(timezone.utc).isoformat(),
            "payload_bruto":      json.dumps(row.to_dict(), ensure_ascii=False),
        })

    resultado = pd.DataFrame(rows)
    log.info(f"Licitações normalizadas: {len(resultado)} registros")
    return resultado


# ── Cache ─────────────────────────────────────────────────────────────────────

def _salvar_cache(contratos: pd.DataFrame, licitacoes: pd.DataFrame) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    for nome, df in [("portal_macae_contratos", contratos),
                     ("portal_macae_licitacoes", licitacoes)]:
        if not df.empty:
            path = CACHE_DIR / f"{nome}.json"
            df.to_json(path, orient="records", force_ascii=False, indent=2)
            log.info(f"Cache salvo: {path} ({len(df)} registros)")


def _carregar_cache() -> dict[str, pd.DataFrame]:
    resultado = {}
    for nome in ["portal_macae_contratos", "portal_macae_licitacoes"]:
        path = CACHE_DIR / f"{nome}.json"
        if path.exists():
            df = pd.read_json(path, orient="records")
            resultado[nome] = df
            log.warning(f"Cache carregado: {path.name} ({len(df)} registros)")
        else:
            resultado[nome] = pd.DataFrame()
    return resultado


# ── Pipeline principal ────────────────────────────────────────────────────────

def run() -> dict[str, pd.DataFrame]:
    """
    Executa o pipeline completo de coleta do Portal de Transparência de Macaé.

        1. Exporta contratos de obras via CSV (filtro: Obras e Serviços de Engenharia)
        2. Exporta licitações de obras via CSV (busca por palavra-chave)
        3. Normaliza ambos os DataFrames

    Retorna dict:
        "contratos"   — DataFrame com contratos normalizados
        "licitacoes"  — DataFrame com licitações normalizadas

    Em caso de falha, serve o cache da última execução bem-sucedida.
    """
    log.info("=" * 55)
    log.info("Portal de Transparência de Macaé — início da coleta")
    log.info(f"URL: {BASE_URL}")
    log.info("=" * 55)

    try:
        # Etapa 1 — contratos
        df_contratos_raw = fetch_contratos()
        df_contratos = normalizar_contratos(df_contratos_raw)

        # Etapa 2 — licitações
        df_licitacoes_raw = fetch_licitacoes()
        df_licitacoes = normalizar_licitacoes(df_licitacoes_raw)

        # Salvar cache se pelo menos uma fonte retornou dados
        if not df_contratos.empty or not df_licitacoes.empty:
            _salvar_cache(df_contratos, df_licitacoes)

    except Exception as e:
        log.error(f"Falha na coleta: {e}. Tentando cache local...")
        cache = _carregar_cache()
        df_contratos  = cache["portal_macae_contratos"]
        df_licitacoes = cache["portal_macae_licitacoes"]

    resultado = {
        "contratos":  df_contratos,
        "licitacoes": df_licitacoes,
    }

    log.info("=" * 55)
    log.info("Coleta finalizada:")
    log.info(f"  Contratos de obras:  {len(df_contratos)} registros")
    log.info(f"  Licitações de obras: {len(df_licitacoes)} registros")
    log.info("=" * 55)

    return resultado


if __name__ == "__main__":
    resultado = run()

    for nome, df in resultado.items():
        if df.empty:
            print(f"\n{nome}: nenhum dado coletado.")
        else:
            print(f"\n── {nome.upper()} ({len(df)} registros) ──\n")
            colunas_preview = [
                c for c in [
                    "id_contrato", "id_licitacao", "objeto",
                    "valor", "valor_estimado", "fornecedor",
                    "secretaria", "data_assinatura", "data_abertura",
                ] if c in df.columns
            ]
            print(df[colunas_preview].head(10).to_string(index=False))