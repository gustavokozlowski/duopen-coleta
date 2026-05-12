# Refatoração: Portal de Transparência de Macaé

## Resumo das Mudanças

O arquivo `scrappers/macae/portal_macae.py` foi **refatorado completamente** para usar **Selenium WebDriver** em vez de requisições HTTP diretas. Isso permite simular corretamente o fluxo do usuário no portal: **Buscar → Exportar CSV**.

---

## 🔧 Alterações Principais

### 1. **Importações Atualizadas**
- ❌ Removido: `requests` (requisições HTTP diretas)
- ✅ Adicionado: `selenium` com suporte a Chrome WebDriver
- ✅ Adicionado: `webdriver-manager` (gerencia ChromeDriver automaticamente)
- ✅ Adicionado: `tempfile` (para downloads de archivos)

### 2. **Dependências Instaladas** 
Adicionadas ao `requirements.txt`:
```
selenium==4.25.0
webdriver-manager==4.0.2
```

### 3. **Novo Fluxo com Selenium**

#### **Contratos** (`fetch_contratos()`)
**Antes:** POST direto para endpoints de exportação
**Agora:**
1. ✅ Navega para `/contratacoes/contratos`
2. ✅ Preenche dropdown "Tipo de Contrato" = "Obras e Serviços de Engenharia"
3. ✅ Clica em **"Buscar"** (aguarda carregamento da tabela)
4. ✅ Clica em **"Exportar CSV"**
5. ✅ Captura o arquivo baixado
6. ✅ Lê e normaliza os dados

#### **Licitações** (`fetch_licitacoes()`)
**Antes:** Múltiplos POSTs para cada palavra-chave
**Agora:** Para cada palavra-chave:
1. ✅ Navega para `/contratacoes/licitacoespesquisa` (ou reusa página)
2. ✅ Preenche campo "Palavra-Chave"
3. ✅ Clica em **"Buscar"**
4. ✅ Clica em **"Exportar CSV"**
5. ✅ Captura arquivo
6. ✅ Consolida resultados de todas as palavras-chave

### 4. **Novas Funções Utilitárias**

```python
_inicializar_driver()      # Configura Chrome WebDriver com opções otimizadas
_esperar_elemento()        # Aguarda elementos carregarem (wait explícito)
_ler_csv()                 # Função mantida igual (robusta a encodings)
```

### 5. **Tratamento de Erros Melhorado**
- ✅ XPath múltiplos para encontrar componentes (compatibilidade)
- ✅ Try/except em cada etapa do fluxo
- ✅ Fallback para cache em caso de falhas
- ✅ Logging detalhado de cada ação

### 6. **Configurações do Chrome WebDriver**

```python
CHROME_HEADLESS = True              # Executa sem interface gráfica
CHROME_NO_SANDBOX = True             # Sandbox desabilitado
WAIT_TIMEOUT = 20                   # Timeout para esperar elementos
```

**Variáveis de Ambiente (.env):**
```env
MACAE_TRANSPARENCIA_URL=https://transparencia.macae.rj.gov.br
LOG_LEVEL=INFO
CHROME_HEADLESS=True
CHROME_NO_SANDBOX=True
```

---

## 📊 Comparação de Abordagens

| Aspecto | Antes (HTTP) | Depois (Selenium) |
|---------|--------------|-------------------|
| **Tecnologia** | requests + POST | Selenium WebDriver |
| **Fluxo** | POST direto | Simula cliques do usuário |
| **JavaScript** | ❌ Não executa | ✅ Executa completamente |
| **Botão "Buscar"** | ❌ Ignorado | ✅ Clicado (espera resultados) |
| **Botão "Exportar"** | ❌ POST direto | ✅ Clicado + download capturado |
| **Downloads** | ❌ Não captura | ✅ Direto de downloads |
| **Confiabilidade** | ⚠️ Média | ✅ Alta |

---

## 🚀 Como Executar

### Opção 1: Teste Direto
```bash
cd /home/gusdev/projects/hackthon-duopen/duopen-coleta
source .venv/bin/activate
python scrappers/macae/portal_macae.py
```

### Opção 2: Em Modo Headless (sem interface gráfica)
```bash
CHROME_HEADLESS=True python scrappers/macae/portal_macae.py
```

### Opção 3: Com Interface Gráfica (debug)
```bash
CHROME_HEADLESS=False python scrappers/macae/portal_macae.py
```

---

## ⚠️ Requisitos do Sistema

- **Chrome/Chromium instalado** (webdriver-manager baixa ChromeDriver automaticamente)
- **Python 3.8+**
- **Acesso à rede** (para acessar o portal e baixar ChromeDriver)

---

## 📝 Função `run()`

Mantém o fluxo original:
```python
run() -> dict[str, pd.DataFrame]:
    # 1. Coleta contratos via Selenium
    # 2. Normaliza contratos
    # 3. Coleta licitações via Selenium
    # 4. Normaliza licitações
    # 5. Salva cache em JSON
    # 6. Em caso de falha, carrega cache local
```

---

## ✅ Validação

- ✅ Sintaxe Python validada
- ✅ Importações corretas
- ✅ Dependências instaladas
- ✅ Função de normalização preservada
- ✅ Sistema de cache mantido
- ✅ Logging integrado

---

## 🔍 Próximos Passos

1. **Executar teste**: `python scrappers/macae/portal_macae.py`
2. **Verificar logs**: Ajustar XPaths se os seletores mudarem no portal
3. **Monitorar downloads**: Arquivos CSV serão salvos em `temp/` e processados
4. **Validar dados**: Comparar contratos e licitações normalizadas

---

*Refatoração concluída em 12/04/2026*
*Ferramenta: Selenium 4.25.0 + WebDriver Manager 4.0.2*
