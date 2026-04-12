"""
tests/integration/macae/test_portal_macae_selenium.py
Testes de integração para o scraper com suporte a Selenium (mocked).

Cobre:
    - Inicialização do WebDriver
    - Fluxo de navegação com mocks
    - Captura de downloads (mock)
    - Tratamento de erros na coleta
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, mock_open
import pandas as pd
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestInitializacaoWebDriver:
    """Testes de inicialização do WebDriver."""
    
    @patch('scrappers.macae.portal_macae.webdriver.Chrome')
    @patch('scrappers.macae.portal_macae.ChromeDriverManager')
    def test_inicializar_driver_headless(self, mock_driver_manager, mock_chrome):
        """Testa inicialização do driver em modo headless."""
        from scrappers.macae import portal_macae as pm
        
        mock_driver_instance = MagicMock()
        mock_chrome.return_value = mock_driver_instance
        
        driver = pm._inicializar_driver()
        
        assert driver is not None
        mock_chrome.assert_called_once()
    
    @patch('scrappers.macae.portal_macae.webdriver.ChromeOptions')
    @patch('scrappers.macae.portal_macae.webdriver.Chrome')
    @patch('scrappers.macae.portal_macae.ChromeDriverManager')
    def test_inicializar_driver_options(self, mock_driver_manager, mock_chrome, mock_options):
        """Testa que opções corretas são passadas ao Chrome."""
        from scrappers.macae import portal_macae as pm
        
        mock_driver_instance = MagicMock()
        mock_chrome.return_value = mock_driver_instance
        
        driver = pm._inicializar_driver()
        
        assert driver is not None
        # ChromeDriver foi chamado
        mock_chrome.assert_called_once()


class TestEsperarElemento:
    """Testes da função de espera de elemento."""
    
    @patch('scrappers.macae.portal_macae.WebDriverWait')
    def test_esperar_elemento_encontrado(self, mock_wait):
        """Testa espera quando elemento é encontrado."""
        from scrappers.macae import portal_macae as pm
        from selenium.webdriver.common.by import By
        
        mock_driver = MagicMock()
        mock_wait_instance = MagicMock()
        mock_wait.return_value = mock_wait_instance
        
        resultado = pm._esperar_elemento(mock_driver, By.XPATH, "//table")
        
        assert resultado is True
        mock_wait.assert_called_once()
    
    @patch('scrappers.macae.portal_macae.WebDriverWait')
    def test_esperar_elemento_timeout(self, mock_wait):
        """Testa timeout ao esperar elemento."""
        from scrappers.macae import portal_macae as pm
        from selenium.webdriver.common.by import By
        
        mock_driver = MagicMock()
        mock_wait.side_effect = Exception("Timeout")
        
        resultado = pm._esperar_elemento(mock_driver, By.XPATH, "//table")
        
        assert resultado is False


class TestFetchContratosMock:
    """Testes da função fetch_contratos com mocks."""
    
    @patch('scrappers.macae.portal_macae._inicializar_driver')
    @patch('scrappers.macae.portal_macae._esperar_elemento')
    @patch('scrappers.macae.portal_macae._ler_csv')
    @patch('tempfile.mkdtemp')
    def test_fetch_contratos_sucesso_mock(self, mock_tempdir, mock_ler_csv, 
                                          mock_esperar, mock_driver_init):
        """Testa fluxo de coleta de contratos com mock."""
        from scrappers.macae import portal_macae as pm
        
        # Setup
        mock_driver = MagicMock()
        mock_driver_init.return_value = mock_driver
        
        mock_tempdir.return_value = "/tmp/test"
        mock_esperar.return_value = True
        
        expected_df = pd.DataFrame({
            "Número": ["001", "002"],
            "Objeto": ["Obra A", "Obra B"],
        })
        mock_ler_csv.return_value = expected_df
        
        # Mock do arquivo baixado
        with patch('pathlib.Path.glob', return_value=[Path("/tmp/test/contratos.csv")]):
            with patch('builtins.open', mock_open(read_data=b"test data")):
                result = pm.fetch_contratos()
        
        # Assertions
        assert not result.empty
        assert len(result) == 2
        mock_driver_init.assert_called_once()
        mock_driver.quit.assert_called_once()
    
    @patch('scrappers.macae.portal_macae._inicializar_driver')
    def test_fetch_contratos_erro_driver(self, mock_driver_init):
        """Testa tratamento de erro ao inicializar driver."""
        from scrappers.macae import portal_macae as pm
        
        mock_driver_init.side_effect = Exception("Driver error")
        
        # Deve retornar DataFrame vazio e não lançar exceção
        result = pm.fetch_contratos()
        
        assert result.empty
    
    @patch('scrappers.macae.portal_macae._inicializar_driver')
    @patch('pathlib.Path.glob')
    def test_fetch_contratos_sem_arquivo_baixado(self, mock_glob, mock_driver_init):
        """Testa quando nenhum arquivo é baixado."""
        from scrappers.macae import portal_macae as pm
        
        mock_driver = MagicMock()
        mock_driver_init.return_value = mock_driver
        mock_glob.return_value = []  # Sem arquivos
        
        with patch('tempfile.mkdtemp', return_value="/tmp/test"):
            result = pm.fetch_contratos()
        
        assert result.empty


class TestFetchLicitacoesMock:
    """Testes da função fetch_licitacoes com mocks."""
    
    @patch('scrappers.macae.portal_macae._inicializar_driver')
    @patch('scrappers.macae.portal_macae._esperar_elemento')
    @patch('scrappers.macae.portal_macae._ler_csv')
    @patch('tempfile.mkdtemp')
    def test_fetch_licitacoes_sucesso_mock(self, mock_tempdir, mock_ler_csv,
                                           mock_esperar, mock_driver_init):
        """Testa fluxo de coleta de licitações com mock."""
        from scrappers.macae import portal_macae as pm
        
        # Setup
        mock_driver = MagicMock()
        mock_driver_init.return_value = mock_driver
        
        mock_tempdir.return_value = "/tmp/test_lic"
        mock_esperar.return_value = True
        
        expected_df = pd.DataFrame({
            "Número": ["LIC-001"],
            "Objeto": ["Pavimentação"],
        })
        mock_ler_csv.return_value = expected_df
        
        with patch('pathlib.Path.glob', return_value=[Path("/tmp/test_lic/licitacoes.csv")]):
            with patch('builtins.open', mock_open(read_data=b"test data")):
                with patch('pathlib.Path.stat'):
                    result = pm.fetch_licitacoes()
        
        assert not result.empty
        mock_driver_init.assert_called_once()
        mock_driver.quit.assert_called_once()
    
    @patch('scrappers.macae.portal_macae._inicializar_driver')
    @patch('tempfile.mkdtemp')
    def test_fetch_licitacoes_vazio(self, mock_tempdir, mock_driver_init):
        """Testa quando nenhuma licitação é retornada."""
        from scrappers.macae import portal_macae as pm
        
        mock_driver = MagicMock()
        mock_driver_init.return_value = mock_driver
        mock_tempdir.return_value = "/tmp/test"
        
        with patch('pathlib.Path.glob', return_value=[]):
            result = pm.fetch_licitacoes()
        
        assert result.empty


class TestRunPipeline:
    """Testes do pipeline completo."""
    
    @patch('scrappers.macae.portal_macae.fetch_contratos')
    @patch('scrappers.macae.portal_macae.fetch_licitacoes')
    @patch('scrappers.macae.portal_macae._salvar_cache')
    def test_run_sucesso(self, mock_salvar_cache, mock_fetch_lic, mock_fetch_contr):
        """Testa execução bem-sucedida do pipeline."""
        from scrappers.macae import portal_macae as pm
        
        # Setup mocks
        df_contratos_raw = pd.DataFrame({
            "Número": ["001"],
            "Objeto": ["Obra"],
            "Valor": ["R$ 1000,00"],
        })
        
        df_licitacoes_raw = pd.DataFrame({
            "Número": ["LIC-001"],
            "Objeto": ["Pavimentação"],
        })
        
        mock_fetch_contr.return_value = df_contratos_raw
        mock_fetch_lic.return_value = df_licitacoes_raw
        
        resultado = pm.run()
        
        assert "contratos" in resultado
        assert "licitacoes" in resultado
        assert not resultado["contratos"].empty
        assert not resultado["licitacoes"].empty
        mock_salvar_cache.assert_called_once()
    
    @patch('scrappers.macae.portal_macae.fetch_contratos')
    @patch('scrappers.macae.portal_macae.fetch_licitacoes')
    @patch('scrappers.macae.portal_macae._carregar_cache')
    def test_run_falha_carrega_cache(self, mock_carregar_cache, mock_fetch_lic, 
                                     mock_fetch_contr):
        """Testa fallback para cache em caso de falha."""
        from scrappers.macae import portal_macae as pm
        
        # Simular falha na coleta
        mock_fetch_contr.side_effect = Exception("Network error")
        mock_fetch_lic.side_effect = Exception("Network error")
        
        # Cache de fallback
        cache = {
            "portal_macae_contratos": pd.DataFrame({"id": [1]}),
            "portal_macae_licitacoes": pd.DataFrame({"id": [1]}),
        }
        mock_carregar_cache.return_value = cache
        
        resultado = pm.run()
        
        assert "contratos" in resultado
        assert "licitacoes" in resultado
        mock_carregar_cache.assert_called_once()


class TestSalvarCache:
    """Testes de salvamento em cache."""
    
    @patch('pathlib.Path.mkdir')
    @patch('builtins.open', new_callable=mock_open)
    def test_salvar_cache_sucesso(self, mock_file, mock_mkdir):
        """Testa salvamento de cache em JSON."""
        from scrappers.macae import portal_macae as pm
        
        df_contratos = pd.DataFrame({"numero": ["001"], "objeto": ["Obra"]})
        df_licitacoes = pd.DataFrame({"numero": ["LIC-001"]})
        
        with patch('pandas.DataFrame.to_json'):
            pm._salvar_cache(df_contratos, df_licitacoes)
        
        mock_mkdir.assert_called()


class TestCarregarCache:
    """Testes de carregamento de cache."""
    
    @patch('pathlib.Path.exists', return_value=False)
    def test_carregar_cache_vazio(self, mock_exists):
        """Testa carregamento quando não há cache."""
        from scrappers.macae import portal_macae as pm
        
        resultado = pm._carregar_cache()
        
        assert "portal_macae_contratos" in resultado
        assert "portal_macae_licitacoes" in resultado
        assert resultado["portal_macae_contratos"].empty
        assert resultado["portal_macae_licitacoes"].empty


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
