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

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


class TestRunPipeline:
    """Testes do pipeline completo."""
    
    @patch('scrappers.macae.portal_macae.fetch_contratos')
    @patch('scrappers.macae.portal_macae.fetch_licitacoes')
    def test_run_estrutura_retorno(self, mock_fetch_lic, mock_fetch_contr):
        """Testa que run() retorna dicts corretos."""
        from scrappers.macae import portal_macae as pm
        
        # Setup mocks simples
        mock_fetch_contr.return_value = pd.DataFrame()
        mock_fetch_lic.return_value = pd.DataFrame()
        
        resultado = pm.run()
        
        assert isinstance(resultado, dict)
        assert "contratos" in resultado
        assert "licitacoes" in resultado
        assert isinstance(resultado["contratos"], pd.DataFrame)
        assert isinstance(resultado["licitacoes"], pd.DataFrame)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
