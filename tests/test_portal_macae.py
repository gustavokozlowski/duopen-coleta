"""
tests/test_portal_macae.py
Testes unitários para o scraper do Portal de Transparência de Macaé.

Cobre:
    - Funções de normalização (contratos e licitações)
    - Leitura de CSV com múltiplos encodings
    - Configuração do módulo
    - Utilitários de parsing
"""

import json
import pytest
import pandas as pd
import io
from datetime import datetime, timezone
from pathlib import Path

# Importar o módulo a testar
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestConfiguração:
    """Testes de configuração do módulo."""
    
    def test_importacao_modulo(self):
        """Verifica se o módulo pode ser importado sem erros."""
        from scrappers.macae import portal_macae
        assert portal_macae is not None
    
    def test_constantes_carregadas(self):
        """Valida constantes de configuração."""
        from scrappers.macae import portal_macae as pm
        
        assert pm.BASE_URL == "https://transparencia.macae.rj.gov.br"
        assert pm.TIPO_CONTRATO_OBRAS == "Obras e Serviços de Engenharia"
        assert isinstance(pm.KEYWORDS_OBRAS, list)
        assert len(pm.KEYWORDS_OBRAS) > 0
        assert "obra" in pm.KEYWORDS_OBRAS
    
    def test_timeout_selenium(self):
        """Verifica timeout do WebDriver."""
        from scrappers.macae import portal_macae as pm
        assert pm.WAIT_TIMEOUT == 20
    
    def test_palavras_chave_obras(self):
        """Valida lista de palavras-chave para licitações."""
        from scrappers.macae import portal_macae as pm
        
        keywords = pm.KEYWORDS_OBRAS
        assert "construção" in keywords
        assert "pavimentação" in keywords
        assert "infraestrutura" in keywords


class TestLeituraCsv:
    """Testes da função de leitura de CSV."""
    
    def test_ler_csv_utf8_simples(self):
        """Testa leitura de CSV UTF-8 com separador semicolon."""
        from scrappers.macae import portal_macae as pm
        
        csv_content = b"numero;objeto;valor\n001;Construcao;1000.00\n002;Reforma;2000.00"
        df = pm._ler_csv(csv_content)
        
        assert not df.empty
        assert len(df) == 2
        assert list(df.columns) == ["numero", "objeto", "valor"]
    
    def test_ler_csv_latin1(self):
        """Testa leitura de CSV com encoding latin-1."""
        from scrappers.macae import portal_macae as pm
        
        # CSV com caracteres especiais (é, ç, ã)
        csv_content = "número;descrição\n001;Pavimentação\n002;Drenagem".encode("latin-1")
        df = pm._ler_csv(csv_content)
        
        assert not df.empty
        assert len(df) == 2
    
    def test_ler_csv_virgula_separador(self):
        """Testa leitura com separador vírgula (CSV padrão)."""
        from scrappers.macae import portal_macae as pm
        
        csv_content = b"id,nome,valor\n1,Obra A,5000\n2,Obra B,7000"
        df = pm._ler_csv(csv_content)
        
        assert not df.empty
        assert len(df) == 2
        assert "id" in df.columns
    
    def test_ler_csv_invalido_raise_error(self):
        """Testa que CSV inválido lança exceção."""
        from scrappers.macae import portal_macae as pm
        
        # Conteúdo sem estrutura válida
        csv_content = b"%%%@@@###$$$"
        
        with pytest.raises(ValueError, match="Não foi possível ler o CSV"):
            pm._ler_csv(csv_content)
    
    def test_ler_csv_vazio(self):
        """Testa leitura de CSV vazio."""
        from scrappers.macae import portal_macae as pm
        
        csv_content = b""
        
        with pytest.raises(ValueError):
            pm._ler_csv(csv_content)


class TestNormalizadorContratos:
    """Testes da função de normalização de contratos."""
    
    def criar_df_bruto_contratos(self):
        """Helper para criar DataFrame bruto de teste."""
        return pd.DataFrame({
            "Número": ["001", "002"],
            "Objeto": ["Pavimentação de rua", "Construção de praça"],
            "Valor": ["R$ 10.000,00", "R$ 25.000,50"],
            "Empresa": ["Empresa A", "Empresa B"],
            "CNPJ": ["12.345.678/0001-90", "98.765.432/0001-10"],
            "Data Assinatura": ["01/01/2024", "15/02/2024"],
            "Vigência Fim": ["31/12/2024", "30/06/2024"],
            "Secretaria": ["Obras", "Infraestrutura"],
            "Modalidade Licitação": ["Concorrência", "Tomada de Preço"],
            "Número Licitação": ["LIC-001", "LIC-002"],
            "Situação": ["Vigente", "Vigente"],
            "Aditivo": ["Não", "Sim"],
        })
    
    def test_normalizar_contratos_basico(self):
        """Testa normalização básica de contratos."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = self.criar_df_bruto_contratos()
        df_norm = pm.normalizar_contratos(df_bruto)
        
        assert not df_norm.empty
        assert len(df_norm) == 2
        assert "id_contrato" in df_norm.columns
        assert "objeto" in df_norm.columns
        assert "valor" in df_norm.columns
    
    def test_normalizar_contratos_valor_float(self):
        """Verifica conversão de valores para float."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = self.criar_df_bruto_contratos()
        df_norm = pm.normalizar_contratos(df_bruto)
        
        assert df_norm["valor"].dtype in [float, "float64"]
        assert df_norm["valor"].iloc[0] == 10000.0
        assert df_norm["valor"].iloc[1] == 25000.5
    
    def test_normalizar_contratos_data_iso(self):
        """Verifica conversão de datas para ISO format."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = self.criar_df_bruto_contratos()
        df_norm = pm.normalizar_contratos(df_bruto)
        
        assert df_norm["data_assinatura"].iloc[0] == "2024-01-01T00:00:00+00:00"
        # Validar que é uma string ISO válida
        datetime.fromisoformat(df_norm["data_assinatura"].iloc[0])
    
    def test_normalizar_contratos_campos_obrigatorios(self):
        """Verifica presença de campos obrigatórios."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = self.criar_df_bruto_contratos()
        df_norm = pm.normalizar_contratos(df_bruto)
        
        campos_obrigatorios = [
            "id_contrato", "objeto", "valor", "fornecedor", "cnpj_fornecedor",
            "data_assinatura", "data_vigencia_fim", "secretaria",
            "modalidade_licitacao", "tipo_contrato", "fonte", "coletado_em",
            "payload_bruto"
        ]
        
        for campo in campos_obrigatorios:
            assert campo in df_norm.columns, f"Campo obrigatório ausente: {campo}"
    
    def test_normalizar_contratos_tipo_contrato_fixo(self):
        """Verifica se tipo de contrato é sempre "Obras e Serviços de Engenharia"."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = self.criar_df_bruto_contratos()
        df_norm = pm.normalizar_contratos(df_bruto)
        
        assert (df_norm["tipo_contrato"] == "Obras e Serviços de Engenharia").all()
    
    def test_normalizar_contratos_fonte_correcta(self):
        """Verifica se fonte é corretamente definida."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = self.criar_df_bruto_contratos()
        df_norm = pm.normalizar_contratos(df_bruto)
        
        assert (df_norm["fonte"] == "portal_transparencia_macae_contratos").all()
    
    def test_normalizar_contratos_vazio(self):
        """Testa normalização de DataFrame vazio."""
        from scrappers.macae import portal_macae as pm
        
        df_vazio = pd.DataFrame()
        df_norm = pm.normalizar_contratos(df_vazio)
        
        assert df_norm.empty
    
    def test_normalizar_contratos_colunas_ausentes(self):
        """Testa normalização quando algumas colunas estão ausentes."""
        from scrappers.macae import portal_macae as pm
        
        # DataFrame com poucas colunas
        df_incompleto = pd.DataFrame({
            "Número": ["001"],
            "Objeto": ["Obra"],
        })
        
        df_norm = pm.normalizar_contratos(df_incompleto)
        
        assert not df_norm.empty
        assert len(df_norm) == 1
        # Campos ausentes devem ser None
        assert pd.isna(df_norm["valor"].iloc[0])


class TestNormalizadorLicitacoes:
    """Testes da função de normalização de licitações."""
    
    def criar_df_bruto_licitacoes(self):
        """Helper para criar DataFrame bruto de teste."""
        return pd.DataFrame({
            "Número": ["LIC-2024-001", "LIC-2024-002"],
            "Objeto": ["Pavimentação", "Ampliação de escola"],
            "Modalidade": ["Concorrência", "Tomada de Preço"],
            "Status": ["Publicada", "Em Andamento"],
            "Valor Estimado": ["R$ 50.000,00", "R$ 100.000,00"],
            "Data Abertura": ["10/03/2024", "20/03/2024"],
            "Data Publicação": ["01/03/2024", "15/03/2024"],
            "Secretaria": ["Obras", "Educação"],
            "Ano": ["2024", "2024"],
        })
    
    def test_normalizar_licitacoes_basico(self):
        """Testa normalização básica de licitações."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = self.criar_df_bruto_licitacoes()
        df_norm = pm.normalizar_licitacoes(df_bruto)
        
        assert not df_norm.empty
        assert len(df_norm) == 2
        assert "id_licitacao" in df_norm.columns
        assert "objeto" in df_norm.columns
    
    def test_normalizar_licitacoes_valor_estimado(self):
        """Verifica conversão de valor estimado."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = self.criar_df_bruto_licitacoes()
        df_norm = pm.normalizar_licitacoes(df_bruto)
        
        assert df_norm["valor_estimado"].dtype in [float, "float64"]
        assert df_norm["valor_estimado"].iloc[0] == 50000.0
    
    def test_normalizar_licitacoes_datas(self):
        """Verifica conversão de datas em licitações."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = self.criar_df_bruto_licitacoes()
        df_norm = pm.normalizar_licitacoes(df_bruto)
        
        # Validar ISO format
        assert datetime.fromisoformat(df_norm["data_abertura"].iloc[0])
    
    def test_normalizar_licitacoes_campo_ano(self):
        """Testa se campo de ano é preservado."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = self.criar_df_bruto_licitacoes()
        df_norm = pm.normalizar_licitacoes(df_bruto)
        
        assert "ano" in df_norm.columns
        assert df_norm["ano"].iloc[0] == "2024"
    
    def test_normalizar_licitacoes_fonte_correcta(self):
        """Verifica se fonte é corretamente definida para licitações."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = self.criar_df_bruto_licitacoes()
        df_norm = pm.normalizar_licitacoes(df_bruto)
        
        assert (df_norm["fonte"] == "portal_transparencia_macae_licitacoes").all()


class TestUtilitariosColunas:
    """Testes das funções utilitárias de busca de colunas."""
    
    def test_col_encontra_coluna_exata(self):
        """Testa busca de coluna com match exato."""
        from scrappers.macae import portal_macae as pm
        
        df = pd.DataFrame({
            "número": [1, 2],
            "objeto": ["A", "B"],
        })
        
        col = pm._col(df, "número")
        assert col == "número"
    
    def test_col_encontra_primeira_candidata(self):
        """Testa busca retorna primeira coluna candidata."""
        from scrappers.macae import portal_macae as pm
        
        df = pd.DataFrame({
            "Número": [1, 2],
            "número": [3, 4],
        })
        
        col = pm._col(df, "número", "numero")
        assert col in ["Número", "número"]
    
    def test_col_retorna_none(self):
        """Testa busca retorna None quando nenhuma coluna encontrada."""
        from scrappers.macae import portal_macae as pm
        
        df = pd.DataFrame({
            "foo": [1, 2],
            "bar": [3, 4],
        })
        
        col = pm._col(df, "inexistente")
        assert col is None
    
    def test_val_extrai_valor(self):
        """Testa extração de valor de célula."""
        from scrappers.macae import portal_macae as pm
        
        df = pd.DataFrame({
            "col1": ["valor  ", "outro"],
        })
        
        row = df.iloc[0]
        val = pm._val(row, "col1")
        assert val == "valor"  # stripped
    
    def test_val_coluna_ausente(self):
        """Testa retorno None quando coluna ausente."""
        from scrappers.macae import portal_macae as pm
        
        df = pd.DataFrame({"col1": ["abc"]})
        row = df.iloc[0]
        
        val = pm._val(row, "inexistente")
        assert val is None
    
    def test_float_converte_valor_brasileir(self):
        """Testa conversão de valor em formato brasileiro."""
        from scrappers.macae import portal_macae as pm
        
        valor = pm._float("R$ 10.000,50")
        assert valor == 10000.5
    
    def test_float_retorna_none(self):
        """Testa que float invalid retorna None."""
        from scrappers.macae import portal_macae as pm
        
        valor = pm._float("abc")
        assert valor is None
    
    def test_data_converte_formato_br(self):
        """Testa conversão de data em formato DD/MM/YYYY."""
        from scrappers.macae import portal_macae as pm
        
        data_str = pm._data("01/01/2024")
        assert data_str == "2024-01-01T00:00:00+00:00"
    
    def test_data_retorna_original_invalida(self):
        """Testa que data inválida é retornada como está."""
        from scrappers.macae import portal_macae as pm
        
        data_str = pm._data("xyz")
        assert data_str == "xyz"


class TestPayloadBruto:
    """Testes de armazenamento de payload bruto."""
    
    def test_payload_bruto_json_valido(self):
        """Verifica se payload_bruto é JSON válido."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = pd.DataFrame({
            "Número": ["001"],
            "Objeto": ["Obra"],
        })
        
        df_norm = pm.normalizar_contratos(df_bruto)
        payload = df_norm["payload_bruto"].iloc[0]
        
        # Deve ser JSON válido
        parsed = json.loads(payload)
        assert "Número" in parsed
        assert parsed["Número"] == "001"


class TestColetadoEm:
    """Testes do timestamp de coleta."""
    
    def test_coletado_em_iso_format(self):
        """Verifica se coletado_em está em formato ISO."""
        from scrappers.macae import portal_macae as pm
        
        df_bruto = pd.DataFrame({
            "Número": ["001"],
            "Objeto": ["Obra"],
        })
        
        df_norm = pm.normalizar_contratos(df_bruto)
        coletado = df_norm["coletado_em"].iloc[0]
        
        # Deve ser ISO válido
        dt = datetime.fromisoformat(coletado)
        assert dt.tzinfo is not None  # Com timezone
    
    def test_coletado_em_recente(self):
        """Verifica se timestamp é recente."""
        from scrappers.macae import portal_macae as pm
        
        agora = datetime.now(timezone.utc)
        
        df_bruto = pd.DataFrame({
            "Número": ["001"],
            "Objeto": ["Obra"],
        })
        
        df_norm = pm.normalizar_contratos(df_bruto)
        coletado = datetime.fromisoformat(df_norm["coletado_em"].iloc[0])
        
        # Deve ser dentro dos últimos 10 segundos
        diff = agora - coletado
        assert diff.total_seconds() < 10


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
