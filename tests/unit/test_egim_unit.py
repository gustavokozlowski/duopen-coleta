import pytest
import xml.etree.ElementTree as ET
from scrappers.macae import egim

# Exemplo de placemark KML para testar parsing de status
KML_PLACEMARK_TEMPLATE = '''
<Placemark>
  <name>Obra Teste</name>
  <description><![CDATA[Status: Em execução\nSecretaria: Obras]]></description>
  <styleUrl>#icon-blue</styleUrl>
  <Point>
    <coordinates>-42.0,-22.0,0</coordinates>
  </Point>
</Placemark>
'''

def test_inferir_status_por_icone_e_descricao():
    placemark = ET.fromstring(KML_PLACEMARK_TEMPLATE)
    # Simula status por ícone (azul)
    status = egim._inferir_status_por_icone(placemark)
    assert status == "em andamento" or status == "indefinido"
    # Simula status por descrição
    campos = egim._extrair_campos_descricao(egim._tag(placemark, "description"))
    status_desc = egim._inferir_status_por_descricao(campos)
    assert status_desc == "em andamento"

def test_inferir_status_por_descricao_variados():
    exemplos = [
        ("Status: Concluída", "concluída"),
        ("Status: Em execução", "em andamento"),
        ("Status: Paralisada", "paralisada"),
        ("Status: Planejada", "planejada"),
        ("Status: indefinido", "indefinido"),
    ]
    for desc, esperado in exemplos:
        campos = egim._extrair_campos_descricao(desc)
        status = egim._inferir_status_por_descricao(campos)
        assert status == esperado

def test_inferir_status_campo_obra_prioridade():
    # Campo "obra" tem prioridade máxima
    exemplos = [
        ({"obra": "CONCLUÍDA"}, "concluída"),
        ({"obra": "EM ANDAMENTO"}, "em andamento"),
        ({"obra": "PARALISADA"}, "paralisada"),
        ({"obra": "PLANEJADA"}, "planejada"),
        # Mesmo com status genérico diferente, "obra" prevalece
        ({"status": "Indefinido", "obra": "EM ANDAMENTO"}, "em andamento"),
    ]
    for campos, esperado in exemplos:
        status = egim._inferir_status_por_descricao(campos)
        assert status == esperado, f"Campos {campos}: esperado {esperado}, obtido {status}"

def test_parsear_kml_status_prioridade():
    # Status por ícone (indefinido), por camada (em andamento), por descrição (concluída)
    placemark = KML_PLACEMARK_TEMPLATE.replace("Status: Em execução", "Status: Concluída")
    kml = f'''<kml xmlns="http://www.opengis.net/kml/2.2"><Document><Folder><name>Obras em andamento</name>{placemark}</Folder></Document></kml>'''
    placemarks = egim.parsear_kml(kml.encode("utf-8"))
    # O status final deve ser "concluída" (prioridade: descrição > ícone > camada)
    assert placemarks and placemarks[0]["status_icone"] == "concluída"


# ── Novos testes: mapeamento corrigido/expandido ──────────────────────────────

import pandas as pd


def _placemark_egim(extras: dict) -> dict:
    """Monta um placemark no formato que normalizar() recebe."""
    return {
        "nome": "Obra Teste",
        "camada": "Projetos e Obras Monitoradas pelo EGIM",
        "descricao_raw": "",
        "descricao": "",
        "latitude": -22.37,
        "longitude": -41.78,
        "status_icone": "concluída",
        "campos_extras": extras,
        "coords_kml": "-41.78,-22.37,0",
    }


def test_normalizar_previsao_termino_captura_fim():
    """Bug fix: previsao_termino deve capturar campo 'fim' (era 0/36)."""
    p = _placemark_egim({"fim": "Março/2023", "valor da obra": "R$ 100"})
    df = egim.normalizar([p])
    assert df.iloc[0]["previsao_termino"] == "Março/2023"


def test_normalizar_previsao_termino_prazo_em_dias():
    """previsao_termino deve aceitar '360 DIAS' como string."""
    p = _placemark_egim({"fim": "360 DIAS", "valor da obra": "R$ 100"})
    df = egim.normalizar([p])
    assert df.iloc[0]["previsao_termino"] == "360 DIAS"


def test_normalizar_data_inicio_mes_ano():
    """data_inicio deve converter 'Abril/2022' para ISO 8601."""
    p = _placemark_egim({"início": "Abril/2022", "valor da obra": "R$ 100"})
    df = egim.normalizar([p])
    assert df.iloc[0]["data_inicio"] == "2022-04-01T00:00:00+00:00"


def test_normalizar_data_inicio_sem_barra_retorna_none():
    """'2019' sem '/' não é data completa — deve retornar None."""
    p = _placemark_egim({"início": "2019", "valor da obra": "R$ 100"})
    df = egim.normalizar([p])
    assert df.iloc[0]["data_inicio"] is None


def test_normalizar_data_inicio_ausente_retorna_none():
    """Obras sem 'início' em campos_extras devem ter data_inicio None."""
    p = _placemark_egim({"valor da obra": "R$ 100"})
    df = egim.normalizar([p])
    assert df.iloc[0]["data_inicio"] is None


def test_normalizar_setor_administrativo():
    """setor_administrativo deve ser extraído de 'setor administrativo'."""
    p = _placemark_egim({"setor administrativo": "SETOR VERDE", "valor da obra": "R$ 100"})
    df = egim.normalizar([p])
    assert df.iloc[0]["setor_administrativo"] == "SETOR VERDE"


def test_normalizar_objectid():
    """objectid deve ser extraído de campos_extras."""
    p = _placemark_egim({"objectid": "5", "valor da obra": "R$ 100"})
    df = egim.normalizar([p])
    assert df.iloc[0]["objectid"] == "5"


def test_data_mes_ano_todos_meses():
    """_data_mes_ano deve parsear todos os meses em português."""
    casos = [
        ("Janeiro/2021", "2021-01-01T00:00:00+00:00"),
        ("Fevereiro/2022", "2022-02-01T00:00:00+00:00"),
        ("Março/2022", "2022-03-01T00:00:00+00:00"),
        ("Setembro/2021", "2021-09-01T00:00:00+00:00"),
        ("Dezembro/2023", "2023-12-01T00:00:00+00:00"),
    ]
    for entrada, esperado in casos:
        assert egim._data_mes_ano(entrada) == esperado, f"Falhou para: {entrada}"


def test_data_mes_ano_invalidos():
    """_data_mes_ano deve retornar None para entradas inválidas."""
    assert egim._data_mes_ano(None) is None
    assert egim._data_mes_ano("") is None
    assert egim._data_mes_ano("2019") is None
    assert egim._data_mes_ano("360 DIAS") is None
    assert egim._data_mes_ano("-") is None


# ── Testes de _calcular_percentual ────────────────────────────────────────────

def test_calcular_percentual_concluida_retorna_100():
    """Obra CONCLUÍDA deve retornar 100 independente das datas."""
    assert egim._calcular_percentual({"obra": "CONCLUÍDA"}) == 100.0
    assert egim._calcular_percentual({"obra": "CONCLUÍDA", "início": "Abril/2022"}) == 100.0


def test_calcular_percentual_sem_inicio_retorna_none():
    """Obra EM ANDAMENTO sem início não pode ser estimada."""
    assert egim._calcular_percentual({"obra": "EM ANDAMENTO", "fim": "Dezembro/2023"}) is None


def test_calcular_percentual_sem_obra_retorna_none():
    """Sem campo obra não é possível aplicar nenhuma regra."""
    assert egim._calcular_percentual({}) is None
    assert egim._calcular_percentual({"início": "Abril/2022", "fim": "Abril/2023"}) is None


def test_calcular_percentual_em_andamento_datas_futuras():
    """Obra EM ANDAMENTO com fim no futuro deve retornar valor entre 0 e 99."""
    from datetime import datetime, timezone, timedelta
    hoje = datetime.now(timezone.utc)
    # início 6 meses atrás, fim 6 meses à frente
    mes_inicio = (hoje.replace(day=1) - timedelta(days=180))
    mes_fim = (hoje.replace(day=1) + timedelta(days=180))
    def fmt(dt): return f"{['janeiro','fevereiro','março','abril','maio','junho','julho','agosto','setembro','outubro','novembro','dezembro'][dt.month-1].capitalize()}/{dt.year}"
    extras = {"obra": "EM ANDAMENTO", "início": fmt(mes_inicio), "fim": fmt(mes_fim)}
    pct = egim._calcular_percentual(extras)
    assert pct is not None
    assert 0 < pct < 99


def test_calcular_percentual_em_andamento_prazo_dias():
    """Obra EM ANDAMENTO com fim em '360 DIAS' deve estimar percentual."""
    extras = {"obra": "EM ANDAMENTO", "início": "Julho/2023", "fim": "360 DIAS"}
    pct = egim._calcular_percentual(extras)
    # início jul/2023, prazo 360 dias → fim jun/2024, hoje > fim → limitado a 99
    assert pct == 99.0


def test_calcular_percentual_limitado_a_99_em_andamento():
    """Obra EM ANDAMENTO nunca deve retornar 100 mesmo com prazo expirado."""
    extras = {"obra": "EM ANDAMENTO", "início": "Janeiro/2020", "fim": "Janeiro/2021"}
    pct = egim._calcular_percentual(extras)
    assert pct == 99.0


def test_normalizar_percentual_preenchido():
    """normalizar() deve preencher percentual via regras derivadas."""
    p_conc = _placemark_egim({"obra": "CONCLUÍDA", "valor da obra": "R$ 100"})
    p_and = _placemark_egim({"obra": "EM ANDAMENTO", "início": "Janeiro/2020", "fim": "Janeiro/2021", "valor da obra": "R$ 100"})
    p_sem = _placemark_egim({"obra": "EM ANDAMENTO", "valor da obra": "R$ 100"})

    df = egim.normalizar([p_conc, p_and, p_sem])
    assert df.iloc[0]["percentual"] == 100.0
    assert df.iloc[1]["percentual"] == 99.0
    assert pd.isna(df.iloc[2]["percentual"])  # EGIM não passa pelo transformer, None permanece no raw
