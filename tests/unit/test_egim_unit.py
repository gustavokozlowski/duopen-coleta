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
