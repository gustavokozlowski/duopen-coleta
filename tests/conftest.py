"""Configuração compartilhada de testes.

Desabilita o geocoding (Nominatim) por padrão nos testes para que nenhum teste
unitário bata na rede. Testes específicos de geocoding mockam a camada de rede
e controlam `geocoding.ENABLED` por conta própria.
"""

import os

# Setado antes de qualquer import de etl.geocoding (que lê ENABLED no import).
os.environ.setdefault("GEOCODING_ENABLED", "false")
