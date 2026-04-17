import pytest
import requests

from scrappers.tce import tce_rj

pytestmark = pytest.mark.integration


def test_integracao_api_real_contratos_macae_controlada(monkeypatch):
    """Valida chamada real ao endpoint de contratos com parametros controlados."""
    monkeypatch.setattr(tce_rj, "RETRY_ATTEMPTS", 1)
    monkeypatch.setattr(tce_rj, "REQUEST_TIMEOUT", 20)

    params = {
        "municipio": tce_rj.MUNICIPIO,
        "inicio": 0,
        "limite": 3,
        "jsonfull": False,
    }

    try:
        payload = tce_rj._get("contratos_municipio", params)
    except (requests.exceptions.RequestException, RuntimeError) as exc:
        pytest.skip(f"API TCE-RJ indisponivel no ambiente de teste: {exc}")

    registros = tce_rj._extract_records(payload, preferred_keys=("Contratos",))

    assert isinstance(registros, list)
    assert len(registros) > 0
    assert len(registros) <= 3

    primeiro = registros[0]
    assert isinstance(primeiro, dict)
    assert tce_rj._municipio_match(primeiro.get("Ente")) is True
    assert "NumeroContrato" in primeiro
    assert "Objeto" in primeiro
