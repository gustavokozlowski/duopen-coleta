import pytest

from scrappers.federal import transparencia

pytestmark = pytest.mark.integration


class DummyResponse:
    def __init__(self, status_code=200, payload=None, http_error=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self._http_error = http_error

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self._http_error is not None:
            raise self._http_error


@pytest.fixture(autouse=True)
def isolated_cache_dir(tmp_path, monkeypatch):
    """Isola e limpa o cache ao fim de cada teste de integração."""
    monkeypatch.setattr(transparencia, "CACHE_DIR", tmp_path)
    yield
    for file_path in tmp_path.glob("*.json"):
        file_path.unlink(missing_ok=True)


# Integração: cache em disco + pipeline run() sem rede real

def test_integracao_salvar_e_carregar_cache_em_disco(tmp_path, monkeypatch):
    dados = [{"id": "A1", "objeto": "Obra de teste"}]
    transparencia._salvar_cache("transparencia_contratos", dados)

    carregado = transparencia._carregar_cache("transparencia_contratos")

    assert carregado == dados


def test_integracao_carregar_cache_invalido_retorna_lista_vazia(tmp_path, monkeypatch):
    path = tmp_path / "transparencia_contratos.json"
    path.write_text('{"invalido": true}', encoding="utf-8")

    carregado = transparencia._carregar_cache("transparencia_contratos")

    assert carregado == []


def test_integracao_run_sucesso_com_api_mockada(tmp_path, monkeypatch):
    monkeypatch.setattr(transparencia.time, "sleep", lambda *_: None)

    def fake_get(url, headers, params, timeout):
        if url.endswith("/contratos"):
            return DummyResponse(
                status_code=200,
                payload=[
                    {
                        "numero": "C-INT-1",
                        "objetoContrato": "Obra de drenagem integrada",
                        "valorContrato": "R$ 10.000,00",
                        "dataAssinatura": "01/04/2026",
                    }
                ],
            )
        if url.endswith("/licitacoes"):
            return DummyResponse(
                status_code=200,
                payload=[
                    {
                        "id": "L-INT-1",
                        "objeto": "Reforma de praça central",
                        "valorTotal": 22000,
                        "dataAbertura": "2026-04-02",
                    }
                ],
            )
        raise AssertionError(f"URL não esperada no teste: {url}")

    monkeypatch.setattr(transparencia.requests, "get", fake_get)

    out = transparencia.run()

    assert not out["contratos"].empty
    assert not out["licitacoes"].empty
    assert out["contratos"].iloc[0]["id_contrato"] == "C-INT-1"
    assert out["licitacoes"].iloc[0]["id_licitacao"] == "L-INT-1"
    assert (tmp_path / "transparencia_contratos.json").exists()
    assert (tmp_path / "transparencia_licitacoes.json").exists()


def test_integracao_run_fallback_com_cache_em_disco(tmp_path, monkeypatch):
    monkeypatch.setattr(transparencia.time, "sleep", lambda *_: None)

    transparencia._salvar_cache(
        "transparencia_contratos",
        [
            {
                "numero": "C-CACHE-1",
                "objetoContrato": "Obra de pavimentação",
                "valorContrato": "R$ 12.345,67",
            }
        ],
    )
    transparencia._salvar_cache(
        "transparencia_licitacoes",
        [
            {
                "id": "L-CACHE-1",
                "objeto": "Reforma de escola municipal",
                "valorTotal": 98765,
            }
        ],
    )

    monkeypatch.setattr(
        transparencia,
        "fetch_contratos",
        lambda: (_ for _ in ()).throw(RuntimeError("api fora")),
    )
    monkeypatch.setattr(
        transparencia,
        "fetch_licitacoes",
        lambda: (_ for _ in ()).throw(RuntimeError("api fora")),
    )

    out = transparencia.run()

    assert out["contratos"].iloc[0]["id_contrato"] == "C-CACHE-1"
    assert out["licitacoes"].iloc[0]["id_licitacao"] == "L-CACHE-1"
