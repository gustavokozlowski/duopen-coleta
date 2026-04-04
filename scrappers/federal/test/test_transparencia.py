import json

import pandas as pd
import pytest
import requests

from scrappers.federal import transparencia


# Simula respostas da API do Portal da Transparência sem acessar rede externa.
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


# ── Cobertura: construção de headers e autenticação ─────────────────────────
def test_headers_com_api_key(monkeypatch):
	monkeypatch.setattr(transparencia, "API_KEY", "token-teste")

	headers = transparencia._headers()

	assert headers["Accept"] == "application/json"
	assert headers["Content-Type"] == "application/json"
	assert headers["chave-api"] == "token-teste"


def test_headers_sem_api_key(monkeypatch):
	monkeypatch.setattr(transparencia, "API_KEY", "")

	headers = transparencia._headers()

	assert "chave-api" not in headers


# ── Cobertura: cliente HTTP com retry/backoff/erros ─────────────────────────
def test_get_retorna_json_em_status_200(monkeypatch):
	called = {}

	def fake_get(url, headers, params, timeout):
		called["url"] = url
		called["headers"] = headers
		called["params"] = params
		called["timeout"] = timeout
		return DummyResponse(status_code=200, payload={"ok": True})

	monkeypatch.setattr(transparencia.requests, "get", fake_get)
	monkeypatch.setattr(transparencia, "API_KEY", "abc")

	data = transparencia._get("contratos", {"pagina": 1})

	assert data == {"ok": True}
	assert called["url"].endswith("/contratos")
	assert called["params"] == {"pagina": 1}
	assert called["timeout"] == transparencia.REQUEST_TIMEOUT


def test_get_retry_quando_429(monkeypatch):
	# Valida backoff exponencial em rate limit antes de tentar novamente.
	responses = [
		DummyResponse(status_code=429, payload={"erro": "rate"}),
		DummyResponse(status_code=200, payload={"ok": True}),
	]
	sleep_calls = []

	def fake_get(*args, **kwargs):
		return responses.pop(0)

	monkeypatch.setattr(transparencia.requests, "get", fake_get)
	monkeypatch.setattr(transparencia.time, "sleep", lambda s: sleep_calls.append(s))

	result = transparencia._get("licitacoes", {"pagina": 1})

	assert result == {"ok": True}
	assert sleep_calls == [transparencia.RETRY_BACKOFF * (2 ** 1)]


def test_get_lanca_runtimeerror_apos_timeouts(monkeypatch):
	# Garante falha controlada após esgotar as tentativas de timeout.
	monkeypatch.setattr(
		transparencia.requests,
		"get",
		lambda *a, **k: (_ for _ in ()).throw(requests.exceptions.Timeout()),
	)
	monkeypatch.setattr(transparencia.time, "sleep", lambda *_: None)

	with pytest.raises(RuntimeError):
		transparencia._get("contratos", {"pagina": 1})


def test_get_lanca_http_error(monkeypatch):
	# HTTPError deve ser propagado para tratamento no nível superior.
	error = requests.exceptions.HTTPError("erro 500")

	monkeypatch.setattr(
		transparencia.requests,
		"get",
		lambda *a, **k: DummyResponse(status_code=500, http_error=error),
	)

	with pytest.raises(requests.exceptions.HTTPError):
		transparencia._get("contratos", {"pagina": 1})


# ── Cobertura: paginação dos endpoints /contratos e /licitacoes ─────────────
def test_paginar_com_lista(monkeypatch):
	monkeypatch.setattr(transparencia, "PAGE_SIZE", 2)
	monkeypatch.setattr(transparencia, "MAX_PAGES", 5)
	monkeypatch.setattr(transparencia.time, "sleep", lambda *_: None)

	sequencia = [
		[{"id": 1}, {"id": 2}],
		[{"id": 3}],
	]

	monkeypatch.setattr(transparencia, "_get", lambda *a, **k: sequencia.pop(0))

	resultado = transparencia._paginar("contratos", {"municipioCodigoIbge": "3302403"})

	assert [r["id"] for r in resultado] == [1, 2, 3]


def test_paginar_com_dict_content(monkeypatch):
	monkeypatch.setattr(transparencia, "PAGE_SIZE", 3)
	monkeypatch.setattr(transparencia, "MAX_PAGES", 3)
	monkeypatch.setattr(transparencia.time, "sleep", lambda *_: None)

	sequencia = [
		{"content": [{"id": "a"}, {"id": "b"}]},
	]

	monkeypatch.setattr(transparencia, "_get", lambda *a, **k: sequencia.pop(0))

	resultado = transparencia._paginar("licitacoes", {"municipioCodigoIbge": "3302403"})

	assert resultado == [{"id": "a"}, {"id": "b"}]


def test_paginar_interrompe_em_runtime_error(monkeypatch):
	monkeypatch.setattr(transparencia, "PAGE_SIZE", 2)

	def fake_get(*args, **kwargs):
		raise RuntimeError("falha")

	monkeypatch.setattr(transparencia, "_get", fake_get)

	resultado = transparencia._paginar("contratos", {"municipioCodigoIbge": "3302403"})

	assert resultado == []


# ── Cobertura: parâmetros dos endpoints conforme integração esperada ─────────
def test_fetch_contratos_usa_endpoint_e_municipio(monkeypatch):
	captured = {}

	def fake_paginar(endpoint, params):
		captured["endpoint"] = endpoint
		captured["params"] = params
		return [{"id": 1}]

	monkeypatch.setattr(transparencia, "_paginar", fake_paginar)
	monkeypatch.setattr(transparencia, "MUNICIPIO_CODE", "3302403")

	dados = transparencia.fetch_contratos()

	assert dados == [{"id": 1}]
	assert captured["endpoint"] == "contratos"
	assert captured["params"] == {"municipioCodigoIbge": "3302403"}


def test_fetch_licitacoes_usa_endpoint_e_municipio(monkeypatch):
	captured = {}

	def fake_paginar(endpoint, params):
		captured["endpoint"] = endpoint
		captured["params"] = params
		return [{"id": 2}]

	monkeypatch.setattr(transparencia, "_paginar", fake_paginar)
	monkeypatch.setattr(transparencia, "MUNICIPIO_CODE", "3302403")

	dados = transparencia.fetch_licitacoes()

	assert dados == [{"id": 2}]
	assert captured["endpoint"] == "licitacoes"
	assert captured["params"] == {"municipioCodigoIbge": "3302403"}


# ── Cobertura: funções utilitárias de normalização ───────────────────────────
@pytest.mark.parametrize(
	"valor,esperado",
	[
		(None, None),
		(100, 100.0),
		("R$ 1.234,56", 1234.56),
		("abc", None),
	],
)
def test_normalizar_valor(valor, esperado):
	assert transparencia._normalizar_valor(valor) == esperado


@pytest.mark.parametrize(
	"data_str,esperado",
	[
		("01/01/2026", "2026-01-01T00:00:00+00:00"),
		("2026-01-02", "2026-01-02T00:00:00+00:00"),
		("2026-01-03T10:20:30Z", "2026-01-03T10:20:30+00:00"),
		("invalida", None),
	],
)
def test_normalizar_data(data_str, esperado):
	assert transparencia._normalizar_data(data_str) == esperado


def test_e_obra_detecta_keywords():
	assert transparencia._e_obra("Reforma de escola municipal") is True
	assert transparencia._e_obra("Aquisição de computadores") is False


# ── Cobertura: transformação de payload bruto em DataFrame ───────────────────
def test_normalizar_contratos_filtra_e_mapeia(monkeypatch):
	monkeypatch.setattr(transparencia, "MUNICIPIO_CODE", "3302403")

	registros = [
		{
			"numero": "C-01",
			"objetoContrato": "Obra de drenagem no bairro",
			"modalidadeCompra": {"descricao": "Concorrência"},
			"valorInicialCompra": "R$ 2.000,00",
			"dataAssinatura": "01/02/2026",
			"fornecedor": {"cnpjFormatado": "00.000.000/0001-00", "nome": "Construtora X"},
		},
		{
			"numero": "C-02",
			"objetoContrato": "Compra de papel",
		},
	]

	df = transparencia.normalizar_contratos(registros)

	assert len(df) == 1
	assert df.iloc[0]["id_contrato"] == "C-01"
	assert df.iloc[0]["valor_inicial"] == 2000.0
	assert json.loads(df.iloc[0]["payload_bruto"])["numero"] == "C-01"


def test_normalizar_licitacoes_filtra_e_mapeia(monkeypatch):
	monkeypatch.setattr(transparencia, "MUNICIPIO_CODE", "3302403")

	registros = [
		{
			"id": "L-01",
			"objeto": "Pavimentação de vias urbanas",
			"modalidade": "Pregão",
			"situacao": "Aberta",
			"valorTotal": 500000,
			"dataAbertura": "2026-03-10",
		},
		{
			"id": "L-02",
			"objeto": "Serviço de buffet",
		},
	]

	df = transparencia.normalizar_licitacoes(registros)

	assert len(df) == 1
	assert df.iloc[0]["id_licitacao"] == "L-01"
	assert df.iloc[0]["valor_estimado"] == 500000.0


# ── Cobertura: pipeline principal com e sem fallback de cache ────────────────
def test_run_sucesso(monkeypatch):
	raw_contratos = [{"numero": "C-1", "objetoContrato": "Obra de ponte"}]
	raw_licitacoes = [{"id": "L-1", "objeto": "Reforma de praça"}]

	monkeypatch.setattr(transparencia, "fetch_contratos", lambda: raw_contratos)
	monkeypatch.setattr(transparencia, "fetch_licitacoes", lambda: raw_licitacoes)

	saved = []
	monkeypatch.setattr(transparencia, "_salvar_cache", lambda nome, dados: saved.append((nome, dados)))

	monkeypatch.setattr(
		transparencia,
		"normalizar_contratos",
		lambda dados: pd.DataFrame([{"id_contrato": dados[0]["numero"]}]),
	)
	monkeypatch.setattr(
		transparencia,
		"normalizar_licitacoes",
		lambda dados: pd.DataFrame([{"id_licitacao": dados[0]["id"]}]),
	)

	out = transparencia.run()

	assert list(out.keys()) == ["contratos", "licitacoes"]
	assert out["contratos"].iloc[0]["id_contrato"] == "C-1"
	assert out["licitacoes"].iloc[0]["id_licitacao"] == "L-1"
	assert saved == [
		("transparencia_contratos", raw_contratos),
		("transparencia_licitacoes", raw_licitacoes),
	]


def test_run_fallback_para_cache(monkeypatch):
	# Simula indisponibilidade da API e garante uso de cache local.
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

	monkeypatch.setattr(
		transparencia,
		"_carregar_cache",
		lambda nome: [{"origem": nome, "objeto": "Obra de teste"}],
	)

	monkeypatch.setattr(
		transparencia,
		"normalizar_contratos",
		lambda dados: pd.DataFrame([{"from_cache": dados[0]["origem"]}]),
	)
	monkeypatch.setattr(
		transparencia,
		"normalizar_licitacoes",
		lambda dados: pd.DataFrame([{"from_cache": dados[0]["origem"]}]),
	)

	out = transparencia.run()

	assert out["contratos"].iloc[0]["from_cache"] == "transparencia_contratos"
	assert out["licitacoes"].iloc[0]["from_cache"] == "transparencia_licitacoes"


# ── Integração: cache em disco + pipeline run() sem rede real ───────────────
def test_integracao_salvar_e_carregar_cache_em_disco(tmp_path, monkeypatch):
	monkeypatch.setattr(transparencia, "CACHE_DIR", tmp_path)

	dados = [{"id": "A1", "objeto": "Obra de teste"}]
	transparencia._salvar_cache("transparencia_contratos", dados)

	carregado = transparencia._carregar_cache("transparencia_contratos")

	assert carregado == dados


def test_integracao_carregar_cache_invalido_retorna_lista_vazia(tmp_path, monkeypatch):
	monkeypatch.setattr(transparencia, "CACHE_DIR", tmp_path)
	path = tmp_path / "transparencia_contratos.json"
	path.write_text('{"invalido": true}', encoding="utf-8")

	carregado = transparencia._carregar_cache("transparencia_contratos")

	assert carregado == []


def test_integracao_run_sucesso_com_api_mockada(tmp_path, monkeypatch):
	monkeypatch.setattr(transparencia, "CACHE_DIR", tmp_path)
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
	monkeypatch.setattr(transparencia, "CACHE_DIR", tmp_path)
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
