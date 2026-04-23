import pandas as pd
import pytest

from etl import loader

pytestmark = pytest.mark.unit


class DummyApiError(Exception):
	def __init__(self, message: str, status_code: int | None = None):
		super().__init__(message)
		self.status_code = status_code


class FakeQuery:
	def __init__(self, client, table_name: str):
		self.client = client
		self.table_name = table_name
		self.payload = []
		self.on_conflict = None

	def upsert(self, payload, on_conflict=None):
		self.payload = payload
		self.on_conflict = on_conflict
		return self

	def execute(self):
		self.client.calls.append(
			{
				"table": self.table_name,
				"payload": self.payload,
				"on_conflict": self.on_conflict,
			}
		)
		behavior = self.client.next_behavior()
		if isinstance(behavior, Exception):
			raise behavior
		return {"data": self.payload}


class FakeClient:
	def __init__(self, behaviors=None):
		self.behaviors = list(behaviors or [])
		self.calls = []

	def table(self, table_name: str):
		return FakeQuery(self, table_name)

	def next_behavior(self):
		if not self.behaviors:
			return None
		return self.behaviors.pop(0)


def _build_df(total: int) -> pd.DataFrame:
	return pd.DataFrame(
		[
			{
				"id_contrato": f"C-{i}",
				"municipio": "Macae",
				"fonte": "teste",
			}
			for i in range(total)
		]
	)


def test_load_divide_em_batches_de_500_e_retorna_total(monkeypatch):
	fixed_now = "2026-04-22T12:00:00+00:00"
	monkeypatch.setattr(loader, "_utc_now_iso", lambda: fixed_now)

	df = _build_df(1200)
	client = FakeClient()

	total = loader.load(df, tabela="raw_contratos", client=client)

	assert total == 1200
	assert len(client.calls) == 3
	assert [len(call["payload"]) for call in client.calls] == [500, 500, 200]
	assert all(call["on_conflict"] == "id_contrato" for call in client.calls)
	assert client.calls[0]["payload"][0]["coletado_em"] == fixed_now
	assert "coletado_em" not in df.columns


def test_load_continua_apos_falha_de_batch(caplog):
	df = _build_df(700)
	client = FakeClient(behaviors=[None, DummyApiError("bad request", status_code=400)])

	with caplog.at_level("ERROR"):
		total = loader.load(df, tabela="raw_contratos", client=client)

	assert total == 500
	assert len(client.calls) == 2
	assert "IDs com problema" in caplog.text
	assert "C-500" in caplog.text


def test_load_retry_com_backoff_exponencial_em_5xx(monkeypatch):
	df = _build_df(1)
	client = FakeClient(
		behaviors=[
			DummyApiError("http 503", status_code=503),
			DummyApiError("http 502", status_code=502),
			None,
		]
	)
	sleeps = []
	monkeypatch.setattr(loader.time, "sleep", lambda s: sleeps.append(s))

	total = loader.load(df, tabela="raw_contratos", client=client)

	assert total == 1
	assert sleeps == [2.0, 4.0]
	assert len(client.calls) == 3


def test_load_nao_retry_em_4xx(monkeypatch):
	df = _build_df(1)
	client = FakeClient(behaviors=[DummyApiError("http 400", status_code=400)])
	sleeps = []
	monkeypatch.setattr(loader.time, "sleep", lambda s: sleeps.append(s))

	total = loader.load(df, tabela="raw_contratos", client=client)

	assert total == 0
	assert sleeps == []
	assert len(client.calls) == 1


def test_load_converte_bytes_para_hex_bytea(monkeypatch):
	monkeypatch.setattr(loader, "_utc_now_iso", lambda: "2026-04-22T00:00:00+00:00")

	df = pd.DataFrame(
		[
			{
				"id_contrato": "C-1",
				"objeto_contrato": b"abc",
			}
		]
	)
	client = FakeClient()

	loader.load(df, tabela="raw_contratos", client=client)

	payload = client.calls[0]["payload"][0]
	assert payload["objeto_contrato"] == "\\x616263"


def test_init_client_exige_credenciais(monkeypatch):
	monkeypatch.delenv("SUPABASE_URL", raising=False)
	monkeypatch.delenv("SUPABASE_KEY", raising=False)

	with pytest.raises(ValueError):
		loader.init_client()


def test_init_client_chama_create_client(monkeypatch):
	captured = {}

	def fake_create_client(url, key):
		captured["url"] = url
		captured["key"] = key
		return "client-ok"

	monkeypatch.setattr(loader, "create_client", fake_create_client)

	client = loader.init_client("https://example.supabase.co", "service-role-key")

	assert client == "client-ok"
	assert captured["url"] == "https://example.supabase.co"
	assert captured["key"] == "service-role-key"


def test_init_client_rejeita_chave_anon():
	with pytest.raises(ValueError):
		loader.init_client("https://example.supabase.co", "anon-key")
