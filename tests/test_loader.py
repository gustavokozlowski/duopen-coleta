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
	assert all(call["on_conflict"] == "id_contrato,fonte" for call in client.calls)
	assert client.calls[0]["payload"][0]["coletado_em"] == fixed_now
	assert "coletado_em" not in df.columns


def test_load_continua_apos_falha_de_batch(caplog):
	df = _build_df(700)
	client = FakeClient(behaviors=[None, DummyApiError("bad request", status_code=400)])

	with caplog.at_level("ERROR"):
		total = loader.load(df, tabela="raw_contratos", client=client)

	assert total == 500
	assert len(client.calls) == 2
	assert "Chaves" in caplog.text
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


def test_load_aceita_conflict_composto_como_string(monkeypatch):
	monkeypatch.setattr(loader, "_utc_now_iso", lambda: "2026-04-22T00:00:00+00:00")

	df = _build_df(1)
	client = FakeClient()

	loader.load(
		df,
		tabela="raw_contratos",
		conflict_column="id_contrato, fonte",
		client=client,
	)

	assert client.calls[0]["on_conflict"] == "id_contrato,fonte"


def test_load_filtra_colunas_para_schema_alvo(monkeypatch):
	monkeypatch.setattr(loader, "_utc_now_iso", lambda: "2026-04-22T00:00:00+00:00")

	df = pd.DataFrame(
		[
			{
				"id_contrato": "C-1",
				"fonte": "teste",
				"campo_extra": "ignorar",
			}
		]
	)
	client = FakeClient()

	loader.load(
		df,
		tabela="raw_contratos",
		allowed_columns=["id_contrato", "fonte", "coletado_em", "payload_bruto"],
		client=client,
	)

	payload = client.calls[0]["payload"][0]
	assert "campo_extra" not in payload
	assert set(payload.keys()) == {"id_contrato", "fonte", "coletado_em", "payload_bruto"}


def test_load_serializa_payload_bruto_como_json_string(monkeypatch):
	monkeypatch.setattr(loader, "_utc_now_iso", lambda: "2026-04-22T00:00:00+00:00")

	df = pd.DataFrame(
		[
			{
				"id_contrato": "C-1",
				"fonte": "teste",
				"valor": 123,
			}
		]
	)
	client = FakeClient()

	loader.load(df, tabela="raw_contratos", client=client)

	payload = client.calls[0]["payload"][0]
	import json
	parsed = json.loads(payload["payload_bruto"])
	assert parsed["id_contrato"] == "C-1"
	assert parsed["valor"] == 123


def test_load_converte_bytes_para_hex_bytea(monkeypatch):
	monkeypatch.setattr(loader, "_utc_now_iso", lambda: "2026-04-22T00:00:00+00:00")

	df = pd.DataFrame(
		[
			{
				"id_contrato": "C-1",
				"fonte": "teste",
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


def test_load_none_df_retorna_zero():
	assert loader.load(None, tabela="raw_contratos", client=FakeClient()) == 0


def test_load_batch_size_zero_lanca_erro():
	df = _build_df(1)
	with pytest.raises(ValueError):
		loader.load(df, tabela="raw_contratos", batch_size=0, client=FakeClient())


def test_to_supabase_value_converte_timestamp(monkeypatch):
	monkeypatch.setattr(loader, "_utc_now_iso", lambda: "2026-01-01T00:00:00+00:00")
	ts = pd.Timestamp("2025-06-01 12:00:00")
	result = loader._to_supabase_value(ts)
	assert "2025-06-01" in result


def test_to_supabase_value_converte_datetime():
	from datetime import datetime, timezone
	dt = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
	result = loader._to_supabase_value(dt)
	assert "2025-06-01" in result


def test_to_supabase_value_converte_lista():
	result = loader._to_supabase_value([1, None, "x"])
	assert result == [1, None, "x"]


def test_to_supabase_value_converte_dict():
	result = loader._to_supabase_value({"a": 1, "b": None})
	assert result == {"a": 1, "b": None}


def test_to_supabase_value_bool():
	assert loader._to_supabase_value(True) is True
	assert loader._to_supabase_value(False) is False


def test_to_supabase_value_float_nao_inteiro():
	assert loader._to_supabase_value(3.14) == 3.14


def test_to_supabase_value_tipo_desconhecido():
	class Obj:
		def __str__(self):
			return "repr_obj"
	result = loader._to_supabase_value(Obj())
	assert result == "repr_obj"


def test_to_bytes_bytearray():
	result = loader._to_bytes(bytearray(b"abc"))
	assert result == b"abc"


def test_to_bytes_memoryview():
	result = loader._to_bytes(memoryview(b"abc"))
	assert result == b"abc"


def test_to_iso_utc_datetime_com_timezone():
	from datetime import datetime, timezone, timedelta
	dt = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=-3)))
	result = loader._to_iso_utc_datetime(dt)
	assert "15:00:00" in result  # 12h BRT = 15h UTC


def test_is_missing_excecao_em_pd_isna():
	# objetos customizados que lançam exceção em pd.isna devem retornar False
	class Obj:
		pass
	result = loader._is_missing(Obj())
	assert result is False


def test_colunas_da_tabela_sucesso(monkeypatch):
	import unittest.mock as mock
	client = mock.MagicMock()
	client.table.return_value.select.return_value.limit.return_value.execute.return_value.data = [
		{"id": 1, "nome": "X"}
	]
	colunas = loader.colunas_da_tabela(client, "raw_contratos")
	assert "id" in colunas
	assert "nome" in colunas


def test_colunas_da_tabela_dados_vazios(monkeypatch):
	import unittest.mock as mock
	client = mock.MagicMock()
	client.table.return_value.select.return_value.limit.return_value.execute.return_value.data = []
	colunas = loader.colunas_da_tabela(client, "raw_contratos")
	assert colunas == []


def test_colunas_da_tabela_erro_retorna_lista_vazia():
	import unittest.mock as mock
	client = mock.MagicMock()
	client.table.side_effect = RuntimeError("erro")
	result = loader.colunas_da_tabela(client, "qualquer")
	assert result == []


def test_registrar_ingestao_sucesso(monkeypatch):
	import unittest.mock as mock
	client = mock.MagicMock()
	loader.registrar_ingestao(client, "teste", "ok", 10, 1.5)
	client.table.return_value.insert.assert_called_once()


def test_registrar_ingestao_com_mensagem(monkeypatch):
	import unittest.mock as mock
	client = mock.MagicMock()
	loader.registrar_ingestao(client, "teste", "erro", 0, 0.1, mensagem="algo deu errado")
	call_args = client.table.return_value.insert.call_args[0][0]
	assert call_args.get("mensagem") == "algo deu errado"


def test_registrar_ingestao_falha_silenciosa(caplog):
	import unittest.mock as mock
	client = mock.MagicMock()
	client.table.return_value.insert.return_value.execute.side_effect = RuntimeError("db down")
	with caplog.at_level("WARNING"):
		loader.registrar_ingestao(client, "fonte_x", "ok", 5, 0.5)
	assert "Falha" in caplog.text
