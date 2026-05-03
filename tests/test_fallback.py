import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from etl.fallback import carregar_cache, cache_valido, listar_caches, salvar_cache


@pytest.fixture
def cache_env(tmp_path, monkeypatch):
    monkeypatch.setenv("CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("CACHE_MAX_DIAS", "1")
    return tmp_path


@pytest.mark.unit
def test_salvar_e_carregar_dataframe(cache_env):
    df = pd.DataFrame([
        {"id": 1, "nome": "obra-1"},
        {"id": 2, "nome": "obra-2"},
    ])

    ok = salvar_cache("df", df)

    assert ok is True
    out = carregar_cache("df")
    assert out is not None
    pd.testing.assert_frame_equal(out, df)


@pytest.mark.unit
def test_salvar_e_carregar_lista(cache_env):
    dados = [{"a": 1}, {"a": 2}]

    ok = salvar_cache("lista", dados)

    assert ok is True
    out = carregar_cache("lista")
    assert out is not None
    assert len(out) == 2
    assert out.iloc[0]["a"] == 1


@pytest.mark.unit
def test_salvar_e_carregar_dict(cache_env):
    dados = {"a": 1, "b": "x"}

    ok = salvar_cache("dict", dados)

    assert ok is True
    out = carregar_cache("dict")
    assert out is not None
    assert len(out) == 1
    assert out.iloc[0]["b"] == "x"


@pytest.mark.unit
def test_carregar_retorna_none_se_nao_existe(cache_env):
    out = carregar_cache("nao_existe")

    assert out is None


@pytest.mark.unit
def test_cache_valido_retorna_true_quando_recente(cache_env):
    salvar_cache("recente", {"x": 1})

    assert cache_valido("recente", max_dias=1) is True


@pytest.mark.unit
def test_cache_valido_retorna_false_quando_expirado(cache_env):
    salvar_cache("expirado", {"x": 1})

    path = cache_env / "expirado.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["metadata"]["salvo_em"] = (
        datetime.now(timezone.utc) - timedelta(days=2, hours=1)
    ).isoformat()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    assert cache_valido("expirado", max_dias=1) is False


@pytest.mark.unit
def test_salvar_nao_quebra_pipeline_em_excecao(cache_env, monkeypatch):
    def boom(*_args, **_kwargs):
        raise OSError("erro de escrita")

    monkeypatch.setattr(Path, "write_text", boom, raising=True)

    ok = salvar_cache("falha", {"x": 1})

    assert ok is False


@pytest.mark.unit
def test_listar_caches_retorna_lista_vazia_sem_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("CACHE_DIR", str(tmp_path / "cache_vazio"))

    out = listar_caches()

    assert out == []


@pytest.mark.unit
def test_default_str_serializa_datetime(cache_env):
    data = {"ts": datetime(2026, 1, 1, tzinfo=timezone.utc)}

    ok = salvar_cache("datetime", data)

    assert ok is True
    path = cache_env / "datetime.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["dados"][0]["ts"] == "2026-01-01 00:00:00+00:00"


@pytest.mark.unit
def test_carregar_cache_json_invalido(cache_env):
    path = cache_env / "invalido.json"
    path.write_text("{nao_json}", encoding="utf-8")

    out = carregar_cache("invalido")

    assert out is None


@pytest.mark.unit
def test_cache_valido_metadata_invalida(cache_env):
    path = cache_env / "sem_meta.json"
    path.write_text(json.dumps({"dados": []}), encoding="utf-8")

    out = cache_valido("sem_meta")

    assert out is False


@pytest.mark.unit
def test_listar_caches_retorna_itens(cache_env, monkeypatch):
    monkeypatch.setenv("CACHE_MAX_DIAS", "1")
    data = {
        "metadata": {
            "nome": "foo",
            "salvo_em": datetime.now(timezone.utc).isoformat(),
            "total_registros": 1,
            "versao": "1.0",
        },
        "dados": [{"x": 1}],
    }
    path = cache_env / "foo.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    out = listar_caches()

    assert len(out) == 1
    assert out[0]["nome"] == "foo"
    assert out[0]["registros"] == 1
    assert out[0]["valido"] is True
