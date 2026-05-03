from __future__ import annotations

import gzip
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from scrappers import xd_software


@pytest.fixture
def cache_env(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    monkeypatch.setenv("CACHE_DIR", str(cache_dir))
    return cache_dir


def _make_xlsx(path: Path, df: pd.DataFrame) -> None:
    df.to_excel(path, index=False)


@pytest.mark.unit
def test_run_ler_xlsx(cache_env, tmp_path):
    df_in = pd.DataFrame([
        {"Coluna A": "valor-1", "Valor": 123.45},
        {"Coluna A": "valor-2", "Valor": 678.90},
    ])
    path = tmp_path / "xd_export.xlsx"
    _make_xlsx(path, df_in)

    out = xd_software.run(str(path))

    assert len(out) == 2
    assert "payload_bruto" in out.columns
    assert "fonte" in out.columns
    assert "coletado_em" in out.columns
    assert out.iloc[0]["payload_bruto"]["Coluna A"] == "valor-1"
    assert out.iloc[0]["fonte"] == "XD Software"


@pytest.mark.unit
def test_run_ler_zip(cache_env, tmp_path):
    df_in = pd.DataFrame([
        {"Coluna A": "valor-1", "Valor": 123.45},
        {"Coluna A": "valor-2", "Valor": 678.90},
    ])
    xlsx_path = tmp_path / "xd_export.xlsx"
    _make_xlsx(xlsx_path, df_in)

    zip_path = tmp_path / "xd_export.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(xlsx_path, arcname="inner.xlsx")

    out = xd_software.run(str(zip_path))

    assert len(out) == 2
    assert out.iloc[1]["payload_bruto"]["Coluna A"] == "valor-2"
    assert out.iloc[0]["fonte"] == "XD Software"


@pytest.mark.unit
def test_run_ler_gz(cache_env, tmp_path):
    df_in = pd.DataFrame([
        {"Coluna A": "valor-1", "Valor": 123.45},
        {"Coluna A": "valor-2", "Valor": 678.90},
    ])
    xlsx_path = tmp_path / "xd_export.xlsx"
    _make_xlsx(xlsx_path, df_in)

    gz_path = tmp_path / "xd_export.xlsx.gz"
    with gzip.open(gz_path, "wb") as handle:
        handle.write(xlsx_path.read_bytes())

    out = xd_software.run(str(gz_path))

    assert len(out) == 2
    assert out.iloc[0]["payload_bruto"]["Valor"] == 123.45
    assert out.iloc[0]["fonte"] == "XD Software"


@pytest.mark.unit
def test_run_fallback_quando_inexistente(cache_env, monkeypatch):
    fallback_df = pd.DataFrame([
        {"Coluna A": "cache-1", "Valor": 1},
    ])
    monkeypatch.setattr(xd_software, "carregar_cache", lambda _name: fallback_df)

    out = xd_software.run("/tmp/nao_existe.xlsx")

    assert len(out) == 1
    assert out.iloc[0]["Coluna A"] == "cache-1"


@pytest.mark.unit
def test_run_normaliza_datas_para_utc(cache_env, tmp_path):
    df_in = pd.DataFrame([
        {"data_inicio": "2024-01-02", "Valor": 10},
    ])
    path = tmp_path / "xd_export.xlsx"
    _make_xlsx(path, df_in)

    out = xd_software.run(str(path))

    assert pd.api.types.is_datetime64tz_dtype(out["data_inicio"])
    assert out.iloc[0]["data_inicio"] == pd.Timestamp("2024-01-02", tz="UTC")


@pytest.mark.unit
def test_run_normaliza_bytes_para_utf8(cache_env, tmp_path, monkeypatch):
    df_in = pd.DataFrame([
        {"Nome": b"Obra"},
    ])
    path = tmp_path / "xd_export.xlsx"
    path.write_text("x", encoding="utf-8")

    monkeypatch.setattr(xd_software.pd, "read_excel", lambda *_a, **_k: df_in)

    out = xd_software.run(str(path))

    assert out.iloc[0]["Nome"] == "Obra"
    assert out.iloc[0]["payload_bruto"]["Nome"] == b"Obra"


@pytest.mark.unit
def test_run_fallback_quando_vazio(cache_env, tmp_path, monkeypatch):
    path = tmp_path / "xd_export.xlsx"
    path.write_text("x", encoding="utf-8")

    monkeypatch.setattr(xd_software.pd, "read_excel", lambda *_a, **_k: pd.DataFrame())
    fallback_df = pd.DataFrame([
        {"Coluna A": "cache-2", "Valor": 2},
    ])
    monkeypatch.setattr(xd_software, "carregar_cache", lambda _name: fallback_df)

    out = xd_software.run(str(path))

    assert len(out) == 1
    assert out.iloc[0]["Coluna A"] == "cache-2"
