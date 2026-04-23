import pandas as pd
import pytest

from etl.compressor import (
	MIN_COMPRESS_BYTES,
	benchmark_compression_ratio,
	compress,
	compress_field,
	decompress_field,
)

pytestmark = pytest.mark.unit


def _large_text() -> str:
	return "pavimentacao " * 30


def test_compress_field_round_trip_para_texto_grande() -> None:
	text = _large_text()
	blob = compress_field(text)

	assert isinstance(blob, bytes)
	assert decompress_field(blob) == text


def test_compress_field_ignora_texto_menor_que_64_bytes() -> None:
	text = "texto curto"
	out = compress_field(text)

	assert out == text


def test_decompress_field_mantem_valor_nao_binario() -> None:
	text = "valor literal"

	assert decompress_field(text) == text


def test_compress_aplica_apenas_campos_alvo() -> None:
	long_text = _large_text()
	df = pd.DataFrame(
		[
			{
				"id_contrato": "1",
				"objeto_contrato": long_text,
				"historico_obra": long_text,
				"razao_social_fornecedor": long_text,
				"descricao_item": long_text,
				"valor_contrato": 1000.0,
				"municipio": "Macae",
			}
		]
	)

	out = compress(df)

	assert isinstance(out.loc[0, "objeto_contrato"], bytes)
	assert isinstance(out.loc[0, "historico_obra"], bytes)
	assert isinstance(out.loc[0, "razao_social_fornecedor"], bytes)
	assert isinstance(out.loc[0, "descricao_item"], bytes)
	assert out.loc[0, "valor_contrato"] == 1000.0
	assert out.loc[0, "municipio"] == "Macae"


def test_compress_mantem_estrutura_e_campos_nao_existentes() -> None:
	df = pd.DataFrame(
		[
			{
				"id_contrato": "1",
				"valor_contrato": 500.0,
			}
		]
	)

	out = compress(df)

	assert list(out.columns) == ["id_contrato", "valor_contrato"]
	assert out.loc[0, "valor_contrato"] == 500.0


def test_benchmark_compression_ratio_retorna_percentual_esperado() -> None:
	long_text = _large_text()
	df_before = pd.DataFrame(
		[
			{
				"objeto_contrato": long_text,
				"historico_obra": long_text,
			}
		]
	)
	df_after = compress(df_before)

	ratio = benchmark_compression_ratio(df_before, df_after)

	assert ratio is not None
	assert ratio > 40
	assert ratio <= 100


def test_decompress_field_payload_binario_invalido_nao_quebra() -> None:
	invalid_blob = b"nao-zlib-valido"

	out = decompress_field(invalid_blob)

	assert out == invalid_blob


def test_compress_field_retorna_binario_sem_recompressao() -> None:
	original_text = _large_text()
	blob = compress_field(original_text)

	out = compress_field(blob)

	assert isinstance(out, bytes)
	assert out == blob
	assert decompress_field(out) == original_text


def test_compress_respeita_limite_customizado() -> None:
	text = "x" * (MIN_COMPRESS_BYTES - 1)
	df = pd.DataFrame([{"objeto_contrato": text}])

	out = compress(df, min_compress_bytes=MIN_COMPRESS_BYTES + 20)

	assert out.loc[0, "objeto_contrato"] == text
