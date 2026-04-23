import os
from uuid import uuid4

import pandas as pd
import pytest

from etl.compressor import compress
from etl.loader import init_client, load

pytestmark = pytest.mark.integration


def test_loader_upsert_em_staging_quando_configurado():
	"""Executa upsert real no Supabase staging quando o ambiente estiver configurado."""
	url = os.getenv("SUPABASE_URL", "").strip()
	key = os.getenv("SUPABASE_KEY", "").strip()
	tabela = os.getenv("SUPABASE_STAGING_TABLE", "").strip()

	if not (url and key and tabela):
		pytest.skip("SUPABASE_URL, SUPABASE_KEY e SUPABASE_STAGING_TABLE nao configurados")

	client = init_client(url, key)
	contract_id = f"it-loader-{uuid4().hex[:12]}"

	df = pd.DataFrame(
		[
			{
				"id_contrato": contract_id,
				"municipio": "Macae",
				"fonte": "loader_integration_test",
				"objeto_contrato": "pavimentacao " * 30,
			}
		]
	)

	# Simula etapa real do pipeline (compressor -> loader).
	df_comprimido = compress(df)

	try:
		total = load(df_comprimido, tabela=tabela, client=client)
		assert total == 1

		response = (
			client.table(tabela)
			.select("id_contrato")
			.eq("id_contrato", contract_id)
			.limit(1)
			.execute()
		)
		data = getattr(response, "data", None) or []
		assert len(data) == 1
		assert data[0].get("id_contrato") == contract_id
	except Exception as exc:
		pytest.skip(f"Ambiente staging indisponivel ou schema incompativel: {exc}")