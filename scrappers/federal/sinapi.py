"""
scrappers/federal/sinapi.py
Referencia de custos SINAPI/CUB — duopen-coleta · DUOPEN 2026

Fornece os custos de referencia por m2 para cada tipo de obra no Rio de
Janeiro. Essencial para calcular o componente C (Custo por m2) do IEOP:

    razao = custo_real_m2 / sinapi_referencia_m2

Para o prototipo, a fonte e a tabela embutida `SINAPI_REFERENCIA_RJ`
(unica fonte confiavel sem autenticacao). O download automatico do
arquivo SINAPI da CEF fica como melhoria pos-hackathon.

Fluxo:
    1. Materializa a tabela embutida em uma linha por tipo_obra
    2. Grava cache/sinapi.json como lista de registros
    3. Retorna DataFrame pronto para o ETL (rota "sinapi" -> raw_sinapi)

Variaveis de ambiente: nenhuma necessaria.
"""

import os
import json
import logging
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Configuração ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scrapper.sinapi")

CACHE_DIR = Path(__file__).parent.parent.parent / "cache"


# ── Tabela de referência embutida ──────────────────────────────────────────────
# Valores aproximados para o Rio de Janeiro (R$/m²).
# Fonte: SINAPI/CUB RJ — referência 2026. Atualizar manualmente a cada trimestre.

SINAPI_REFERENCIA_RJ = {
    # tipo_obra            custo_m2 (R$)
    "residencial_popular":      1850.00,
    "residencial_normal":       2400.00,
    "residencial_alto_padrao":  3800.00,
    "comercial_salas_lojas":    2200.00,
    "galpao_industrial":        1200.00,
    "escola":                   2800.00,
    "ubs":                      3200.00,
    "upa":                      3500.00,
    "caps":                     3000.00,
    "quadra_esportiva":          900.00,
    "pavimentacao_asfalto":      450.00,
    "pavimentacao_concreto":     650.00,
    "drenagem":                  380.00,
    "calcamento":                350.00,
    "praca_urbanizacao":         800.00,
    "padrao":                   2000.00,  # fallback genérico
}


# ── Mapeamento tipo_obra → categoria SINAPI ─────────────────────────────────────
# Chaves mais específicas vêm antes das genéricas — a ordem importa na busca
# parcial (ex.: "recapeamento" antes de "pavimentacao").

TIPO_PARA_SINAPI = {
    "ubs":            "ubs",
    "upa":            "upa",
    "caps":           "caps",
    "hospital":       "ubs",
    "creche":         "escola",
    "colegio":        "escola",
    "escola":         "escola",
    "cras":           "comercial_salas_lojas",
    "creas":          "comercial_salas_lojas",
    "recapeamento":   "pavimentacao_asfalto",
    "pavimentacao":   "pavimentacao_asfalto",
    "drenagem":       "drenagem",
    "galeria":        "drenagem",
    "calcamento":     "calcamento",
    "praca":          "praca_urbanizacao",
    "parque":         "praca_urbanizacao",
    "quadra":         "quadra_esportiva",
    "construcao":     "residencial_normal",
    "reforma":        "residencial_normal",
    "ampliacao":      "residencial_normal",
}


def _sem_acento(texto: str) -> str:
    """Remove acentos e baixa para minúsculas — busca robusta."""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def mapear_tipo_sinapi(tipo_obra: str) -> str:
    """
    Mapeia o tipo cru de uma obra para a categoria SINAPI mais próxima.
    Busca parcial case-insensitive e sem acentos.
    Retorna 'padrao' quando não há correspondência.
    """
    if not tipo_obra:
        return "padrao"
    t = _sem_acento(str(tipo_obra))
    for chave, categoria in TIPO_PARA_SINAPI.items():
        if chave in t:
            return categoria
    return "padrao"


def custo_referencia(tipo_obra: str) -> float:
    """Retorna o custo/m² de referência para o tipo de obra informado."""
    return SINAPI_REFERENCIA_RJ[mapear_tipo_sinapi(tipo_obra)]


# ── Cache ───────────────────────────────────────────────────────────────────────

def _salvar_cache(registros: list[dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / "sinapi.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, indent=2)
    log.info(f"Cache salvo: {path} ({len(registros)} registros)")


# ── Pipeline principal ──────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
    """
    Materializa a tabela de referência SINAPI/CUB do RJ.

    Retorna DataFrame com uma linha por tipo_obra:
        uf · competencia · tipo_obra · custo_m2 · coletado_em

    A coluna `fonte` é definida pelo routing (`.assign(fonte=rota["fonte"])`),
    então não é incluída aqui. Grava cache/sinapi.json como lista de registros.
    """
    competencia = datetime.now(timezone.utc).strftime("%Y-%m")
    coletado_em = datetime.now(timezone.utc).isoformat()

    rows = [
        {
            "uf":          "RJ",
            "competencia": competencia,
            "tipo_obra":   tipo_obra,
            "custo_m2":    custo_m2,
            "coletado_em": coletado_em,
        }
        for tipo_obra, custo_m2 in SINAPI_REFERENCIA_RJ.items()
    ]

    _salvar_cache(rows)
    df = pd.DataFrame(rows)
    log.info(f"SINAPI: {len(df)} referências de custo carregadas (fonte: embutida)")
    return df


if __name__ == "__main__":
    df = run()
    print(f"\n── SINAPI · {len(df)} referências de custo (RJ) ──\n")
    print(df.to_string(index=False))
