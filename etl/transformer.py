"""ETL transformer: lê Raw tables e popula a camada Estruturada do Supabase."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import Client, create_client

# ── Configuração ──────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("etl.transformer")

# ── Constantes e mapeamentos ──────────────────────────────────────────────────

PRIORIDADE_FONTES = [
    "painel_obras_atual_macae",
    "painel_obras_legado_macae",
    "portal_transparencia_macae_contratos",
    "portal_transparencia_federal",
    "tce_rj_contratos",
    "tce_rj_compras_diretas",
    "sismob_cidadao",
    "egim_google_mymaps",
    "tce_rj_obras_paralisadas",
]

SITUACAO_SAUDE_MAP = {
    "concluída":    "Concluída",
    "concluida":    "Concluída",
    "em execução":  "Em andamento",
    "em execucao":  "Em andamento",
    "paralisada":   "Paralisada",
    "não iniciada": "Planejada",
    "nao iniciada": "Planejada",
    "licitação":    "Planejada",
    "licitacao":    "Planejada",
    "projeto":      "Planejada",
    "cancelada":    "Cancelada",
}

SITUACAO_GEOREF_MAP = {
    "concluída":    "Concluída",
    "em andamento": "Em andamento",
    "paralisada":   "Paralisada",
    "planejada":    "Planejada",
    "indefinido":   "Em andamento",
}

KEYWORDS_OBRA = {
    "obra", "construção", "reforma", "ampliação", "pavimentação",
    "drenagem", "saneamento", "urbanização", "infraestrutura",
    "recapeamento", "iluminação", "edificação", "galeria",
}


# ── Cliente Supabase ──────────────────────────────────────────────────────────

def get_client() -> Client:
    load_dotenv()
    return create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_KEY"],
    )


# ── Leitura das Raw tables ────────────────────────────────────────────────────

def ler_raw(client: Client, tabela: str) -> pd.DataFrame:
    try:
        resp = client.table(tabela).select("*").execute()
        df = pd.DataFrame(resp.data or [])
        log.info("ler_raw: %s → %d registros", tabela, len(df))
        return df
    except Exception as exc:
        log.error("ler_raw: falha ao ler %s: %s", tabela, exc)
        return pd.DataFrame()


# ── Transformação: fornecedores ───────────────────────────────────────────────

def transformar_fornecedores(raw_contratos: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por CNPJ único com métricas calculadas."""
    if raw_contratos.empty or "cnpj_fornecedor" not in raw_contratos.columns:
        return pd.DataFrame()

    df = raw_contratos.copy()
    df = df[df["cnpj_fornecedor"].notna() & (df["cnpj_fornecedor"].astype(str).str.strip() != "")].copy()

    if df.empty:
        return pd.DataFrame()

    df = df.assign(cnpj_fornecedor=df["cnpj_fornecedor"].astype(str).str.strip())

    # data_assinatura para ordenação do nome mais recente
    if "data_assinatura" in df.columns:
        df = df.assign(_data_ord=pd.to_datetime(df["data_assinatura"], errors="coerce"))
    else:
        df = df.assign(_data_ord=pd.Series(pd.NaT, index=df.index))

    # nome mais recente por cnpj
    if "nome_fornecedor" in df.columns:
        nome_recente = (
            df.sort_values("_data_ord", na_position="first")
            .groupby("cnpj_fornecedor")["nome_fornecedor"]
            .last()
            .reset_index()
            .rename(columns={"nome_fornecedor": "razao_social"})
        )
    else:
        nome_recente = pd.DataFrame(
            {"cnpj_fornecedor": df["cnpj_fornecedor"].unique(), "razao_social": None}
        )

    # métricas por cnpj
    agg: dict = {"_total": ("cnpj_fornecedor", "count")}

    if "valor_inicial" in df.columns:
        df = df.assign(valor_inicial=pd.to_numeric(df["valor_inicial"], errors="coerce"))
        agg["valor_total"] = ("valor_inicial", "sum")

    if "data_assinatura" in df.columns:
        # converter para datetime para garantir min/max funcionem com valores nulos
        df = df.assign(data_assinatura=pd.to_datetime(df["data_assinatura"], errors="coerce"))
        agg["primeiro_contrato"] = ("data_assinatura", "min")
        agg["ultimo_contrato"] = ("data_assinatura", "max")

    # anos de atuação
    if "_data_ord" in df.columns:
        df = df.assign(_ano=df["_data_ord"].dt.year)

    grp = df.groupby("cnpj_fornecedor")

    metricas = grp.agg(**{k: v for k, v in agg.items()}).reset_index()
    metricas = metricas.rename(columns={"_total": "total_contratos"})

    # anos de atuação
    if "_ano" in df.columns:
        anos = grp["_ano"].nunique().reset_index().rename(columns={"_ano": "anos_atuacao"})
        metricas = metricas.merge(anos, on="cnpj_fornecedor", how="left")

    # taxa de aditivo
    if "possui_aditivo" in df.columns:
        tem_aditivo = (
            df["possui_aditivo"].astype(str).str.lower().str.strip() == "sim"
        ).astype(float)
        taxa = (
            df.assign(_tem=tem_aditivo)
            .groupby("cnpj_fornecedor")["_tem"]
            .mean()
            .reset_index()
            .rename(columns={"_tem": "taxa_aditivo"})
        )
        taxa = taxa.assign(taxa_aditivo=(taxa["taxa_aditivo"] * 100).round(2))
        metricas = metricas.merge(taxa, on="cnpj_fornecedor", how="left")
    elif "qtd_aditivos" in df.columns:
        df = df.assign(
            _tem_adit=(pd.to_numeric(df["qtd_aditivos"], errors="coerce").fillna(0) > 0).astype(float)
        )
        grp = df.groupby("cnpj_fornecedor")
        taxa = (
            grp["_tem_adit"]
            .mean()
            .reset_index()
            .rename(columns={"_tem_adit": "taxa_aditivo"})
        )
        taxa = taxa.assign(taxa_aditivo=(taxa["taxa_aditivo"] * 100).round(2))
        metricas = metricas.merge(taxa, on="cnpj_fornecedor", how="left")

    resultado = metricas.merge(nome_recente, on="cnpj_fornecedor", how="left")
    resultado = resultado.rename(columns={"cnpj_fornecedor": "cnpj"})

    # fallback para razao_social nula
    mask_nulo = resultado["razao_social"].isna() | (resultado["razao_social"].astype(str).str.strip() == "")
    resultado.loc[mask_nulo, "razao_social"] = "Fornecedor " + resultado.loc[mask_nulo, "cnpj"]

    resultado = resultado.drop_duplicates(subset=["cnpj"])
    return resultado.reset_index(drop=True)


# ── Transformação: obras ──────────────────────────────────────────────────────

def _contem_palavra_obra(texto: str) -> bool:
    if not isinstance(texto, str):
        return False
    lower = texto.lower()
    return any(kw in lower for kw in KEYWORDS_OBRA)


def _col(df: pd.DataFrame, nome: str):
    return df[nome] if nome in df.columns else None


def _get(df: pd.DataFrame, nome: str) -> pd.Series:
    if nome in df.columns:
        return df[nome]
    return pd.Series([None] * len(df), index=df.index)


def _obras_de_atual(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    r = pd.DataFrame()
    r["nome"] = _get(df, "nome_obra")
    r["objeto"] = _get(df, "nome_obra")
    r["tipo"] = "Obra Municipal"
    r["situacao"] = _get(df, "situacao")
    r["secretaria"] = _get(df, "secretaria")
    r["bairro"] = _get(df, "bairro")
    r["endereco"] = _get(df, "endereco")
    r["municipio"] = "Macaé"
    r["uf"] = "RJ"
    r["percentual_executado"] = _get(df, "percentual_executado")
    r["valor_contrato"] = _get(df, "valor_contrato")
    r["valor_aditivos"] = _get(df, "valor_aditivos")
    r["data_inicio"] = _get(df, "data_inicio")
    r["data_prevista_fim"] = _get(df, "data_prevista_fim")
    r["latitude"] = _get(df, "latitude")
    r["longitude"] = _get(df, "longitude")
    r["fonte_origem"] = "painel_obras_atual_macae"
    r["id_origem"] = _get(df, "id_obra").astype(str)
    return r


def _obras_de_legado(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    r = pd.DataFrame()
    r["nome"] = _get(df, "nome_obra")
    r["objeto"] = _get(df, "nome_obra")
    r["tipo"] = "Obra Municipal"
    r["situacao"] = _get(df, "situacao")
    r["secretaria"] = _get(df, "secretaria")
    r["bairro"] = _get(df, "bairro")
    r["municipio"] = "Macaé"
    r["uf"] = "RJ"
    r["percentual_executado"] = _get(df, "percentual_executado")
    r["valor_contrato"] = _get(df, "valor_contrato")
    r["valor_aditivos"] = _get(df, "valor_aditivos")
    r["valor_final"] = _get(df, "valor_final")
    r["data_inicio"] = _get(df, "data_inicio")
    r["data_prevista_fim"] = _get(df, "data_prevista_fim")
    r["data_conclusao"] = _get(df, "data_conclusao")
    r["dias_atraso"] = _get(df, "dias_atraso")
    r["latitude"] = _get(df, "latitude")
    r["longitude"] = _get(df, "longitude")
    r["fonte_origem"] = "painel_obras_legado_macae"
    r["id_origem"] = _get(df, "id_obra").astype(str)
    return r


def _obras_de_contratos(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "objeto" not in df.columns:
        return pd.DataFrame()

    mask = df["objeto"].apply(_contem_palavra_obra)
    df = df[mask].copy()
    if df.empty:
        return pd.DataFrame()

    r = pd.DataFrame()
    r["nome"] = _get(df, "objeto").astype(str).str[:200]
    r["objeto"] = _get(df, "objeto")
    if "tipo_contrato" in df.columns:
        r["tipo"] = _get(df, "tipo_contrato").fillna("Obra Municipal")
    else:
        r["tipo"] = "Obra Municipal"
    r["situacao"] = _get(df, "situacao")

    # secretaria: primeiro disponível
    sec = _get(df, "secretaria")
    if sec.isna().all() and "orgao" in df.columns:
        sec = _get(df, "orgao")
    if sec.isna().all() and "unidade_gestora" in df.columns:
        sec = _get(df, "unidade_gestora")
    r["secretaria"] = sec

    r["municipio"] = "Macaé"
    r["uf"] = "RJ"
    r["valor_contrato"] = _get(df, "valor_inicial")
    r["valor_aditivos"] = _get(df, "valor_aditivos")
    r["data_inicio"] = _get(df, "data_inicio_vigencia")
    r["data_prevista_fim"] = _get(df, "data_fim_vigencia")
    r["fonte_origem"] = _get(df, "fonte")
    r["id_origem"] = _get(df, "id_contrato").astype(str)
    return r


def _obras_de_saude(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    r = pd.DataFrame()
    r["nome"] = _get(df, "nome_estabelecimento")
    r["objeto"] = _get(df, "tipo_obra")
    r["tipo"] = "Saúde"

    sit_raw = _get(df, "situacao_obra").astype(str).str.lower().str.strip()
    r["situacao"] = sit_raw.map(SITUACAO_SAUDE_MAP).where(
        sit_raw.map(SITUACAO_SAUDE_MAP).notna(),
        _get(df, "situacao_obra"),
    )

    r["bairro"] = _get(df, "bairro")
    r["endereco"] = _get(df, "logradouro")
    r["municipio"] = "Macaé"
    r["uf"] = "RJ"
    r["percentual_executado"] = _get(df, "percentual_executado")
    r["valor_contrato"] = _get(df, "valor_proposta")
    r["data_prevista_fim"] = _get(df, "dt_prevista_conclusao")
    r["data_conclusao"] = _get(df, "dt_conclusao_final")
    r["latitude"] = _get(df, "latitude")
    r["longitude"] = _get(df, "longitude")
    r["fonte_origem"] = "sismob_cidadao"
    r["id_origem"] = _get(df, "proposta_id").astype(str)
    return r


def _obras_de_georef(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    r = pd.DataFrame()
    r["nome"] = _get(df, "nome_obra")
    r["objeto"] = _get(df, "descricao")
    r["tipo"] = "Georreferenciada"

    sit_raw = _get(df, "status").astype(str).str.lower().str.strip()
    r["situacao"] = sit_raw.map(SITUACAO_GEOREF_MAP).where(
        sit_raw.map(SITUACAO_GEOREF_MAP).notna(),
        sit_raw,
    )

    r["secretaria"] = _get(df, "secretaria")
    r["bairro"] = _get(df, "bairro")
    r["endereco"] = _get(df, "endereco")
    r["municipio"] = "Macaé"
    r["uf"] = "RJ"
    r["latitude"] = _get(df, "latitude")
    r["longitude"] = _get(df, "longitude")
    r["fonte_origem"] = "egim_google_mymaps"
    r["id_origem"] = _get(df, "nome_obra").astype(str)
    return r


def _obras_de_paralisadas(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    r = pd.DataFrame()
    r["nome"] = _get(df, "nome_obra")
    r["objeto"] = _get(df, "nome_obra")
    r["tipo"] = _get(df, "tipo_obra")
    r["situacao"] = "Paralisada"
    r["secretaria"] = _get(df, "orgao")
    r["municipio"] = "Macaé"
    r["uf"] = "RJ"
    r["percentual_executado"] = _get(df, "percentual_executado")
    r["valor_contrato"] = _get(df, "valor_contrato")
    r["data_inicio"] = _get(df, "data_inicio")
    r["fonte_origem"] = "tce_rj_obras_paralisadas"
    r["id_origem"] = _get(df, "id_obra").astype(str)
    return r


def _prioridade(fonte: str) -> int:
    try:
        return PRIORIDADE_FONTES.index(fonte)
    except ValueError:
        return len(PRIORIDADE_FONTES)


def _gerar_geometry(lat, lon) -> Optional[str]:
    try:
        if lat is None or lon is None:
            return None
        lat_f = float(lat)
        lon_f = float(lon)
        if pd.isna(lat_f) or pd.isna(lon_f):
            return None
        return f"POINT({lon_f} {lat_f})"
    except (TypeError, ValueError):
        return None


def transformar_obras(
    raw_contratos: pd.DataFrame,
    raw_obras_atual: pd.DataFrame,
    raw_obras_legado: pd.DataFrame,
    raw_obras_saude: pd.DataFrame,
    raw_obras_georef: pd.DataFrame,
    raw_obras_paralisadas: pd.DataFrame,
) -> pd.DataFrame:
    parciais = [
        _obras_de_atual(raw_obras_atual),
        _obras_de_legado(raw_obras_legado),
        _obras_de_contratos(raw_contratos),
        _obras_de_saude(raw_obras_saude),
        _obras_de_georef(raw_obras_georef),
        _obras_de_paralisadas(raw_obras_paralisadas),
    ]

    nao_vazios = [p for p in parciais if not p.empty]
    if not nao_vazios:
        return pd.DataFrame()

    df = pd.concat(nao_vazios, ignore_index=True, sort=False)

    df = df.assign(_prioridade=df["fonte_origem"].apply(_prioridade))
    df = df.sort_values(["id_origem", "_prioridade"], ascending=True)
    df = df.drop_duplicates(subset=["fonte_origem", "id_origem"], keep="first")
    df = df.drop(columns=["_prioridade"])

    # geometry — LONGITUDE primeiro (padrão WKT PostGIS)
    if "latitude" in df.columns and "longitude" in df.columns:
        df = df.assign(geometry=df.apply(
            lambda row: _gerar_geometry(row.get("latitude"), row.get("longitude")),
            axis=1,
        ))

    # garantir municipio/uf
    df = df.assign(
        municipio=df["municipio"].fillna("Macaé"),
        uf=df["uf"].fillna("RJ"),
    )

    # log por fonte
    for fonte in df["fonte_origem"].unique():
        qtd = (df["fonte_origem"] == fonte).sum()
        log.info("transformar_obras: %s → %d registros", fonte, qtd)
    log.info("transformar_obras: total consolidado → %d registros", len(df))

    return df.reset_index(drop=True)


# ── Transformação: contratos ──────────────────────────────────────────────────

def transformar_contratos(
    raw_contratos: pd.DataFrame,
    obras_df: pd.DataFrame,
    fornecedores_df: pd.DataFrame,
) -> pd.DataFrame:
    """Retorna apenas contratos com id_obra válido."""
    if raw_contratos.empty:
        return pd.DataFrame()

    df = raw_contratos.copy()

    # join com obras por (id_contrato == id_origem AND fonte == fonte_origem)
    if obras_df.empty or "id" not in obras_df.columns:
        log.warning("transformar_contratos: obras_df sem coluna 'id' — descartando todos os contratos")
        return pd.DataFrame()

    obras_lookup = (
        obras_df[["id", "id_origem", "fonte_origem"]]
        .rename(columns={"id": "id_obra", "id_origem": "_id_contrato_str", "fonte_origem": "_fonte"})
        .assign(_id_contrato_str=lambda x: x["_id_contrato_str"].astype(str))
    )

    df = df.assign(
        _id_contrato_str=_get(df, "id_contrato").astype(str),
        _fonte=_get(df, "fonte").astype(str),
    )

    df = df.merge(
        obras_lookup.rename(columns={"id_origem": "_id_contrato_str", "fonte_origem": "_fonte"}),
        on=["_id_contrato_str", "_fonte"],
        how="left",
    )

    total_antes = len(df)
    descartados = df["id_obra"].isna().sum()
    df = df[df["id_obra"].notna()]
    if descartados:
        log.warning(
            "transformar_contratos: %d contratos descartados por falta de obra linkada",
            descartados,
        )
    log.info("transformar_contratos: %d/%d contratos com obra", len(df), total_antes)

    # join com fornecedores
    if not fornecedores_df.empty and "id" in fornecedores_df.columns and "cnpj" in fornecedores_df.columns:
        forn_lookup = fornecedores_df[["id", "cnpj"]].rename(columns={"id": "id_fornecedor"})
        df = df.merge(forn_lookup, left_on="cnpj_fornecedor", right_on="cnpj", how="left")
    else:
        df = df.assign(id_fornecedor=None)

    # numero: num_licitacao ou id_contrato
    if "num_licitacao" in df.columns:
        num = df["num_licitacao"].where(
            df["num_licitacao"].notna() & (df["num_licitacao"].astype(str).str.strip() != ""),
            _get(df, "id_contrato"),
        )
        df = df.assign(numero=num)
    else:
        df = df.assign(numero=_get(df, "id_contrato"))

    resultado = pd.DataFrame()
    resultado["id_obra"] = df["id_obra"]
    resultado["id_fornecedor"] = df.get("id_fornecedor", pd.Series([None] * len(df), index=df.index))
    resultado["numero"] = df["numero"]
    resultado["objeto"] = _get(df, "objeto")
    resultado["modalidade"] = _get(df, "modalidade")
    resultado["situacao"] = _get(df, "situacao")
    resultado["valor_inicial"] = _get(df, "valor_inicial")
    resultado["valor_global"] = _get(df, "valor_global")
    resultado["valor_aditivos"] = _get(df, "valor_aditivos")
    resultado["data_assinatura"] = _get(df, "data_assinatura")
    resultado["data_inicio"] = _get(df, "data_inicio_vigencia")
    resultado["data_fim"] = _get(df, "data_fim_vigencia")
    qtd = pd.to_numeric(_get(df, "qtd_aditivos"), errors="coerce").fillna(0)
    resultado["qtd_aditivos"] = qtd
    resultado["fonte_origem"] = _get(df, "fonte")

    return resultado.reset_index(drop=True)


# ── Transformação: aditivos ───────────────────────────────────────────────────

def transformar_aditivos(
    raw_contratos: pd.DataFrame,
    contratos_df: pd.DataFrame,
) -> pd.DataFrame:
    """Um registro consolidado por contrato com valor_aditivos > 0."""
    if raw_contratos.empty or contratos_df.empty:
        return pd.DataFrame()

    if "id" not in contratos_df.columns:
        log.warning("transformar_aditivos: contratos_df sem coluna 'id'")
        return pd.DataFrame()

    df = raw_contratos.copy()

    if "valor_aditivos" not in df.columns:
        return pd.DataFrame()

    df = df.assign(_valor_adic=pd.to_numeric(df["valor_aditivos"], errors="coerce").fillna(0))
    df = df[df["_valor_adic"] > 0].copy()

    if df.empty:
        return pd.DataFrame()

    # join com contratos por (fonte == fonte_origem AND id_contrato == numero)
    lookup = (
        contratos_df[["id", "fonte_origem", "numero"]]
        .rename(columns={"id": "_id_struct", "numero": "_id_contrato_str", "fonte_origem": "_fonte"})
        .assign(_id_contrato_str=lambda x: x["_id_contrato_str"].astype(str))
    )

    df = df.assign(
        _id_contrato_str=_get(df, "id_contrato").astype(str),
        _fonte=_get(df, "fonte").astype(str),
    )

    df = df.merge(lookup, on=["_id_contrato_str", "_fonte"], how="left")

    descartados = int(df["_id_struct"].isna().sum())
    df = df[df["_id_struct"].notna()]
    if descartados:
        log.info("transformar_aditivos: %d linhas descartadas sem id_contrato", descartados)

    resultado = pd.DataFrame()
    resultado["id_contrato"] = df["_id_struct"]
    resultado["tipo"] = "Consolidado"
    resultado["motivo"] = "Aditivo(s) registrado(s) na fonte original"
    resultado["valor"] = df["_valor_adic"]
    resultado["prazo_dias"] = None
    resultado["data_assinatura"] = None

    log.info("transformar_aditivos: %d aditivos consolidados criados", len(resultado))
    return resultado.reset_index(drop=True)


# ── Upsert nas tabelas estruturadas ──────────────────────────────────────────

def _nan_to_none(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def upsert(
    client: Client,
    tabela: str,
    df: pd.DataFrame,
    chave_conflito: list[str],
    batch_size: int = 500,
) -> int:
    if df is None or df.empty:
        return 0

    df = df.copy()
    df["atualizado_em"] = datetime.now(timezone.utc).isoformat()

    on_conflict = ",".join(chave_conflito)
    records = df.to_dict(orient="records")
    records = [
        {k: _nan_to_none(v) for k, v in row.items()}
        for row in records
    ]

    total = 0
    num_batches = (len(records) + batch_size - 1) // batch_size

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        lote_n = i // batch_size + 1
        try:
            client.table(tabela).upsert(batch, on_conflict=on_conflict).execute()
            total += len(batch)
            log.info(
                "upsert: %s lote %d/%d — %d registros processados",
                tabela, lote_n, num_batches, len(batch),
            )
        except Exception as exc:
            log.error(
                "upsert: falha no lote %d/%d de %s: %s",
                lote_n, num_batches, tabela, exc,
            )

    return total


# ── Pipeline principal: run() ─────────────────────────────────────────────────

def run() -> dict:
    """
    ORDEM OBRIGATÓRIA (respeita FK):
        1. fornecedores  — sem FK
        2. obras         — sem FK
        3. contratos     — FK: obras + fornecedores
        4. aditivos      — FK: contratos

    Falha em uma etapa não aborta as próximas.
    """
    client = get_client()

    raw_contratos         = ler_raw(client, "raw_contratos")
    raw_obras_atual       = ler_raw(client, "raw_obras_atual")
    raw_obras_legado      = ler_raw(client, "raw_obras_legado")
    raw_obras_saude       = ler_raw(client, "raw_obras_saude")
    raw_obras_georef      = ler_raw(client, "raw_obras_georef")
    raw_obras_paralisadas = ler_raw(client, "raw_obras_paralisadas")

    resultado = {"fornecedores": 0, "obras": 0, "contratos": 0, "aditivos": 0}

    # 1. Fornecedores
    try:
        fornecedores_df = transformar_fornecedores(raw_contratos)
        resultado["fornecedores"] = upsert(client, "fornecedores", fornecedores_df, ["cnpj"])
    except Exception as exc:
        log.error("run: etapa fornecedores falhou: %s", exc)
        fornecedores_df = pd.DataFrame()

    # 2. Obras
    try:
        obras_df = transformar_obras(
            raw_contratos, raw_obras_atual, raw_obras_legado,
            raw_obras_saude, raw_obras_georef, raw_obras_paralisadas,
        )
        resultado["obras"] = upsert(client, "obras", obras_df, ["fonte_origem", "id_origem"])
    except Exception as exc:
        log.error("run: etapa obras falhou: %s", exc)
        obras_df = pd.DataFrame()

    # 3. Contratos
    try:
        contratos_df = transformar_contratos(raw_contratos, obras_df, fornecedores_df)
        resultado["contratos"] = upsert(client, "contratos", contratos_df, ["numero", "fonte_origem"])
    except Exception as exc:
        log.error("run: etapa contratos falhou: %s", exc)
        contratos_df = pd.DataFrame()

    # 4. Aditivos
    try:
        aditivos_df = transformar_aditivos(raw_contratos, contratos_df)
        resultado["aditivos"] = upsert(client, "aditivos", aditivos_df, ["id_contrato"])
    except Exception as exc:
        log.error("run: etapa aditivos falhou: %s", exc)

    return resultado


if __name__ == "__main__":
    resultado = run()
    for tabela, total in resultado.items():
        print(f"  {tabela}: {total} registros")
