"""ETL transformer: lê Raw tables e popula a camada Estruturada do Supabase."""

from __future__ import annotations

import logging
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import Client, create_client

from etl.cleaner import normalize_situacao

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


# Contratos não trazem endereço/bairro estruturado, mas o objeto frequentemente
# cita o local em texto livre. Extração conservadora (precisão > cobertura).
_RE_BAIRRO = re.compile(
    r"\bBAIRRO\s+(?:D[AEO]S?\s+)?([A-ZÀ-Ú][\wÀ-ú\s]{2,28}?)"
    r"(?:\s*[,;.]|\s+MACA[ÉE]|\s+LOCALIZAD|/RJ)",
    re.IGNORECASE,
)
_RE_LOGRADOURO = re.compile(
    r"LOCALIZAD[AO]S?\s+N[AOE]S?\s+"
    r"((?:RUA|AVENIDA|AV\.|RODOVIA|RJ-?\d+|ESTRADA|TRAVESSA|ALAMEDA|LARGO|PRA[ÇC]A)"
    r"\s+[A-ZÀ-Ú][^,;]{2,55}?)(?:\s*[,;]|\s+MACA[ÉE]|\s+BAIRRO|/RJ|$)",
    re.IGNORECASE,
)


def _extrair_bairro_do_objeto(texto) -> Optional[str]:
    """Extrai o bairro citado no objeto do contrato (ex: 'no BAIRRO Lagomar')."""
    if not isinstance(texto, str):
        return None
    m = _RE_BAIRRO.search(texto)
    if not m:
        return None
    bairro = re.sub(r"\s+", " ", m.group(1)).strip(" .,-").title()
    return bairro or None


def _extrair_logradouro_do_objeto(texto) -> Optional[str]:
    """Extrai o logradouro citado no objeto (ex: 'LOCALIZADA NA RUA Alfredo Tanos')."""
    if not isinstance(texto, str):
        return None
    m = _RE_LOGRADOURO.search(texto)
    if not m:
        return None
    logr = re.sub(r"\s+", " ", m.group(1)).strip(" .,-")
    return logr or None


def _col(df: pd.DataFrame, nome: str):
    return df[nome] if nome in df.columns else None


def _get(df: pd.DataFrame, nome: str) -> pd.Series:
    if nome in df.columns:
        return df[nome]
    return pd.Series([None] * len(df), index=df.index)


def _normalizar_situacao(serie: pd.Series) -> pd.Series:
    """Delega ao cleaner.normalize_situacao — aplica as 3 regras oficiais por elemento."""
    return serie.map(normalize_situacao)


def _obras_de_atual(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    r = pd.DataFrame()
    r["nome"] = _get(df, "nome_obra")
    r["objeto"] = _get(df, "nome_obra")
    r["tipo"] = "Obra Municipal"
    r["situacao"] = _normalizar_situacao(_get(df, "situacao"))
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
    # Chave de junção obra↔contrato (consumida pelo duopen-ml para enriquecimento)
    r["cnpj_executora"] = _get(df, "cnpj_executora")
    r["num_contrato"] = _get(df, "num_contrato")
    r["num_licitacao"] = _get(df, "num_licitacao")
    r["fonte_origem"] = "painel_obras_atual_macae"
    r["id_origem"] = _get(df, "id_obra").astype(str)
    return r


def _obras_de_legado(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    r = pd.DataFrame()
    r["nome"] = _get(df, "nome_obra")
    r["objeto"] = _get(df, "objeto").fillna(_get(df, "nome_obra"))
    r["tipo"] = "Obra Municipal"
    r["situacao"] = _normalizar_situacao(_get(df, "situacao"))
    r["secretaria"] = _get(df, "secretaria")
    r["bairro"] = _get(df, "bairro")
    r["municipio"] = "Macaé"
    r["uf"] = "RJ"
    r["percentual_executado"] = _get(df, "percentual_executado")
    r["percentual_executado_financeiro"] = _get(df, "percentual_executado_financeiro")
    r["valor_contrato"] = _get(df, "valor_contrato")
    r["valor_aditivos"] = _get(df, "valor_aditivos")
    r["valor_final"] = _get(df, "valor_final")
    r["data_inicio"] = _get(df, "data_inicio")
    r["data_prevista_fim"] = _get(df, "data_prevista_fim")
    r["data_conclusao"] = _get(df, "data_conclusao")
    # Legado não tem data de conclusão exata, só o ano (~40% preenchido). Exposto cru
    # para o duopen-ml derivar um atraso de granularidade anual (sem fabricar data).
    r["ano_conclusao"] = _get(df, "ano_conclusao")
    r["dias_atraso"] = _get(df, "dias_atraso")
    r["latitude"] = _get(df, "latitude")
    r["longitude"] = _get(df, "longitude")
    # Chave de junção obra↔contrato (consumida pelo duopen-ml para enriquecimento)
    # legado: codigo_transacao_obras→num_contrato, nr_convenio_obras→num_licitacao
    r["cnpj_executora"] = _get(df, "cnpj_executora")
    r["num_contrato"] = _get(df, "num_contrato")
    r["num_licitacao"] = _get(df, "num_licitacao")
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
    r["situacao"] = _normalizar_situacao(_get(df, "situacao"))

    # secretaria: primeiro disponível
    sec = _get(df, "secretaria")
    if sec.isna().all() and "orgao" in df.columns:
        sec = _get(df, "orgao")
    if sec.isna().all() and "unidade_gestora" in df.columns:
        sec = _get(df, "unidade_gestora")
    r["secretaria"] = sec

    r["municipio"] = "Macaé"
    r["uf"] = "RJ"
    # bairro/endereço não vêm estruturados em contratos — extrair do texto do objeto
    objeto_série = _get(df, "objeto")
    r["bairro"] = objeto_série.apply(_extrair_bairro_do_objeto)
    r["endereco"] = objeto_série.apply(_extrair_logradouro_do_objeto)
    r["valor_contrato"] = _get(df, "valor_inicial")
    r["valor_aditivos"] = _get(df, "valor_aditivos")
    # data_inicio: vigência quando houver, senão data de assinatura (proxy do início)
    r["data_inicio"] = _get(df, "data_inicio_vigencia").fillna(_get(df, "data_assinatura"))
    r["data_prevista_fim"] = _get(df, "data_fim_vigencia")
    r["fonte_origem"] = _get(df, "fonte")
    r["id_origem"] = _get(df, "id_contrato").astype(str)
    return r


def _obras_de_saude(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    r = pd.DataFrame()
    nome = _get(df, "nome_estabelecimento")
    tipo = _get(df, "tipo_obra")
    id_str = "Obra SISMOB " + _get(df, "proposta_id").astype(str)
    # fallback: nome_estabelecimento → tipo_obra → "Obra SISMOB {id}"
    r["nome"] = nome.where(nome.notna() & (nome.astype(str).str.strip() != ""),
                 tipo.where(tipo.notna() & (tipo.astype(str).str.strip() != ""), id_str))
    r["objeto"] = tipo
    r["tipo"] = "Saúde"

    r["situacao"] = _normalizar_situacao(_get(df, "situacao"))

    # SISMOB são todas obras de infraestrutura de saúde do Ministério da Saúde
    r["secretaria"] = "Saúde"
    r["bairro"] = _get(df, "bairro")
    r["endereco"] = _get(df, "logradouro")
    r["municipio"] = "Macaé"
    r["uf"] = "RJ"
    r["percentual_executado"] = _get(df, "percentual_executado")
    r["valor_contrato"] = _get(df, "valor_proposta")
    # dt_prevista_conclusao quase sempre vem nula no SISMOB; dt_prevista_conclusao_final
    # (provável conclusão final) é o prazo de fato preenchido. Sem ele, data_prevista_fim
    # fica nula e dias_atraso não é calculável para a saúde.
    r["data_prevista_fim"] = _get(df, "dt_prevista_conclusao_final").fillna(
        _get(df, "dt_prevista_conclusao")
    )
    r["data_conclusao"] = _get(df, "dt_conclusao_final")
    r["latitude"] = _get(df, "latitude")
    r["longitude"] = _get(df, "longitude")
    r["fonte_origem"] = "sismob_cidadao"
    r["id_origem"] = _get(df, "proposta_id").astype(str)
    return r


_MESES_PT = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8, "setembro": 9,
    "outubro": 10, "novembro": 11, "dezembro": 12,
}


def _previsao_termino_para_iso(previsao, data_inicio_iso):
    """
    Converte o campo livre 'previsao_termino' do EGIM para timestamp ISO ou None.

    A coluna obras.data_prevista_fim é TIMESTAMPTZ — não aceita texto livre.
    Formatos tratados:
      - 'Dezembro/2023'        → 2023-12-01 (primeiro dia do mês)
      - '360 DIAS' / '15 meses' → data_inicio + N (requer data_inicio)
      - qualquer outro          → None (evita quebrar o upsert)
    """
    if previsao is None or (isinstance(previsao, float) and pd.isna(previsao)):
        return None
    texto = str(previsao).strip()
    if not texto or texto.lower() in ("none", "nan", "-"):
        return None

    # Formato "Mês/Ano"
    if "/" in texto:
        partes = texto.split("/")
        if len(partes) == 2:
            mes = _MESES_PT.get(partes[0].strip().lower())
            ano = partes[1].strip()
            if mes and ano.isdigit():
                try:
                    return datetime(int(ano), mes, 1, tzinfo=timezone.utc).isoformat()
                except (ValueError, TypeError):
                    return None

    # Formato "N DIAS/MESES/ANOS" — soma ao data_inicio
    m = re.search(r"(\d+)\s*(DIA|MES|SEMANA|ANO)", texto, flags=re.IGNORECASE)
    if m and data_inicio_iso:
        n = int(m.group(1))
        unidade = m.group(2).upper()
        dias = {"DIA": n, "MES": n * 30, "SEMANA": n * 7, "ANO": n * 365}[unidade]
        try:
            dt_ini = pd.to_datetime(data_inicio_iso, utc=True, errors="coerce")
            if pd.notna(dt_ini):
                return (dt_ini + timedelta(days=dias)).isoformat()
        except (ValueError, TypeError):
            return None

    return None


def _obras_de_georef(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    r = pd.DataFrame()
    r["nome"] = _get(df, "nome_obra")
    r["objeto"] = _get(df, "descricao")
    r["tipo"] = "Georreferenciada"

    r["situacao"] = _normalizar_situacao(_get(df, "situacao"))

    r["secretaria"] = _get(df, "secretaria")
    r["bairro"] = _get(df, "bairro")
    r["endereco"] = _get(df, "endereco")
    r["municipio"] = "Macaé"
    r["uf"] = "RJ"
    r["percentual_executado"] = pd.to_numeric(_get(df, "percentual"), errors="coerce")
    r["valor_contrato"]       = (
        _get(df, "valor")
        .str.replace(r"[R$\s\.]", "", regex=True)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )
    data_inicio = _get(df, "data_inicio")
    r["data_inicio"]          = data_inicio
    r["data_prevista_fim"]    = [
        _previsao_termino_para_iso(p, di)
        for p, di in zip(_get(df, "previsao_termino"), data_inicio)
    ]
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
    # classificacao_obra (ex: "SAÚDE - UBS, CAPS") é mais descritivo que nome_obra
    r["objeto"] = _get(df, "tipo_obra").fillna(_get(df, "nome_obra"))
    r["tipo"] = _get(df, "tipo_obra").fillna("Obra Paralisada")
    r["situacao"] = "Paralisada"
    # orgao raramente preenchido nessa fonte — nome_obra contém a entidade gestora
    r["secretaria"] = _get(df, "orgao").fillna(
        _get(df, "funcao_governo").fillna(_get(df, "nome_obra"))
    )
    r["municipio"] = "Macaé"
    r["uf"] = "RJ"
    # percentual financeiro: valor pago / valor contratado × 100
    valor_pago = pd.to_numeric(_get(df, "valor_pago_obra"), errors="coerce")
    valor_cont = pd.to_numeric(_get(df, "valor_contrato"), errors="coerce")
    pct_calc = (valor_pago / valor_cont * 100).round(1).clip(upper=99.0)
    pct_fonte = pd.to_numeric(_get(df, "percentual_executado"), errors="coerce")
    r["percentual_executado"] = pct_fonte.where(pct_fonte.notna(), pct_calc)
    r["valor_contrato"] = valor_cont
    r["valor_final"] = valor_pago
    r["data_inicio"] = _get(df, "data_inicio")
    # data_paralisacao = quando a obra parou (mais informativo que data_prevista_fim)
    r["data_prevista_fim"] = _get(df, "data_paralisacao")
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


def _ajustar_percentual(df: pd.DataFrame) -> pd.DataFrame:
    """
    Última linha de defesa: aplica regras de fallback para percentual_executado
    sobre todas as fontes consolidadas no DataFrame de obras.

    Regras (só aplicadas quando percentual está ausente ou negativo):
      1. Concluída / Em funcionamento        → 100
      2. Cancelada / Rescindida / Suspensa   → 0
      3. Em fase de planejamento / Cadastrada → 0
      4. Em andamento / Em execução + datas  → proporção de tempo (max 99)
    """
    if df.empty or "situacao" not in df.columns or "percentual_executado" not in df.columns:
        return df

    resultado = df.copy()
    sit = resultado["situacao"].astype(str).str.strip().str.lower()
    pct = pd.to_numeric(resultado["percentual_executado"], errors="coerce")
    ausente = pct.isna() | (pct < 0)
    # Para Concluída, 0% também é inválido — obra concluída necessariamente é 100%
    ausente_concluida = ausente | (pct == 0)

    # Regra 1 — Concluída
    mask = sit.isin({"concluida", "concluída", "em funcionamento"})
    resultado.loc[mask & ausente_concluida, "percentual_executado"] = 100.0

    # Regra 2 — Cancelada / Rescindida / Suspensa
    mask = sit.isin({"cancelada", "rescindida", "suspensa", "em cancelamento"})
    resultado.loc[mask & ausente, "percentual_executado"] = 0.0

    # Regra 3 — Planejada / Não iniciada
    mask = sit.isin({"em fase de planejamento", "planejada", "cadastrada",
                     "em ação preparatória", "em acao preparatoria"})
    resultado.loc[mask & ausente, "percentual_executado"] = 0.0

    # Regra 4 — Em andamento com datas: proporção de tempo (max 99)
    mask_and = sit.isin({"em andamento", "em execução", "em execucao", "em obras", "em obra"})
    if "data_inicio" in resultado.columns and "data_prevista_fim" in resultado.columns:
        hoje = pd.Timestamp.now(tz="UTC")
        dt_ini = pd.to_datetime(resultado["data_inicio"], utc=True, errors="coerce")
        dt_fim = pd.to_datetime(resultado["data_prevista_fim"], utc=True, errors="coerce")
        tem_datas = dt_ini.notna() & dt_fim.notna() & (dt_fim > dt_ini)
        alvo = resultado[mask_and & ausente & tem_datas].index
        for idx in alvo:
            total = (dt_fim[idx] - dt_ini[idx]).days
            if total > 0:
                decorrido = (hoje - dt_ini[idx]).days
                resultado.loc[idx, "percentual_executado"] = min(
                    round(decorrido / total * 100, 1), 99.0
                )

    # Default final: qualquer percentual ainda nulo vira 0
    pct_final = pd.to_numeric(resultado["percentual_executado"], errors="coerce")
    resultado.loc[pct_final.isna(), "percentual_executado"] = 0.0

    return resultado


# mantido como alias para não quebrar chamadas legadas internas
_ajustar_percentual_concluido = _ajustar_percentual


def _calcular_dias_atraso(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deriva dias_atraso a partir das datas já disponíveis (nunca vem da fonte).

    - Concluída/Rescindida com data_conclusao: dias entre conclusão e prazo previsto.
    - Em andamento sem conclusão: dias desde que o prazo previsto venceu até hoje.
    Valor só é positivo quando há atraso real; caso contrário fica 0. NULL apenas
    quando não há data_prevista_fim para comparar.
    """
    if df.empty or "data_prevista_fim" not in df.columns:
        return df

    resultado = df.copy()
    hoje = pd.Timestamp.now(tz="UTC")
    dt_fim = pd.to_datetime(resultado["data_prevista_fim"], utc=True, errors="coerce")
    if "data_conclusao" in resultado.columns:
        dt_concl = pd.to_datetime(resultado["data_conclusao"], utc=True, errors="coerce")
    else:
        dt_concl = pd.Series(pd.NaT, index=resultado.index, dtype="datetime64[ns, UTC]")

    # Deltas calculados sobre as séries inteiras (tz consistente), mascarados depois.
    dias_ate_conclusao = (dt_concl - dt_fim).dt.days           # concluída vs prazo
    dias_ate_hoje = (hoje - dt_fim).dt.days                     # prazo vencido vs hoje

    tem_prazo = dt_fim.notna()
    mask_concl = tem_prazo & dt_concl.notna()
    mask_aberta = tem_prazo & dt_concl.isna()

    # Construção funcional com .mask() (sem atribuição encadeada — pandas 3.0 safe)
    atraso = pd.Series(pd.NA, index=resultado.index, dtype="Float64")
    atraso = atraso.mask(mask_concl, dias_ate_conclusao)
    atraso = atraso.mask(mask_aberta, dias_ate_hoje)

    # Atraso nunca é negativo (adiantamento não conta como atraso).
    # .assign retorna novo DataFrame (evita ChainedAssignmentError do pandas 3.0).
    return resultado.assign(dias_atraso=atraso.clip(lower=0))


def _convenio_concluido(situacao) -> bool:
    """Convênio com desfecho de conclusão (prestação de contas), não anulado/cancelado."""
    s = _sem_acento_lower(situacao)
    if any(t in s for t in ("anulad", "cancelad", "rescis")):
        return False
    return any(t in s for t in ("conclu", "aprovad", "prestacao de contas"))


def _sem_acento_lower(texto) -> str:
    if not isinstance(texto, str):
        return ""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def _enriquecer_aditivos_federais(
    df: pd.DataFrame, raw_aditivos_federais: Optional[pd.DataFrame]
) -> pd.DataFrame:
    """
    Enriquece obras (sobretudo legado) com aditivos federais (TransfereGov/SICONV),
    casando `num_licitacao == nr_convenio`:

    - `valor_aditivos`: preenche onde nulo (0 = só vigência é informativo, não nulo).
    - `qtd_aditivos`: propaga a contagem de termos aditivos do convênio.
    - Para convênios **concluídos**: usa fim-de-vigência como proxy de `data_conclusao`
      e o fim-original como `data_prevista_fim` (preenche/atualiza quando há a data
      original), habilitando `dias_atraso` DIÁRIO real (final − original).
    """
    if (raw_aditivos_federais is None or raw_aditivos_federais.empty
            or "num_licitacao" not in df.columns
            or "nr_convenio" not in raw_aditivos_federais.columns):
        return df

    fed = raw_aditivos_federais.dropna(subset=["nr_convenio"]).drop_duplicates("nr_convenio")
    fed = fed.assign(_nr=fed["nr_convenio"].astype(str))
    chave = df["num_licitacao"].astype(str)

    def _col_map(coluna):
        if coluna not in fed.columns:
            return pd.Series(pd.NA, index=df.index)
        return chave.map(dict(zip(fed["_nr"], fed[coluna])))

    resultado = df.copy()

    # valor_aditivos: preenche onde nulo
    va = pd.to_numeric(resultado.get("valor_aditivos"), errors="coerce")
    resultado["valor_aditivos"] = va.where(va.notna(), pd.to_numeric(_col_map("valor_aditivos"), errors="coerce"))

    # qtd_aditivos: propaga (cria a coluna se não existir)
    qtd = pd.to_numeric(_col_map("qtd_aditivos"), errors="coerce")
    if "qtd_aditivos" in resultado.columns:
        base = pd.to_numeric(resultado["qtd_aditivos"], errors="coerce")
        resultado["qtd_aditivos"] = base.where(base.notna(), qtd)
    else:
        resultado["qtd_aditivos"] = qtd

    # datas (só convênios concluídos): conclusão = fim vigência; prazo = fim original.
    # Normaliza p/ ISO-UTC: a coluna já é tz-aware (painel/SISMOB); injetar date-only
    # ("2019-06-30") deixaria a coluna mista e quebraria pd.to_datetime(utc=True) → NaT.
    def _iso_utc(serie):
        return serie.map(lambda d: f"{d}T00:00:00+00:00"
                         if isinstance(d, str) and len(d) == 10 else d)
    concluido = _col_map("situacao").apply(_convenio_concluido)
    fim = _iso_utc(_col_map("data_fim_vigencia"))
    orig = _iso_utc(_col_map("data_fim_vigencia_original"))
    if "data_conclusao" in resultado.columns:
        dc = resultado["data_conclusao"]
        resultado["data_conclusao"] = dc.where(~(concluido & dc.isna() & fim.notna()), fim)
        # prazo original (mais fiel que a vigência final do painel) p/ atraso real
        dpf = resultado.get("data_prevista_fim", pd.Series(pd.NA, index=resultado.index))
        resultado["data_prevista_fim"] = dpf.where(~(concluido & orig.notna()), orig)

    return resultado


def transformar_obras(
    raw_contratos: pd.DataFrame,
    raw_obras_atual: pd.DataFrame,
    raw_obras_legado: pd.DataFrame,
    raw_obras_saude: pd.DataFrame,
    raw_obras_georef: pd.DataFrame,
    raw_obras_paralisadas: pd.DataFrame,
    raw_aditivos_federais: Optional[pd.DataFrame] = None,
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

    # enriquece valor_aditivos do legado com aditivos federais (TransfereGov/SICONV)
    df = _enriquecer_aditivos_federais(df, raw_aditivos_federais)

    # geocoding: preenche lat/long de obras sem coordenadas que têm bairro/endereço
    # (contratos), antes de gerar a geometry. Falha graciosa se Nominatim indisponível.
    try:
        from etl.geocoding import geocodificar_dataframe
        df = geocodificar_dataframe(df)
    except Exception as exc:
        log.warning("geocoding pulado: %s", exc)

    # geometry — LONGITUDE primeiro (padrão WKT PostGIS)
    if "latitude" in df.columns and "longitude" in df.columns:
        df = df.assign(geometry=df.apply(
            lambda row: _gerar_geometry(row.get("latitude"), row.get("longitude")),
            axis=1,
        ))

    # garantir municipio/uf e nome (NOT NULL no banco)
    df = df.assign(
        municipio=df["municipio"].fillna("Macaé"),
        uf=df["uf"].fillna("RJ"),
        nome=df["nome"].fillna(df.get("objeto", pd.Series(dtype=str))).fillna(df["id_origem"]),
        situacao=df["situacao"].fillna("Indefinido"),
    )

    # fallback centralizado: aplica regras de percentual para todas as fontes
    df = _ajustar_percentual(df)

    # dias_atraso derivado das datas (a fonte nunca publica esse campo)
    df = _calcular_dias_atraso(df)

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

    # Fallback: inferir situação pela data de fim quando a fonte não publica
    sit_raw = _get(df, "situacao")
    data_fim_raw = pd.to_datetime(_get(df, "data_fim_vigencia"), utc=True, errors="coerce")
    hoje = pd.Timestamp.now(tz="UTC")
    sit_inferida = sit_raw.copy().astype(object)
    ausente = sit_raw.isna() | (sit_raw.astype(str).str.lower().isin({"", "nan", "none"}))
    sit_inferida.loc[ausente & data_fim_raw.notna() & (data_fim_raw < hoje)] = "Expirado"
    sit_inferida.loc[ausente & data_fim_raw.notna() & (data_fim_raw >= hoje)] = "Vigente"
    sit_inferida.loc[ausente & data_fim_raw.isna()] = "Indefinido"
    resultado["situacao"] = sit_inferida
    resultado["valor_inicial"] = _get(df, "valor_inicial")
    resultado["valor_global"] = _get(df, "valor_global")
    resultado["valor_aditivos"] = _get(df, "valor_aditivos")
    resultado["data_assinatura"] = _get(df, "data_assinatura")
    resultado["data_inicio"] = _get(df, "data_inicio_vigencia")
    resultado["data_fim"] = _get(df, "data_fim_vigencia")
    qtd = pd.to_numeric(_get(df, "qtd_aditivos"), errors="coerce").fillna(0)
    resultado["qtd_aditivos"] = qtd
    resultado["fonte_origem"] = _get(df, "fonte")

    resultado = resultado.drop_duplicates(subset=["numero", "fonte_origem"], keep="last")
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
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and value.is_integer():
        return int(value)
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
    raw_aditivos_federais = ler_raw(client, "raw_aditivos_federais")

    resultado = {"fornecedores": 0, "obras": 0, "contratos": 0, "aditivos": 0}

    # 1. Fornecedores
    try:
        fornecedores_df = transformar_fornecedores(raw_contratos)
        resultado["fornecedores"] = upsert(client, "fornecedores", fornecedores_df, ["cnpj"])
        # reler para obter o id (UUID) gerado pelo banco — necessário para FK em contratos
        fornecedores_df = ler_raw(client, "fornecedores")
    except Exception as exc:
        log.error("run: etapa fornecedores falhou: %s", exc)
        fornecedores_df = pd.DataFrame()

    # 2. Obras
    try:
        obras_df = transformar_obras(
            raw_contratos, raw_obras_atual, raw_obras_legado,
            raw_obras_saude, raw_obras_georef, raw_obras_paralisadas,
            raw_aditivos_federais,
        )
        resultado["obras"] = upsert(client, "obras", obras_df, ["fonte_origem", "id_origem"])
        # reler para obter o id (UUID) gerado pelo banco — necessário para FK em contratos
        obras_df = ler_raw(client, "obras")
    except Exception as exc:
        log.error("run: etapa obras falhou: %s", exc)
        obras_df = pd.DataFrame()

    # 3. Contratos
    try:
        contratos_df = transformar_contratos(raw_contratos, obras_df, fornecedores_df)
        resultado["contratos"] = upsert(client, "contratos", contratos_df, ["numero", "fonte_origem"])
        # reler para obter o id (UUID) gerado pelo banco — necessário para FK em aditivos
        contratos_df = ler_raw(client, "contratos")
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
