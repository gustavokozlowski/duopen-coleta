import pandas as pd
import pytest
import requests

from scrappers.federal import sismob

pytestmark = pytest.mark.unit


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


def _http_error(status_code: int) -> requests.exceptions.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    return requests.exceptions.HTTPError(f"HTTP {status_code}", response=response)


def test_get_retorna_json_com_sucesso(monkeypatch):
    called = {}

    def fake_get(url, headers, params, timeout):
        called["url"] = url
        called["headers"] = headers
        called["params"] = params
        called["timeout"] = timeout
        return DummyResponse(status_code=200, payload={"ok": True})

    monkeypatch.setattr(sismob.requests, "get", fake_get)

    out = sismob._get("/obras", {"page": 0})

    assert out == {"ok": True}
    assert called["url"].endswith("/obras")
    assert called["params"] == {"page": 0}
    assert called["timeout"] == sismob.REQUEST_TIMEOUT


def test_get_lanca_http_error(monkeypatch):
    monkeypatch.setattr(
        sismob.requests,
        "get",
        lambda *a, **k: DummyResponse(status_code=500, http_error=_http_error(500)),
    )

    with pytest.raises(requests.exceptions.HTTPError):
        sismob._get("/obras", {})


def test_get_retry_ate_runtime_error_em_timeout(monkeypatch):
    calls = {"n": 0}
    sleeps = []

    def fake_get(*args, **kwargs):
        calls["n"] += 1
        raise requests.exceptions.Timeout("timeout")

    monkeypatch.setattr(sismob.requests, "get", fake_get)
    monkeypatch.setattr(sismob.time, "sleep", lambda s: sleeps.append(s))

    with pytest.raises(RuntimeError):
        sismob._get("/obras", {})

    assert calls["n"] == sismob.RETRY_ATTEMPTS
    assert sleeps == [2.0, 4.0]


def test_listar_obras_paginas_e_params(monkeypatch):
    pages = [
        {
            "content": [{"propostaId": 1}],
            "totalPages": 2,
            "totalElements": 2,
            "last": False,
        },
        {
            "content": [{"propostaId": 2}],
            "totalPages": 2,
            "totalElements": 2,
            "last": True,
        },
    ]
    calls = []

    def fake_get(path, params=None):
        calls.append((path, params))
        return pages.pop(0)

    monkeypatch.setattr(sismob, "_get", fake_get)
    monkeypatch.setattr(sismob.time, "sleep", lambda *_: None)

    out = sismob.listar_obras()

    assert [o["propostaId"] for o in out] == [1, 2]
    assert len(calls) == 2
    assert calls[0][0] == "/obras"
    assert calls[0][1]["page"] == 0
    assert calls[1][1]["page"] == 1
    assert calls[0][1]["municipioIbge"] == sismob.MUNICIPIO_IBGE
    assert calls[0][1]["ufIbge"] == sismob.UF_IBGE


def test_buscar_detalhe_retorna_none_em_404(monkeypatch):
    monkeypatch.setattr(
        sismob,
        "_get",
        lambda *_: (_ for _ in ()).throw(_http_error(404)),
    )

    out = sismob.buscar_detalhe(123)

    assert out is None


def test_buscar_todos_detalhes_mescla_e_fallback(monkeypatch):
    obras = [
        {"propostaId": 10, "situacaoObra": "resumo"},
        {"propostaId": 20, "tipoObra": "UPA"},
        {"sem": "id"},
    ]

    def fake_buscar_detalhe(pid):
        if pid == 10:
            return {"propostaId": 10, "situacaoObra": "detalhe", "extra": "ok"}
        return None

    monkeypatch.setattr(sismob, "buscar_detalhe", fake_buscar_detalhe)
    monkeypatch.setattr(sismob.time, "sleep", lambda *_: None)

    out = sismob.buscar_todos_detalhes(obras)

    assert len(out) == 2
    assert out[0]["situacaoObra"] == "detalhe"
    assert out[0]["extra"] == "ok"
    assert out[1] == {"propostaId": 20, "tipoObra": "UPA"}


def test_run_sucesso(monkeypatch):
    monkeypatch.setattr(sismob, "listar_obras", lambda: [{"propostaId": 1}])
    monkeypatch.setattr(sismob, "buscar_todos_detalhes", lambda obras: obras)

    saved = []
    monkeypatch.setattr(sismob, "_salvar_cache", lambda dados: saved.append(dados))
    monkeypatch.setattr(
        sismob,
        "normalizar",
        lambda registros: pd.DataFrame([{"proposta_id": registros[0]["propostaId"]}]),
    )

    out = sismob.run()

    assert len(out) == 1
    assert out.iloc[0]["proposta_id"] == 1
    assert saved == [[{"propostaId": 1}]]


def test_run_fallback_para_cache(monkeypatch):
    monkeypatch.setattr(
        sismob,
        "listar_obras",
        lambda: (_ for _ in ()).throw(RuntimeError("api indisponivel")),
    )
    monkeypatch.setattr(sismob, "_carregar_cache", lambda: [{"propostaId": 9}])
    monkeypatch.setattr(
        sismob,
        "normalizar",
        lambda registros: pd.DataFrame([{"proposta_id": registros[0]["propostaId"]}]),
    )

    out = sismob.run()

    assert len(out) == 1
    assert out.iloc[0]["proposta_id"] == 9


def test_run_retorna_vazio_quando_cache_tambem_vazio(monkeypatch):
    monkeypatch.setattr(
        sismob,
        "listar_obras",
        lambda: (_ for _ in ()).throw(RuntimeError("api indisponivel")),
    )
    monkeypatch.setattr(sismob, "_carregar_cache", lambda: [])

    out = sismob.run()

    assert out.empty is True


def test_normalizar_retorna_dataframe_com_campos():
    registros = [
        {
            "propostaId": 1,
            "situacaoObra": "Em andamento",
            "tipoObra": "UPA",
            "dtInicioProjeto": "2024-01-10",
            "nuLatitude": "-22.37",
            "nuLongitude": "-41.78",
        }
    ]

    out = sismob.normalizar(registros)

    assert len(out) == 1
    assert out.iloc[0]["proposta_id"] == 1
    assert out.iloc[0]["tipo_obra"] == "UPA"
    assert out.iloc[0]["dt_inicio_projeto"] == "2024-01-10T00:00:00+00:00"
    assert out.iloc[0]["latitude"] == -22.37


def test_normalizar_retorna_vazio_quando_lista_vazia():
    out = sismob.normalizar([])

    assert out.empty is True


def test_normalizar_cnes_usa_nucnes_quando_cocnes_ausente():
    """Bug fix: obras novas usam nuCnes, não coCnes — cnes não pode ficar None."""
    registros = [{"propostaId": 1, "nuCnes": "6189954", "coCnes": None, "cnes": None}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["cnes"] == "6189954"


def test_normalizar_cnes_usa_cocnes_quando_nucnes_ausente():
    """Obras legadas usam coCnes — fallback deve funcionar."""
    registros = [{"propostaId": 2, "nuCnes": None, "coCnes": "2276712", "cnes": None}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["cnes"] == "2276712"


def test_normalizar_cnes_prefere_cnes_direto():
    """Quando os três estão preenchidos, cnes direto tem prioridade."""
    registros = [{"propostaId": 3, "cnes": "1111111", "nuCnes": "2222222", "coCnes": "3333333"}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["cnes"] == "1111111"


def test_normalizar_tipo_recurso_filtro_mapeado():
    """dsTipoRecursoFiltro deve ser mapeado para tipo_recurso_filtro."""
    registros = [{"propostaId": 1, "dsTipoRecursoFiltro": "emenda"}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["tipo_recurso_filtro"] == "emenda"


def test_normalizar_porte_programa_mapeado():
    """dsPortePrograma deve ser mapeado para porte_programa."""
    registros = [{"propostaId": 1, "dsPortePrograma": "Porte III"}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["porte_programa"] == "Porte III"


def test_normalizar_possui_etapa_funcionamento_mapeado():
    """stPossuiEtapaFuncionamento deve ser mapeado."""
    registros = [{"propostaId": 1, "stPossuiEtapaFuncionamento": True}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["possui_etapa_funcionamento"] == True  # noqa: E712 — numpy.bool_ não passa em `is`


def test_normalizar_forma_execucao_projeto_mapeada():
    """dsTipoFormaExecucaoProjeto deve ser mapeado para forma_execucao_projeto."""
    registros = [{"propostaId": 1, "dsTipoFormaExecucaoProjeto": "Elaboração com recursos próprios"}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["forma_execucao_projeto"] == "Elaboração com recursos próprios"


def test_normalizar_dt_prevista_inauguracao_mapeada():
    """dtPrevistaInauguracao deve ser mapeado para dt_prevista_inauguracao."""
    registros = [{"propostaId": 1, "dtPrevistaInauguracao": "2014-12-30"}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["dt_prevista_inauguracao"] == "2014-12-30T00:00:00+00:00"


def test_fotos_grupos_achata_estrutura():
    """_fotos_grupos deve retornar lista plana com grupo, foto_id e dt_atualizacao."""
    grupos = [
        {
            "noGrupo": "Fachada",
            "fotos": [
                {"id": "uuid-1", "dtAtualizacao": "2024-01-01T00:00:00.000+0000"},
                {"id": "uuid-2", "dtAtualizacao": "2024-01-02T00:00:00.000+0000"},
            ],
        },
        {
            "noGrupo": "Terreno",
            "fotos": [
                {"id": "uuid-3", "dtAtualizacao": "2024-01-03T00:00:00.000+0000"},
            ],
        },
    ]
    resultado = sismob._fotos_grupos(grupos)
    assert len(resultado) == 3
    assert resultado[0] == {"grupo": "Fachada", "foto_id": "uuid-1", "dt_atualizacao": "2024-01-01T00:00:00.000+0000"}
    assert resultado[2]["grupo"] == "Terreno"
    assert resultado[2]["foto_id"] == "uuid-3"


def test_fotos_grupos_vazio_retorna_lista_vazia():
    """_fotos_grupos com lista vazia deve retornar []."""
    assert sismob._fotos_grupos([]) == []
    assert sismob._fotos_grupos(None) == []


def test_normalizar_fotos_grupos_serializado_como_json():
    """fotos_grupos deve ser string JSON com lista de fotos achatadas."""
    import json
    registros = [
        {
            "propostaId": 1,
            "gruposFotografias": [
                {
                    "noGrupo": "Placa da obra",
                    "fotos": [{"id": "abc-123", "dtAtualizacao": "2024-06-01T00:00:00.000+0000"}],
                }
            ],
        }
    ]
    out = sismob.normalizar(registros)
    fotos = json.loads(out.iloc[0]["fotos_grupos"])
    assert len(fotos) == 1
    assert fotos[0]["grupo"] == "Placa da obra"
    assert fotos[0]["foto_id"] == "abc-123"


def test_normalizar_fotos_grupos_vazio_quando_sem_fotos():
    """fotos_grupos deve ser '[]' quando não há gruposFotografias."""
    registros = [{"propostaId": 1}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["fotos_grupos"] == "[]"


def test_normalizar_dt_inicio_obra_fallback_para_ordem_servico():
    """dt_inicio_obra deve usar dtOrdemServico quando dtInicioObra é None."""
    registros = [{"propostaId": 1, "dtInicioObra": None, "dtOrdemServico": "2013-08-20"}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["dt_inicio_obra"] == "2013-08-20T00:00:00+00:00"


def test_normalizar_dt_inicio_obra_prefere_dtinicio_obra():
    """Quando dtInicioObra existe, não deve ser substituído por dtOrdemServico."""
    registros = [{"propostaId": 1, "dtInicioObra": "2015-01-01", "dtOrdemServico": "2014-06-01"}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["dt_inicio_obra"] == "2015-01-01T00:00:00+00:00"


def test_normalizar_valor_total_contrato_fallback_para_proposta():
    """valor_total_contrato deve usar vlProposta quando vlTotalContrato é None."""
    registros = [{"propostaId": 1, "vlTotalContrato": None, "vlProposta": 773000.0}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["valor_total_contrato"] == 773000.0


def test_normalizar_valor_total_contrato_prefere_total_contrato():
    """Quando vlTotalContrato existe, não deve ser substituído por vlProposta."""
    registros = [{"propostaId": 1, "vlTotalContrato": 90693.2, "vlProposta": 90750.0}]
    out = sismob.normalizar(registros)
    assert out.iloc[0]["valor_total_contrato"] == 90693.2