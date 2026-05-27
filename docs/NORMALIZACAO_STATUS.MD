# Normalização de Status de Obras — DUOPEN 2026

Documentação oficial dos valores aceitos para o campo `situacao`
na tabela `obras` e das regras de normalização aplicadas pelo
`etl/cleaner.py`.

---

## Status oficiais

O campo `situacao` na tabela `obras` aceita exatamente estes valores:

| Status | Descrição |
|---|---|
| `Em andamento` | Obra em execução física no momento |
| `Concluída` | Obra entregue e finalizada formalmente |
| `Paralisada` | Obra que estava em andamento e teve execução interrompida |
| `Em fase de planejamento` | Obra planejada, cadastrada ou ainda não iniciada |
| `Cancelada` | Obra cancelada formalmente pela administração |
| `Rescindida` | Contrato rescindido antes da conclusão da obra |
| `Indefinido` | Status nulo, vazio ou undefined na fonte de origem |
| *(valor original)* | Status recebido da fonte mas não mapeado — gravado como veio |

---

## Regras de normalização

### Regra 1 — Status NULL ou vazio → `"Indefinido"`

Quando o campo de status vier nulo, vazio, `"undefined"`, `"null"`
ou qualquer variação desses valores, normalizar para `"Indefinido"`.

**Motivação:** `NULL` não significa que a obra está paralisada —
significa que a fonte não informou o status. Misturar com
`"Paralisada"` distorceria as métricas do ML e os indicadores
do dashboard.

```python
VALORES_INDEFINIDO = {
    None, "", "null", "none", "undefined",
    "n/a", "na", "não informado", "nao informado",
    "-", "--", "s/i", "sem informação",
}

# Se situacao.strip().lower() in VALORES_INDEFINIDO → "Indefinido"
```

---

### Regra 2 — Status mapeado → valor normalizado

Aplicar `.strip().lower()` no valor recebido e buscar no mapa.
Se encontrar → substituir pelo valor normalizado.

```python
SITUACAO_MAP = {

    # ── Em andamento ─────────────────────────────────────────
    "em andamento":             "Em andamento",
    "em execução":              "Em andamento",
    "em execucao":              "Em andamento",
    "em obras":                 "Em andamento",
    "em execução física":       "Em andamento",
    "iniciada":                 "Em andamento",
    "vigente":                  "Em andamento",
    "em execução (em obras)":   "Em andamento",
    "contrato vigente":         "Em andamento",

    # ── Concluída ─────────────────────────────────────────────
    "concluída":                "Concluída",
    "concluida":                "Concluída",
    "finalizada":               "Concluída",
    "encerrada":                "Concluída",
    "entregue":                 "Concluída",
    "obra concluída":           "Concluída",
    "concluído":                "Concluída",
    "concluido":                "Concluída",

    # ── Paralisada ────────────────────────────────────────────
    "paralisada":               "Paralisada",
    "paralisado":               "Paralisada",
    "suspensa":                 "Paralisada",
    "suspensa/paralisada":      "Paralisada",
    "obra paralisada":          "Paralisada",
    "interrompida":             "Paralisada",
    "parada":                   "Paralisada",

    # ── Em fase de planejamento ───────────────────────────────
    # ⚠️ "Planejada" e "Cadastrada/Cadastrado" são normalizados
    #    para "Em fase de planejamento"
    "planejada":                "Em fase de planejamento",
    "planejado":                "Em fase de planejamento",
    "a iniciar":                "Em fase de planejamento",
    "não iniciada":             "Em fase de planejamento",
    "nao iniciada":             "Em fase de planejamento",
    "não iniciado":             "Em fase de planejamento",
    "nao iniciado":             "Em fase de planejamento",
    "licitação":                "Em fase de planejamento",
    "licitacao":                "Em fase de planejamento",
    "em licitação":             "Em fase de planejamento",
    "em licitacao":             "Em fase de planejamento",
    "projeto":                  "Em fase de planejamento",
    "em projeto":               "Em fase de planejamento",
    "cadastrada":               "Em fase de planejamento",
    "cadastrado":               "Em fase de planejamento",
    "em cadastro":              "Em fase de planejamento",
    "habilitada":               "Em fase de planejamento",
    "habilitado":               "Em fase de planejamento",
    "proposta":                 "Em fase de planejamento",

    # ── Cancelada ─────────────────────────────────────────────
    "cancelada":                "Cancelada",
    "cancelado":                "Cancelada",
    "obra cancelada":           "Cancelada",

    # ── Rescindida ────────────────────────────────────────────
    "rescindida":               "Rescindida",
    "rescindido":               "Rescindida",
    "contrato rescindido":      "Rescindida",
    "rescisão":                 "Rescindida",
    "rescisao":                 "Rescindida",
}
```

---

### Regra 3 — Status não mapeado → gravar o valor original

Se o valor recebido **não estiver** no `SITUACAO_MAP` e **não for**
nulo/vazio, gravar exatamente como veio da fonte.

**Motivação:** fontes externas podem introduzir novos status a
qualquer momento. Descartar ou forçar um valor arbitrário perderia
informação. Gravar o valor original permite que:
- O gestor visualize no frontend que existe um status desconhecido
- A equipe identifique novos valores para adicionar ao mapa
- O dado não seja perdido

```python
def normalize_situacao(valor: str) -> str:
    if not valor or str(valor).strip().lower() in VALORES_INDEFINIDO:
        return "Indefinido"

    valor_lower = str(valor).strip().lower()

    if valor_lower in SITUACAO_MAP:
        return SITUACAO_MAP[valor_lower]

    # Não mapeado → gravar original com strip
    # Logar em WARNING para que a equipe saiba que existe
    # um novo valor não mapeado
    log.warning(f"Status não mapeado — gravando original: '{valor}'")
    return str(valor).strip()
```

---

## Comportamento por caso

| Valor recebido da fonte | Resultado em `obras.situacao` | Regra |
|---|---|---|
| `"em execução"` | `"Em andamento"` | Regra 2 |
| `"Em Execução"` | `"Em andamento"` | Regra 2 (case insensitive) |
| `"concluida"` | `"Concluída"` | Regra 2 |
| `"Planejada"` | `"Em fase de planejamento"` | Regra 2 |
| `"Cadastrada"` | `"Em fase de planejamento"` | Regra 2 |
| `"cadastrado"` | `"Em fase de planejamento"` | Regra 2 |
| `"paralisada"` | `"Paralisada"` | Regra 2 |
| `"Suspensa"` | `"Paralisada"` | Regra 2 |
| `"cancelada"` | `"Cancelada"` | Regra 2 |
| `"rescindida"` | `"Rescindida"` | Regra 2 |
| `NULL` | `"Indefinido"` | Regra 1 |
| `""` | `"Indefinido"` | Regra 1 |
| `"undefined"` | `"Indefinido"` | Regra 1 |
| `"Não informado"` | `"Indefinido"` | Regra 1 |
| `"Em vistoria"` | `"Em vistoria"` | Regra 3 — gravar original |
| `"Aguardando licença"` | `"Aguardando licença"` | Regra 3 — gravar original |

---

## Impacto no ML

O campo `situacao` influencia diretamente o feature engineering
do `duopen-ml`:

- `"Paralisada"` → feature `tem_obra_paralisada_similar = True`
  para outras obras do mesmo fornecedor ou secretaria
- `"Indefinido"` → **não alimenta** o treinamento do XGBoost
  (excluir do dataset de treino — dado incompleto)
- *(valor original não mapeado)* → tratar como `"Indefinido"`
  para fins de ML até ser categorizado

---

## Impacto no dashboard (frontend React)

| situacao | Badge | Cor |
|---|---|---|
| `Em andamento` | Em andamento | Azul |
| `Concluída` | Concluída | Verde |
| `Paralisada` | Paralisada | Vermelho |
| `Em fase de planejamento` | Planejamento | Cinza |
| `Cancelada` | Cancelada | Laranja |
| `Rescindida` | Rescindida | Laranja escuro |
| `Indefinido` | Indefinido | Cinza claro |
| *(valor original)* | *(valor original)* | Cinza claro |

---

## Onde esta normalização é aplicada

| Arquivo | Como usa |
|---|---|
| `etl/cleaner.py` | Função `normalize_situacao()` — aplica as 3 regras |
| `etl/transformer.py` | `SITUACAO_SAUDE_MAP` e `SITUACAO_GEOREF_MAP` mapeiam para os valores oficiais antes de chamar `normalize_situacao()` |
| `duopen-infra/docs/business_rules.md` | Referência de regras de negócio |
| `duopen-frontend` | Badge colors no dashboard React |

---

## Como adicionar um novo status

Quando um status não mapeado aparecer nos logs do pipeline
(`WARNING: Status não mapeado`):

1. Identificar a fonte e o valor exato recebido
2. Decidir para qual status oficial ele deve ser normalizado
3. Adicionar no `SITUACAO_MAP` do `etl/cleaner.py`
4. Atualizar esta documentação
5. Criar PR no `duopen-coleta` com a mudança

---

*DUOPEN 2026 · Referência: etl/cleaner.py → normalize_situacao()*