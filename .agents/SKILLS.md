---
name: dev-best-practices
description: Apply software craftsmanship to any code change in this repo — Clean Code, TDD (red-green-refactor), pragmatic design patterns, and Conventional Commits. Use whenever writing, refactoring, or reviewing Python code, adding tests, or composing commit messages.
---

# Boas práticas de desenvolvimento

Guia operacional para escrever, refatorar e commitar código neste projeto.
Não é um livro-texto: são regras acionáveis a aplicar em cada mudança.

## 1. TDD — Red, Green, Refactor

Para toda lógica nova ou alterada que seja testável (funções puras,
transformações, regras de negócio):

1. **Red** — escreva primeiro um teste que falha e expressa o comportamento
   desejado. Um teste por comportamento, não por método.
2. **Green** — escreva o mínimo de código para o teste passar.
3. **Refactor** — limpe o código mantendo os testes verdes.

Regras:
- Nome do teste descreve o cenário: `test_<unidade>_<condição>_<resultado>`.
- Arrange-Act-Assert visível em cada teste (separe as três fases).
- Cubra: caminho feliz, borda (zero, vazio, nulo), e erro/aviso.
- Não testar detalhes de implementação — testar comportamento observável.
- Testes determinísticos: fixe `random_state`, evite I/O real (use mocks para
  Supabase, como em `tests/test_data_loader.py`).
- Rode `pytest tests/ -q` antes de considerar a tarefa pronta.

## 2. Clean Code

- **Nomes revelam intenção**: `df_treino`, `separar_grupos` — não `d`, `proc`.
  Mantenha o domínio em português, como o resto do repo.
- **Funções pequenas e com um propósito**. Se precisa de "e" para descrever o
  que faz, divida.
- **Sem números mágicos**: extraia constantes nomeadas
  (ex.: `AUC_ROC_MINIMO = 0.75`, `RISCO_ALTO = 0.6`).
- **Comentários explicam o porquê, não o quê**. Código legível dispensa
  comentário do óbvio. Docstrings curtas descrevem contrato (args/retorno).
- **Não repita (DRY)**, mas evite abstração prematura: 3 linhas parecidas são
  melhores que uma abstração errada.
- **Falhe cedo e claro**: valide nas fronteiras (entrada do usuário, I/O
  externo), confie no código interno. Levante exceções com mensagem útil.
- **Imutabilidade defensiva**: funções de transformação não mutam o DataFrame
  recebido — `df = df.copy()` no início (padrão já usado em `build_features`).
- **Type hints sempre** em assinaturas públicas. Respeite ruff (E,F,W,I) e
  `line-length = 100`.

## 3. Design Patterns (pragmáticos)

Use o padrão que reduz acoplamento, nunca por enfeite.

- **Fit/Transform (Strategy de parâmetros)**: para transformações que aprendem
  parâmetros (z-score, imputação), separe o *ajuste* (calculado só no treino) da
  *aplicação* (treino e predição). Evita vazamento de dados (data leakage).
- **Pure functions / Pipeline**: encadeie passos puros (como a lista `steps` em
  `build_features`). Cada passo recebe e devolve o DataFrame.
- **Factory**: centralize a construção de objetos complexos (ex.: `_build_pipeline`
  para o `Pipeline` do XGBoost).
- **Single Source of Truth**: constantes compartilhadas (`FEATURE_COLS`,
  conjuntos de situações, thresholds) vivem em um único lugar e são importadas.
- **Dependency Injection**: receba o cliente/recurso externo como argumento
  (como `client: Client`) para permitir mock nos testes.

## 4. Conventional Commits

Formato: `<tipo>(<escopo>): <descrição imperativa em minúsculas>`

Tipos: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`, `build`, `ci`.

Regras:
- Descrição no imperativo, ≤ 72 chars, sem ponto final.
- Escopo opcional reflete a área: `data`, `features`, `models`, `pipeline`,
  `eval`, `ci`. Segue o histórico do repo (ex.: `feat(features): ...`).
- Corpo (opcional) explica o **porquê**, não o como.
- Um commit = uma mudança coesa. Não misture refactor com feature.
- `BREAKING CHANGE:` no rodapé se quebrar contrato público.

Exemplos:
- `feat(data): adiciona separar_grupos para split treino/predição`
- `fix(features): evita vazamento ao normalizar valor com escala do treino`
- `test(models): cobre retorno completo de métricas do modelo de atraso`
- `ci(ml): eleva limiar de AUC-ROC para 0.75 e adiciona cron semanal`

## 5. Checklist antes de finalizar

- [ ] Teste novo/alterado falha sem a mudança e passa com ela.
- [ ] `pytest tests/ -q` verde.
- [ ] `ruff check src/ scripts/ tests/` e `ruff format --check` limpos.
- [ ] Sem número mágico, sem nome obscuro, sem comentário redundante.
- [ ] Sem vazamento de dados entre treino e predição.
- [ ] Commit no formato Conventional Commits, coeso.
