# B6 · A CI não barra nada

> Parte do [guia das refatorações](README.md) · Prioridade P1 · Status: issue aberta · Issue: [#253](https://github.com/GovHub-br/data-application-cidades/issues/253) · PR: —

## Problema

- **Lint que nunca falha.** `.github/workflows/main.yaml:42` roda `make lint-ci || true`. O `lint-ci` (`Makefile:31-33`) só roda sqlfmt e sqlfluff, com `--ignore templating`.
- **Python sem checagem na CI.** black, ruff e mypy não rodam. O `ruff check .` acusa 950 erros: 660 em `data-science/` e 275 em `scripts/`.
- **dbt sem checagem na CI.** Não há `dbt parse`, `compile` nem `build`.
- **DAGs sem teste de import.** Não há teste de DagBag, que pegaria os imports de Airflow 2 e as leituras de env no parse.
- **Cobertura medida no repositório inteiro.** `--cov=.` (`main.yaml:60`) inclui `data-science/` e `docs-pages/`.

## Correção proposta

- Ruff blocante só em `dags/`, `plugins/`, `helpers/` e `scripts/`, com baseline nos erros existentes se preciso.
- Um passo de `dbt parse` sem banco.
- Um teste de DagBag em `tests/`.
- `--cov` restrito aos pacotes de código.

## Critério de aceite

Um PR com erro de lint, de parse do dbt ou de import de DAG fica vermelho.

## Relação

Vai junto com a [F4](f4-uv.md), [B7](b07-pre-commit.md) e [B10](b10-conftest.md).
