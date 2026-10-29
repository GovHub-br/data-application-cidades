# B10 · `tests/` sem `conftest.py`

> Parte do [guia das refatorações](README.md) · Prioridade P1 · Status: issue aberta · Issue: [#257](https://github.com/GovHub-br/data-application-cidades/issues/257) · PR: —

## Problema

- **Cada módulo monta o ambiente sozinho.** Todos fazem `sys.path.insert` ou carregam scripts com `importlib.util.spec_from_file_location`.
- **Setup de env duplicado.** A preparação das variáveis de ambiente está repetida em `test_mascaramento_mdb.py:19-31` e `test_mascaramento_cadunico.py`.
- **`sys.modules` editado à mão** nos `test_superset_*`.
- **Arquivos sem valor:**
  - `test_foo.py` é placeholder (`assert True`);
  - `tests/ingestion/` só tem `__pycache__` de testes apagados.
- **Testes que não rodam.** Os cerca de 22 testes de `data-science/` ficam fora da CI e do Makefile.
- **O `.env` vaza para os testes.** O `dbt-core` faz `load_dotenv()` no import, e por isso as variáveis precisam ser limpas por fixture autouse.

## Correção proposta

- Um `tests/conftest.py` com o `PYTHONPATH` do Airflow (`dags`, `plugins`, `helpers`, `scripts`), a limpeza de env e as fixtures de env compartilhadas.
- Apagar o `test_foo.py`.
- Decidir se os testes de `data-science/` entram na CI.

## Critério de aceite

- Nenhum módulo de teste mexe em `sys.path`.
- `pytest tests` passa do mesmo jeito que antes.

## Relação

Vai junto com a [F4](f4-uv.md) e o [B6](b06-ci-blocante.md). O teste de DagBag do B6 usa este `conftest.py`.
