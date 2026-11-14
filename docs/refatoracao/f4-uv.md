# F4 · Troca do poetry pelo uv

> Parte do [guia das refatorações](README.md) · Frente prevista · Status: issue aberta · Issue: [#246](https://github.com/GovHub-br/data-application-cidades/issues/246) · PR: —

## Objetivo

Trocar o poetry pelo uv e, no caminho, criar uma fonte de verdade única para as dependências, com lock versionado.

## Arquivos que mudam

| Arquivo | O que muda |
|---|---|
| `pyproject.toml` | Sai `[tool.poetry]`, muda o `[build-system]`, a descrição deixa de dizer "IPEA" |
| `.gitignore:10` | O `**/*.lock` ignoraria o `uv.lock`; é por isso que o `poetry.lock` nunca foi versionado |
| `Makefile` | Todo `poetry run`, mais o `poetry lock` que roda a cada `make setup` |
| `.github/actions/setup-poetry/` e `.github/workflows/main.yaml:19-60` | Setup e cache da CI |
| `.github/dependabot.yml:7` | Ecossistema do pip/poetry |
| `tests/test_dependencias_sincronizadas.py:31-39` | Lê `tool.poetry.dependencies` |
| `README.md:63-72` | Instruções de instalação |
| `infra/docker/airflow/Dockerfile`, `compile-lock.sh` | Já usam uv; só conferir |

## O que resolver junto

- **Uma fonte de verdade.** Hoje são quatro listas:
  - `pyproject.toml` (testes e CI);
  - `requirements.txt` (entrada do lock da imagem);
  - `infra/docker/airflow/requirements.lock.txt` (o que é instalado);
  - `requirements.generated.txt` (morto: Airflow 2.8.1, só local).
- **Drift do sqlglot:** os testes usam `^30` e a imagem usa `29.0.1`. A exceção `SO_FORA_DA_IMAGEM` esconde a diferença.
- **Imports diretos não declarados:** `yaml`, `psycopg2`, `paramiko`, `httpx`, `bs4`, `chardet`, `pytz` e `packaging`.
- **Grupo dev:**
  - cerca de 17 stubs `types-*` sem relação com o código (uWSGI, gevent, WTForms…);
  - tudo sem pin;
  - `jupyter` incluído.
- **Cache da CI:** a chave `hashFiles('**/poetry.lock')` é sempre vazia e o cache nunca renova.
- **Workspace:** `data-science/dados-historicos-tratamento` já é um projeto uv e pode entrar como membro.
- **Airflow 3.3.2 fixado em seis lugares:** o `Dockerfile`, o `requirements.txt`, o `pyproject.toml`, o `compile-lock.sh`, o `values.yaml` do k8s e o compose. Avaliar uma fonte só.

## Itens que vão junto

[B6](b06-ci-blocante.md) (CI blocante), [B7](b07-pre-commit.md) (pre-commit) e [B10](b10-conftest.md) (`conftest.py`).

## Critério de aceite

- `uv sync` reproduz o ambiente de testes e `uv.lock` está versionado.
- A CI usa o uv, com cache pela chave do lock.
- O teste de sincronia com a imagem lê o formato novo.
- Nenhum `poetry` sobra no repositório.
