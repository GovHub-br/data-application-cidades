# AGENTS.md

Data pipeline (Medallion architecture) for Gov Hub BR / IPEA: Apache Airflow orchestrates ingestion of Brazilian public data (SIAPE, SIAFI, ComprasGov, TransfereGov, Siorg, …), dbt transforms it into a PostgreSQL warehouse, Superset/Jupyter for viz.

## Two separate toolchains

- **Repo root = Poetry** (poetry 1.8.5, Python 3.11). All Airflow/dbt/tests/lint run through `poetry run`.
- **`data-science/dados-historicos-tratamento/` = a standalone uv-managed sub-project** with its own `AGENTS.md`, `pyproject.toml`, and source layout. It is NOT part of the Airflow pipeline. Read its `AGENTS.md` before touching it — different commands (`uv run …`), different data.

## Commands (root)

- `make setup` — install poetry + deps, generate `requirements.generated.txt`, install git hooks
- `make format` — black + `ruff --fix` + sqlfmt (auto-formats; runs on pre-commit)
- `make lint` — black --check, ruff, mypy, sqlfmt --check, sqlfluff
- `make lint-ci` — sqlfmt --check + sqlfluff with `.sqlfluff.ci` (jinja templater, no DB needed)
- `make test` — `poetry run pytest tests`
- Single test: `poetry run pytest tests/test_foo.py`

## Import convention (critical)

DAGs import plugins/helpers as **flat top-level modules**, not namespaced:

```python
from schedule_loader import get_dynamic_schedule   # plugin
from postgres_helpers import get_postgres_conn     # helper
from cliente_siorg import ClienteSiorg             # plugin
```

This works because `PYTHONPATH`/`MYPYPATH` include `airflow_lappis`, `.../dags`, `.../plugins`, `.../helpers` (set in `Makefile` and `docker-compose.yml`). Do NOT write `from plugins.cliente_siorg import …` or `from airflow_lappis.plugins…`.

## Architecture map

- `airflow_lappis/dags/` (mounted to `/opt/airflow/dags`) — DAGs, organized by domain:
  - `data_ingest/<source>/<name>_ingest_dag.py` — one DAG per ingested dataset
  - `dashboards/` — reporting DAGs
  - `dbt/<project>/cosmos_dag.py` — dbt rendered as a DAG
- `airflow_lappis/plugins/` — one API client per source (`cliente_*.py`); `cliente_base.py` is the shared base. `schedule_loader.py` = runtime schedule override.
- `airflow_lappis/helpers/` — shared Postgres connection, retry, and safe-request utilities.
- `airflow_lappis/templates/siape/*.xml.j2` — SOAP request templates.

### Three independent dbt projects

`airflow_lappis/dags/dbt/{ipea,mcid,mir}/` — each has its own `dbt_project.yml`, `profiles.yml`, and a `cosmos_dag.py` (astronomer-cosmos) that compiles models into an Airflow DAG. Profiles resolve DB hosts/credentials from env vars:

- `ipea` → postgres, `DB_DW_*` (defaults to local `postgres_dw`)
- `mcid` → postgres, `DB_DW_*_MCID`, **plus** a `staging_duckdb` output reading silver from MinIO S3 via DuckDB; `mcmv_silver_dbt` models are gated `+enabled: "{{ target.type == 'duckdb' }}"`
- `mir` → postgres, `DB_DW_*_MIR`

Medallion layers (bronze/silver/gold) are encoded as dbt model subfolders and per-model schemas in each `dbt_project.yml`.

## Environment & ops gotchas

- `docker-compose.yml` reads a `.env` file, but the repo ships only `local.env`. `cp local.env .env` before `docker-compose up -d`. `.env` is gitignored.
- `AIRFLOW_REPO_BASE` env var is used by the cosmos DAGs to locate dbt projects — prod/homolog use a different folder layout. Always build paths on `AIRFLOW_REPO_BASE`, never hardcode `/opt/airflow`.
- DAG schedules can be overridden at runtime via the Airflow Variable `dynamic_schedules` (see `schedule_loader.py` `get_dynamic_schedule()`). DAGs call `get_dynamic_schedule("<dag_id>")`.
- Git hooks (installed by `make setup`): **pre-commit** runs `make format` (it will rewrite your files), **pre-push** runs `make lint -e GITLAB_CI=TRUE` + `make test`. `GITLAB_CI=TRUE` skips sqlfluff, which needs a DB connection.
- `requirements.txt` (root) is a hand-pinned subset `pip install`ed by the Airflow `Dockerfile` — not managed by Poetry. `requirements.generated.txt` is the Poetry export and is gitignored.

## CI

`.github/workflows/main.yaml`: lint (`make lint-ci`, **non-blocking** via `|| true`) → test (pytest + coverage) → docker build → docker push (on `main`) → dbt docs deploy (on `main`, requires VPN secrets to reach the warehouse).

## Style

- Conventional Commits in **Brazilian Portuguese** (`feat:`, `fix:`, `docs:`, `refactor:` …). See `.github/TEMPLATES/COMMIT_TEMPLATE.md`.
- Python: black + ruff, line-length 90; mypy is strict (`disallow_untyped_defs`), runs with `--explicit-package-bases`.
- SQL: sqlfmt + sqlfluff (postgres dialect, dbt templater). sqlfluff's dbt context in `pyproject.toml` points **only** at the `ipea` project (`project_dir = ./airflow_lappis/dags/dbt/ipea`), even though `make lint` lints all of `dags/dbt`.

## Stale docs

`README.md`'s "Project Structure" section is outdated (references `airflow/`, `dbt/`, `jupyter/` — actual root is `airflow_lappis/`). Trust code and `docker-compose.yml` over that section.
