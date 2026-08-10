"""
DAG isolada para o PoC de leitura da staging (MinIO/parquet) via pg_duckdb.

Roda separada do mcid_cosmos_dag de proposito:

  - os models do PoC tem `enabled=var('fgts_poc_enabled', false)`, entao a DAG
    principal simplesmente nao os enxerga -- nada muda em producao;
  - esta DAG liga a var e seleciona apenas a tag fgts_poc, entao o grafo tem
    exatamente os 6 models do PoC;
  - schedule=None: so roda quando voce disparar na mao.

Para comparar os dois desenhos de bronze, troque fgts_poc_materializacao
entre 'view' (padrao, sem copia) e 'table' (bronze materializada) e compare a
duracao das tasks na UI do Airflow.
"""

import os
from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import DBT_LOG_PATH_ENVVAR

dbt_log_path = "/tmp/dbt_logs"  # NOSONAR
os.makedirs(dbt_log_path, exist_ok=True)
os.environ[DBT_LOG_PATH_ENVVAR] = dbt_log_path

profile_config = ProfileConfig(
    profiles_yml_filepath=f"{os.environ['AIRFLOW_REPO_BASE']}/dags/dbt/mcid/profiles.yml",
    profile_name="mcid",
    target_name="prod",
)

fgts_poc_dag = DbtDag(
    project_config=ProjectConfig(
        f"{os.environ['AIRFLOW_REPO_BASE']}/dags/dbt/mcid",
        dbt_vars={
            "fgts_poc_enabled": True,
            "fgts_staging_prefix": os.environ.get(
                "FGTS_STAGING_PREFIX", "s3://data-lake-mcid/staging"
            ),
            "fgts_poc_materializacao": os.environ.get(
                "FGTS_POC_MATERIALIZACAO", "view"
            ),
        },
    ),
    # dbt_vars do ProjectConfig valem tambem no parsing (dbt ls), entao os
    # models chegam habilitados aqui e desabilitados na DAG principal.
    render_config=RenderConfig(select=["tag:fgts_poc"]),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_REPO_BASE']}/.local/bin/dbt",
    ),
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    dag_id="mcid_fgts_poc_staging_dag",
    tags=["poc", "fgts", "minio"],
    default_args={"retries": 0},
)
