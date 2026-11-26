"""DAG do dbt do MCid.

Um task group só, no target `prod` (dbt-postgres). O SQL de DuckDB do projeto (dados
históricos, Reforma Casa Brasil) roda no pg_duckdb, dentro do envelope `duckdb_query()`; o
resto é SQL de Postgres, que usa as UDFs `normalize_apf` / `parse_date_br` /
`corrigir_mojibake` criadas pelo `create_udfs()` no on-run-start. Quem lê o lake é o DuckDB
do banco, com o secret configurado no servidor (infra/playbooks/postgres/README.md).

Até 11/2026 eram dois grupos: Reforma Casa Brasil e Linha Financiada rodavam num target
próprio (`prod_duckdb`, DuckDB no processo do dbt) e o grupo inteiro esperava o do Postgres.
Qualquer falha da conjuntura deixava os dois domínios e a linhagem do OpenMetadata em
upstream_failed, e os testes que ligavam um seed a um model do outro grupo rodavam antes de a
tabela existir. Num grupo só, o Cosmos liga as tasks pelo grafo do dbt: uma falha para só o
que depende dela.
"""

import os
from datetime import datetime

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import DBT_LOG_PATH_ENVVAR

dbt_log_path = "/tmp/dbt_logs"  # NOSONAR
os.makedirs(dbt_log_path, exist_ok=True)
os.environ[DBT_LOG_PATH_ENVVAR] = dbt_log_path

REPO_BASE = os.environ["AIRFLOW_REPO_BASE"]
PROJECT_DIR = f"{REPO_BASE}/dbt/mcid"
PROFILES_YML = f"{PROJECT_DIR}/profiles.yml"

# Seletor do próprio dbt (dbt/mcid/selectors.yml): o projeto inteiro, com as fontes. O `dbt
# docs generate` da ingestão do OpenMetadata usa o mesmo.
SELETOR = "dominios_postgres"

# partial_parse LIGADO (padrão): desligado, cada `dbt ls` da renderização ficava lento o
# bastante para estourar o dagbag_import_timeout.
project_config = ProjectConfig(PROJECT_DIR)

execution_config = ExecutionConfig(
    dbt_executable_path=f"{REPO_BASE}/.local/bin/dbt",
)

profile_config = ProfileConfig(
    profiles_yml_filepath=PROFILES_YML,
    profile_name="mcid",
    target_name="prod",
)

with DAG(
    dag_id="mcid_cosmos_dag",
    schedule="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # cron e minio_transform_dag disparam esta DAG: duas execuções do dbt ao mesmo tempo
    # disputam as mesmas tabelas
    max_active_runs=1,
    default_args={"retries": 2},
    doc_md=__doc__,
) as my_cosmos_dag:
    dominios_postgres = DbtTaskGroup(
        group_id="dominios_postgres",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            selector=SELETOR,
            # Teste com mais de um pai vira task própria, depois de todos eles. Sem isso o
            # Cosmos roda `dbt test --select <model>` e a seleção indireta (eager) puxa o
            # teste para a task de CADA pai: ele roda uma vez por pai, e a primeira pode vir
            # antes de o outro pai ser reconstruído (ou antes de a tabela existir).
            should_detach_multiple_parents_tests=True,
        ),
    )

    # all_done: a ingestão do OpenMetadata gera os próprios artefatos do dbt (docs generate
    # numa cópia do projeto) e lê o catálogo do banco, então não depende de o dbt ter passado
    # inteiro. Antes, uma falha qualquer deixava descrições e linhagem do catálogo paradas.
    publicar_linhagem_openmetadata = TriggerDagRunOperator(
        task_id="publicar_linhagem_openmetadata",
        trigger_dag_id="openmetadata_ingestion_dag",
        wait_for_completion=False,
        reset_dag_run=False,
        trigger_rule="all_done",
    )

    dominios_postgres >> publicar_linhagem_openmetadata
