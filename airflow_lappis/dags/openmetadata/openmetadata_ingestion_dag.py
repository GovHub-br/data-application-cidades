from datetime import datetime, timedelta

from airflow.decorators import dag, task
from schedule_loader import get_dynamic_schedule

from openmetadata.config import (
    ALL_RECIPES,
    INGESTION_REPLACEMENTS,
    OPENMETADATA_GLOSSARY_PATH,
    OPENMETADATA_REQUIREMENTS,
    OPENMETADATA_SEMANTIC_RELATIONSHIPS_PATH,
    RECIPE_PIPELINE,
)


@dag(
    schedule_interval=get_dynamic_schedule("openmetadata_ingestion_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args={
        "owner": "@arthrok",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=3),
    },
    tags=["openmetadata", "dbt", "postgres", "superset", "metadata"],
)
def openmetadata_ingestion_dag() -> None:
    """DAG para executar as recipes do OpenMetadata."""

    @task.virtualenv(
        task_id="warm_openmetadata_virtualenv",
        requirements=OPENMETADATA_REQUIREMENTS,
        system_site_packages=False,
        expect_airflow=False,
        venv_cache_path="/tmp/airflow_venvs",
    )
    def warm_openmetadata_virtualenv() -> None:
        import os
        import sys

        sys.path.append(f"{os.environ['AIRFLOW_REPO_BASE']}/dags")

        from openmetadata.execution import warm_openmetadata_virtualenv as execute_warmup

        execute_warmup()

    @task.virtualenv(
        task_id="run_openmetadata_recipe_base",
        requirements=OPENMETADATA_REQUIREMENTS,
        system_site_packages=False,
        expect_airflow=False,
        venv_cache_path="/tmp/airflow_venvs",
    )
    def run_openmetadata_recipe(
        recipe_path: str,
        command: str,
        replacements: dict,
        dbt_project_dir: str = "",
    ) -> None:
        import os
        import sys

        sys.path.append(f"{os.environ['AIRFLOW_REPO_BASE']}/dags")

        from openmetadata.execution import (
            run_openmetadata_recipe as execute_openmetadata_recipe,
        )

        execute_openmetadata_recipe(
            recipe_path=recipe_path,
            command=command,
            replacements=replacements,
            dbt_project_dir=dbt_project_dir,
        )

    @task.virtualenv(
        task_id="sync_mcid_glossary",
        requirements=OPENMETADATA_REQUIREMENTS,
        system_site_packages=False,
        expect_airflow=False,
        venv_cache_path="/tmp/airflow_venvs",
    )
    def sync_mcid_glossary(
        glossary_definition_path: str,
        host_port: str,
        jwt_token: str,
    ) -> None:
        import logging
        import os
        import sys

        sys.path.append(f"{os.environ['AIRFLOW_REPO_BASE']}/dags")

        from openmetadata.glossary import sync_glossary

        summary = sync_glossary(
            glossary_definition_path=glossary_definition_path,
            host_port=host_port,
            jwt_token=jwt_token,
        )
        logging.info("Resumo da sincronizacao do glossario: %s", summary)

    @task.virtualenv(
        task_id="sync_mcid_semantic_relationships",
        requirements=OPENMETADATA_REQUIREMENTS,
        system_site_packages=False,
        expect_airflow=False,
        venv_cache_path="/tmp/airflow_venvs",
    )
    def sync_mcid_semantic_relationships(
        catalog_path: str,
        host_port: str,
        jwt_token: str,
    ) -> None:
        import logging
        import os
        import sys

        sys.path.append(f"{os.environ['AIRFLOW_REPO_BASE']}/dags")

        from openmetadata.semantic_relationships import (
            sync_semantic_relationships,
        )

        summary = sync_semantic_relationships(
            catalog_path=catalog_path,
            host_port=host_port,
            jwt_token=jwt_token,
        )
        logging.info("Resumo das relacoes semanticas do MCID: %s", summary)

    warmup = warm_openmetadata_virtualenv()
    glossary_sync = sync_mcid_glossary(
        glossary_definition_path=OPENMETADATA_GLOSSARY_PATH,
        host_port=INGESTION_REPLACEMENTS["OM_HOST"],
        jwt_token=INGESTION_REPLACEMENTS["INGESTION_TOKEN"],
    )
    semantic_relationships_sync = sync_mcid_semantic_relationships(
        catalog_path=OPENMETADATA_SEMANTIC_RELATIONSHIPS_PATH,
        host_port=INGESTION_REPLACEMENTS["OM_HOST"],
        jwt_token=INGESTION_REPLACEMENTS["INGESTION_TOKEN"],
    )

    recipe_tasks = {
        recipe.task_id: run_openmetadata_recipe.override(task_id=recipe.task_id)(
            recipe_path=recipe.recipe_path,
            command=recipe.command,
            replacements=dict(recipe.replacements),
            dbt_project_dir=recipe.dbt_project_dir,
        )
        for recipe in ALL_RECIPES
    }

    warmup >> glossary_sync
    previous_task = glossary_sync
    for task_id in RECIPE_PIPELINE:
        previous_task >> recipe_tasks[task_id]
        previous_task = recipe_tasks[task_id]
        if task_id == "dbt_metadata":
            previous_task >> semantic_relationships_sync
            previous_task = semantic_relationships_sync


openmetadata_ingestion_dag()
