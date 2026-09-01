"""Ingestão de metadados do MCID no OpenMetadata, pelos conectores nativos.

Roda as recipes do OpenMetadata na ordem em que uma depende da outra e
sincroniza o glossário e as relações semânticas do MCID. O código de apoio vive
em `helpers/openmetadata/`.

Esta DAG é o que mantém o catálogo em dia. Sem ela, a ingestão do serviço
`Cidades` parou em 21-23/07 e o catálogo passou a descrever um recorte antigo
do banco — foi assim que descrição de tabela, test case e chart do Superset
ficaram para trás sem ninguém perceber.

Complementa `scripts/governance/`, que aplica o que o conector não conhece:
produto de dados, permissão de uso, certificação, o lake e a linhagem de
coluna.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from schedule_loader import get_dynamic_schedule

from openmetadata.config import (
    ALL_RECIPES,
    OPENMETADATA_GLOSSARY_PATH,
    OPENMETADATA_REQUIREMENTS,
    OPENMETADATA_SEMANTIC_RELATIONSHIPS_PATH,
    RECIPE_PIPELINE,
    SEGREDOS_INGESTAO,
)

#: Cache do virtualenv, compartilhado pelas tasks para não reinstalar 255
#: pacotes a cada uma.
VENV_CACHE = "/tmp/airflow_venvs"


@dag(
    schedule_interval=get_dynamic_schedule("openmetadata_ingestion_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args={
        "owner": "mcid-data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=3),
    },
    tags=["openmetadata", "dbt", "postgres", "metadata", "mcid"],
)
def openmetadata_ingestion_dag() -> None:
    """Executa as recipes do OpenMetadata para o escopo do MCID."""

    @task.virtualenv(
        task_id="run_openmetadata_recipe_base",
        requirements=OPENMETADATA_REQUIREMENTS,
        # isolamento é o PONTO, não detalhe: o pacote exige SQLAlchemy 2
        # e o Airflow 2.8.1 exige 1.4. Não há versão que conviva.
        system_site_packages=False,
        expect_airflow=False,
        venv_cache_path=VENV_CACHE,
    )
    def run_openmetadata_recipe(
        recipe_path: str,
        command: str,
        replacements: dict,
        segredos: dict,
        dbt_project_dir: str = "",
    ) -> None:
        import os
        import sys

        sys.path.append(f"{os.environ['AIRFLOW_REPO_BASE']}/helpers")

        from openmetadata.runner import (
            run_openmetadata_recipe as executar_recipe,
        )

        resolvidos = dict(replacements)
        for marcador, nome_do_segredo in segredos.items():
            valor = os.environ.get(nome_do_segredo)
            if not valor:
                raise RuntimeError(
                    f"'{nome_do_segredo}' não está no ambiente. "
                    "Ver infra/env/.env.example."
                )
            resolvidos[marcador] = valor

        executar_recipe(
            recipe_path=recipe_path,
            command=command,
            replacements=resolvidos,
            dbt_project_dir=dbt_project_dir,
        )

    @task.virtualenv(
        task_id="sync_mcid_glossary",
        requirements=OPENMETADATA_REQUIREMENTS,
        # isolamento é o PONTO, não detalhe: o pacote exige SQLAlchemy 2
        # e o Airflow 2.8.1 exige 1.4. Não há versão que conviva.
        system_site_packages=False,
        expect_airflow=False,
        venv_cache_path=VENV_CACHE,
    )
    def sync_mcid_glossary(glossary_definition_path: str) -> None:
        import logging
        import os
        import sys

        sys.path.append(f"{os.environ['AIRFLOW_REPO_BASE']}/helpers")

        from openmetadata.glossary import sync_glossary

        resumo = sync_glossary(
            glossary_definition_path=glossary_definition_path,
            host_port=os.environ[SEGREDOS_INGESTAO["OM_HOST"]],
            jwt_token=os.environ[SEGREDOS_INGESTAO["INGESTION_TOKEN"]],
        )
        logging.info("Glossário MCID sincronizado: %s", resumo)

    @task.virtualenv(
        task_id="sync_mcid_semantic_relationships",
        requirements=OPENMETADATA_REQUIREMENTS,
        # isolamento é o PONTO, não detalhe: o pacote exige SQLAlchemy 2
        # e o Airflow 2.8.1 exige 1.4. Não há versão que conviva.
        system_site_packages=False,
        expect_airflow=False,
        venv_cache_path=VENV_CACHE,
    )
    def sync_mcid_semantic_relationships(catalog_path: str) -> None:
        import logging
        import os
        import sys

        sys.path.append(f"{os.environ['AIRFLOW_REPO_BASE']}/helpers")

        from openmetadata.semantic_relationships import sync_semantic_relationships

        resumo = sync_semantic_relationships(
            catalog_path=catalog_path,
            host_port=os.environ[SEGREDOS_INGESTAO["OM_HOST"]],
            jwt_token=os.environ[SEGREDOS_INGESTAO["INGESTION_TOKEN"]],
        )
        logging.info("Relações semânticas do MCID sincronizadas: %s", resumo)

    # O glossário vem ANTES das recipes: os FQNs que os `schema.yml` do dbt
    # referenciam em `meta.openmetadata.glossary` precisam existir para que a
    # ingestão dbt consiga resolvê-los.
    glossario = sync_mcid_glossary(glossary_definition_path=OPENMETADATA_GLOSSARY_PATH)
    relacoes = sync_mcid_semantic_relationships(
        catalog_path=OPENMETADATA_SEMANTIC_RELATIONSHIPS_PATH
    )

    tarefas = {
        recipe.task_id: run_openmetadata_recipe.override(task_id=recipe.task_id)(
            recipe_path=recipe.recipe_path,
            command=recipe.command,
            replacements=dict(recipe.replacements),
            segredos=dict(recipe.segredos),
            dbt_project_dir=recipe.dbt_project_dir,
        )
        for recipe in ALL_RECIPES
    }

    anterior = glossario
    for task_id in RECIPE_PIPELINE:
        # RECIPE_PIPELINE é a ordem completa; ALL_RECIPES já veio filtrado
        # pelas flags, então o que estiver desligado simplesmente não entra.
        if task_id not in tarefas:
            continue
        anterior >> tarefas[task_id]
        anterior = tarefas[task_id]
        if task_id == "dbt_metadata":
            anterior >> relacoes
            anterior = relacoes


openmetadata_ingestion_dag()
