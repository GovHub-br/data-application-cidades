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
from typing import Any

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


def _encadear(glossario: Any, tarefas: dict, relacoes: Any, governanca: Any) -> None:
    """Liga as tasks na ordem em que uma depende da outra.

    Glossário antes das recipes, porque os FQNs que os `schema.yml` do dbt
    referenciam precisam existir para a ingestão resolvê-los. E a governança
    SEMPRE por último: o conector dbt apaga a certificação das tabelas, e esta
    é a task que a devolve.
    """
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
    anterior >> governanca


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

    @task(task_id="reaplicar_governanca")
    def reaplicar_governanca() -> None:
        """Reaplica a governança DEPOIS do conector. É obrigatório.

        O conector dbt escreve a entidade da tabela sem o campo
        `certification`, e o `createOrUpdate` do OpenMetadata substitui o que
        estava lá por nulo: uma execução apagou a certificação das 140 tabelas
        de uma vez, em silêncio. Domínio, produto e etiqueta sobreviveram;
        certificação não.

        Roda no ambiente do Airflow, não no virtualenv: o script usa só
        `requests` e `yaml`, e precisa do `scripts/` montado no container.
        """
        import logging
        import os
        import subprocess

        caminho = f"{os.environ['AIRFLOW_REPO_BASE']}/scripts/governance"
        resultado = subprocess.run(
            ["python", f"{caminho}/sincronizar_governanca.py", "--confirmar"],
            capture_output=True,
            text=True,
            check=False,
            cwd=caminho,
        )
        logging.info("Governança reaplicada:\n%s", resultado.stdout[-4000:])
        if resultado.returncode != 0:
            raise RuntimeError(
                "Falha ao reaplicar a governança. O catálogo pode ter ficado "
                f"sem certificação:\n{resultado.stderr[-2000:]}"
            )

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

    _encadear(glossario, tarefas, relacoes, reaplicar_governanca())


openmetadata_ingestion_dag()
