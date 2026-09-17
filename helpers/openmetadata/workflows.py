"""Execucao dos workflows do OpenMetadata.

Tres dos comandos precisam rodar dentro do processo em vez de pelo CLI
`metadata`; o porque de cada um esta em `_in_process_runner`.
"""

import logging
import os
import shutil
import subprocess
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import yaml

VALID_METADATA_COMMANDS = {"ingest", "profile", "classify"}
TABLE_ENTITY_PAGE_SIZE = 20
TABLE_WORKFLOW_SUCCESS_THRESHOLD = 100


def validate_command(metadata_command: str) -> None:
    if metadata_command not in VALID_METADATA_COMMANDS:
        raise ValueError(
            f"Comando inválido: {metadata_command}. "
            f"Esperado um de {VALID_METADATA_COMMANDS}"
        )


def _metadata_bin() -> str:
    """Caminho do CLI `metadata`, resolvido pelo PATH.

    Antes isto era `Path(sys.executable).with_name("metadata")`, herdado de
    quando a task rodava em @task.virtualenv -- la o interpretador e o console
    script viviam na mesma pasta. Nativamente nao vivem: o task runner do
    Airflow executa com /usr/python/bin/python, enquanto o pacote instala o
    script em ~/.local/bin, e a busca ia para /usr/python/bin/metadata.
    """
    caminho = shutil.which("metadata")
    if caminho is None:
        raise FileNotFoundError(
            "CLI `metadata` nao encontrado no PATH. Ele vem do "
            "openmetadata-ingestion, que deve estar em "
            "infra/docker/airflow/requirements.lock.txt."
        )
    return caminho


def _in_process_runner(metadata_command: str, source_type: str) -> Callable | None:
    """Qual workflow roda dentro do processo, se algum.

    O CLI `metadata` cobre o caso comum. Tres casos precisam rodar em-process:
    profiler e classifier porque a paginacao padrao de 100 tabelas estoura o
    timeout do proxy e so da para corrigir no cliente; e a ingestao de Airflow
    porque o source inicializa o proprio pacote airflow.
    """
    if metadata_command == "profile":
        return execute_profiler_workflow_in_process
    if metadata_command == "classify":
        return execute_classifier_workflow_in_process
    if metadata_command == "ingest" and source_type == "airflow":
        return execute_metadata_workflow_in_process
    return None


def execute_metadata(metadata_command: str, rendered_recipe_path: Path) -> None:
    rendered_recipe_data = yaml.safe_load(
        rendered_recipe_path.read_text(encoding="utf-8")
    )
    source_type = rendered_recipe_data.get("source", {}).get("type", "")

    logging.info(
        "[openmetadata.execution] Executando metadata %s -c %s",
        metadata_command,
        rendered_recipe_path,
    )

    runner = _in_process_runner(metadata_command, source_type)
    if runner is not None:
        runner(rendered_recipe_data)
        return

    subprocess.run(
        [_metadata_bin(), metadata_command, "-c", str(rendered_recipe_path)],
        env=os.environ.copy(),
        check=True,
    )


def execute_metadata_workflow_in_process(workflow_config: dict) -> None:
    """Executa a ingestao pela API Python, em vez do CLI `metadata`.

    Necessario para o source `airflow`: ele inicializa o proprio pacote airflow,
    e num subprocesso do CLI isso falha com erro generico de plugin ausente.
    """
    from metadata.workflow.metadata import MetadataWorkflow

    logging.info(
        "[openmetadata.execution] Executando workflow em-process "
        "via MetadataWorkflow.create(...)"
    )

    workflow = MetadataWorkflow.create(workflow_config)
    try:
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status()
    finally:
        workflow.stop()


def set_entity_list_page_size(
    metadata_client: object,
    entity_type: type,
    page_size: int,
) -> None:
    """Apply a default page size to one entity type on an OMeta client instance."""
    if page_size <= 0:
        raise ValueError("page_size deve ser maior que zero")

    original_list_all_entities = getattr(metadata_client, "list_all_entities")

    @wraps(original_list_all_entities)
    def list_all_entities_with_page_size(*args: Any, **kwargs: Any) -> Any:
        requested_entity = kwargs.get("entity")
        if requested_entity is None and args:
            requested_entity = args[0]

        has_positional_limit = len(args) >= 3
        if (
            requested_entity is entity_type
            and "limit" not in kwargs
            and not has_positional_limit
        ):
            kwargs["limit"] = page_size

        return original_list_all_entities(*args, **kwargs)

    setattr(metadata_client, "list_all_entities", list_all_entities_with_page_size)


def execute_table_workflow_in_process(
    workflow_config: dict,
    workflow_class: type,
    workflow_name: str,
    table_page_size: int = TABLE_ENTITY_PAGE_SIZE,
) -> None:
    """Run a table workflow with bounded OpenMetadata entity-list API pages."""
    from metadata.cli.common import execute_workflow
    from metadata.generated.schema.entity.data.table import Table

    logging.info(
        "[openmetadata.execution] Executando %s em-process " "com paginas de %s tabelas",
        workflow_name,
        table_page_size,
    )

    workflow = workflow_class.create(workflow_config)
    # ProfilerWorkflow força 80% no construtor, o que permite a task terminar
    # verde mesmo com falhas parciais. Para as 74 tabelas auditadas, qualquer
    # falha deve acionar retry/erro em vez de ficar escondida no resumo.
    workflow.workflow_config.successThreshold = TABLE_WORKFLOW_SUCCESS_THRESHOLD
    set_entity_list_page_size(
        metadata_client=workflow.metadata,
        entity_type=Table,
        page_size=table_page_size,
    )
    execute_workflow(workflow=workflow, config_dict=workflow_config)


def execute_profiler_workflow_in_process(
    workflow_config: dict,
    table_page_size: int = TABLE_ENTITY_PAGE_SIZE,
) -> None:
    """Run the profiler with bounded OpenMetadata table-list API pages."""
    from metadata.workflow.profiler import ProfilerWorkflow

    execute_table_workflow_in_process(
        workflow_config=workflow_config,
        workflow_class=ProfilerWorkflow,
        workflow_name="ProfilerWorkflow",
        table_page_size=table_page_size,
    )


def execute_classifier_workflow_in_process(
    workflow_config: dict,
    table_page_size: int = TABLE_ENTITY_PAGE_SIZE,
) -> None:
    """Run auto-classification with bounded table-list API pages."""
    from metadata.workflow.classification import AutoClassificationWorkflow

    execute_table_workflow_in_process(
        workflow_config=workflow_config,
        workflow_class=AutoClassificationWorkflow,
        workflow_name="AutoClassificationWorkflow",
        table_page_size=table_page_size,
    )
