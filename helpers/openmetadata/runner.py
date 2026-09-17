"""Ponto de entrada usado pela DAG: renderiza e executa uma recipe."""

import logging
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory

from openmetadata.dbt_artifacts import prepare_dbt_artifacts
from openmetadata.rendering import render_recipe
from openmetadata.workflows import execute_metadata, validate_command


def run_openmetadata_recipe(
    recipe_path: str,
    command: str,
    replacements: dict,
    dbt_project_dir: str = "",
) -> None:
    """Renderiza e executa uma unica recipe do OpenMetadata."""
    logging.basicConfig(level=logging.INFO)

    validate_command(command)

    # ExitStack remove os diretorios na ordem inversa da criacao, inclusive se
    # a execucao levantar.
    with ExitStack() as tmp_dirs:
        workdir = Path(tmp_dirs.enter_context(TemporaryDirectory(prefix="om_recipe_")))

        final_replacements = dict(replacements)
        if dbt_project_dir:
            final_replacements["DBT_TARGET_DIR"] = prepare_dbt_artifacts(
                dbt_project_dir,
                tmp_dirs,
            )

        rendered_recipe_path = render_recipe(
            source_recipe_path=recipe_path,
            recipe_replacements=final_replacements,
            output_dir=workdir,
        )

        execute_metadata(
            metadata_command=command,
            rendered_recipe_path=rendered_recipe_path,
        )
