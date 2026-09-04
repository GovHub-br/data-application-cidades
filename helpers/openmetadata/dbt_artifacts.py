"""Geracao dos artefatos dbt que a recipe de dbt consome."""

import logging
import os
import shutil
import subprocess
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory

DBT_COMMANDS = (
    ("deps",),
    ("docs", "generate"),
)

# Sem manifest nao ha o que ingerir. Os outros dois enriquecem e podem faltar:
# no schema do OpenMetadata so dbtManifestFilePath e obrigatorio.
REQUIRED_DBT_ARTIFACTS = ("manifest.json",)
OPTIONAL_DBT_ARTIFACTS = ("catalog.json", "run_results.json")


def prepare_dbt_artifacts(project_dir: str, tmp_dirs: ExitStack) -> str:
    source_project_dir = Path(project_dir)

    if not source_project_dir.exists():
        raise FileNotFoundError(f"Projeto dbt não encontrado: {source_project_dir}")

    workdir = Path(tmp_dirs.enter_context(TemporaryDirectory(prefix="om_dbt_")))
    project_copy = workdir / "dbt_project"

    shutil.copytree(
        source_project_dir,
        project_copy,
        ignore=shutil.ignore_patterns(
            "target",
            "logs",
            "dbt_packages",
            ".venv",
            "__pycache__",
        ),
    )

    env = os.environ.copy()

    for dbt_command in DBT_COMMANDS:
        cmd = [
            "dbt",
            *dbt_command,
            "--project-dir",
            str(project_copy),
            "--profiles-dir",
            str(project_copy),
        ]
        logging.info(
            "[openmetadata.execution] Executando comando: %s",
            " ".join(cmd),
        )
        subprocess.run(
            cmd,
            cwd=str(project_copy),
            env=env,
            check=True,
        )

    target_dir = project_copy / "target"

    for file_name in REQUIRED_DBT_ARTIFACTS:
        artifact = target_dir / file_name
        if not artifact.exists():
            raise FileNotFoundError(f"{file_name} não encontrado em {artifact}")

    ausentes = [
        nome for nome in OPTIONAL_DBT_ARTIFACTS if not (target_dir / nome).exists()
    ]
    if ausentes:
        logging.warning(
            "[openmetadata] Artefatos opcionais ausentes (%s). A ingestao segue "
            "com o manifest; o OpenMetadata perde tipos/estatisticas de execucao.",
            ", ".join(ausentes),
        )

    return str(target_dir)
