"""O endereço do OpenMetadata que chega às recipes de conector.

`OPENMETADATA_URL` é configurada sem o `/api` (os scripts de governança
acrescentam `/api/v1`), mas o SDK das recipes exige o `/api`. Sem a
normalização, toda task de conector da openmetadata_ingestion_dag falha ao
iniciar o cliente com "'Response' object is not subscriptable".
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "helpers"))

from openmetadata import runner  # noqa: E402
from openmetadata.rendering import sdk_host_port  # noqa: E402

RECIPE_POSTGRES = RAIZ / "helpers/openmetadata/recipes/postgres_metadata.yaml"


@pytest.mark.parametrize(
    ("configurada", "esperada"),
    [
        ("http://om:8585", "http://om:8585/api"),
        ("http://om:8585/", "http://om:8585/api"),
        ("http://om:8585/api", "http://om:8585/api"),
        ("http://om:8585/api/", "http://om:8585/api"),
        ("http://om:8585/api/v1", "http://om:8585/api"),
        (
            "https://openmetadata.exemplo.gov.br",
            "https://openmetadata.exemplo.gov.br/api",
        ),
    ],
)
def test_sdk_host_port_sempre_termina_em_api(configurada: str, esperada: str) -> None:
    assert sdk_host_port(configurada) == esperada


def test_sdk_host_port_recusa_vazio() -> None:
    with pytest.raises(ValueError):
        sdk_host_port("  ")


def test_recipe_renderizada_leva_o_api_mesmo_com_url_sem_api(monkeypatch) -> None:
    """A URL do .env (sem /api) chega à recipe com o /api que o SDK precisa."""
    capturado: dict = {}

    def execute_metadata(metadata_command: str, rendered_recipe_path: Path) -> None:
        capturado["recipe"] = yaml.safe_load(
            Path(rendered_recipe_path).read_text(encoding="utf-8")
        )

    monkeypatch.setattr(runner, "execute_metadata", execute_metadata)

    runner.run_openmetadata_recipe(
        recipe_path=str(RECIPE_POSTGRES),
        command="ingest",
        replacements={
            "OM_HOST": "http://openmetadata:8585",
            "INGESTION_TOKEN": "token",
            "DB_DW_HOST": "postgres",
            "DB_DW_PORT": "5432",
            "DB_DW_USER": "cidades",
            "DB_DW_PASSWORD": "senha",
            "DB_DW_DBNAME": "cidades",
        },
    )

    servidor = capturado["recipe"]["workflowConfig"]["openMetadataServerConfig"]
    assert servidor["hostPort"] == "http://openmetadata:8585/api"
