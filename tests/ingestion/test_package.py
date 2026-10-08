"""Estrutura do pacote `ingestion` e o isolamento dele do carregador de plugins."""

import importlib
from pathlib import Path

import pytest

from tests.ingestion.conftest import PLUGINS_DIR

SUBPACKAGES = ["storage", "extractors", "raw", "converters", "loaders", "pipeline"]


@pytest.mark.parametrize("name", ["ingestion", *(f"ingestion.{s}" for s in SUBPACKAGES)])
def test_package_is_importable(name: str) -> None:
    assert importlib.import_module(name).__doc__


def test_airflow_plugin_loader_ignores_the_package() -> None:
    # O Airflow importa todo .py de plugins/ como plugin; sem isto, cada módulo do
    # pacote seria carregado duas vezes e as fábricas registrariam classes duplicadas.
    ignore_file = Path(PLUGINS_DIR) / ".airflowignore"

    assert "ingestion/" in ignore_file.read_text().splitlines()
