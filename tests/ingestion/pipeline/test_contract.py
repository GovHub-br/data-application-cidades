"""Contrato dos passos do pipeline.

As tasks só trocam caminhos (XCom pequeno): por dataset, os prefixos das partições
gravadas e o `latest/` publicado, nunca dado. Os passos não importam o Airflow: a
DAG lê as Variables e traduz o `NothingNew` em skip. Os testes de comportamento
estão em test_steps.py.
"""

import inspect
from pathlib import Path

from ingestion.pipeline import steps


def test_steps_exchange_only_prefixes() -> None:
    extract = inspect.signature(steps.extract_to_raw)
    convert = inspect.signature(steps.convert_to_staging)

    assert extract.return_annotation in (dict[str, list[str]], "dict[str, list[str]]")
    assert list(convert.parameters)[:2] == ["spec", "raw"]
    assert convert.return_annotation in (dict[str, str], "dict[str, str]")


def test_steps_do_not_import_airflow() -> None:
    assert "airflow" not in Path(steps.__file__).read_text()
