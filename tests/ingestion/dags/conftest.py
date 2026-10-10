"""Carrega um arquivo de DAG como o Airflow faz: import do módulo, sem banco."""

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from ingestion.pipeline import NothingNew

# Servidor HTTP falso dos extratores, para os testes de fluxo das DAGs.
from tests.ingestion.extractors.conftest import http_server  # noqa: F401

DAGS_DIR = Path(__file__).resolve().parents[3] / "dags"
RUN_AFTER = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)


def load_dag_module(relative: str) -> ModuleType:
    path = DAGS_DIR / relative
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_task(dag: Any, task_id: str, *args: Any) -> Any:
    """Executa o callable da task fora do Airflow, com um dag_run mínimo."""
    callable_ = dag.task_dict[task_id].python_callable
    if args:
        return callable_(*args)
    return callable_(dag_run=SimpleNamespace(run_after=RUN_AFTER))


def record_steps(
    module: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> list[tuple[Any, ...]]:
    """Troca os passos do pipeline por gravadores; devolve a lista de chamadas.

    Os retornos são opacos ("raw/x/", "staging/x/"): o teste confere que a
    task os repassa. A lista da Variable entra na chamada quando a DAG a passa.
    """
    calls: list[tuple[Any, ...]] = []

    def extract_to_raw(spec: Any, when: Any, entries: Any = None) -> str:
        calls.append(("extract_to_raw", spec, when, *_given(entries)))
        return "raw/x/"

    def convert_to_staging(spec: Any, raw: Any, entries: Any = None) -> str:
        calls.append(("convert_to_staging", spec, raw, *_given(entries)))
        return "staging/x/"

    monkeypatch.setattr(module.steps, "extract_to_raw", extract_to_raw)
    monkeypatch.setattr(module.steps, "convert_to_staging", convert_to_staging)
    return calls


def _given(entries: Any) -> tuple[Any, ...]:
    return () if entries is None else (entries,)


def nothing_new(module: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    """Faz o extract_to_raw dos passos acusar que não há dado novo."""

    def extract_to_raw(*args: Any, **kwargs: Any) -> Any:
        raise NothingNew("nenhum dado novo")

    monkeypatch.setattr(module.steps, "extract_to_raw", extract_to_raw)
