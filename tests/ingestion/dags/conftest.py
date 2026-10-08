"""Carrega um arquivo de DAG como o Airflow faz: import do módulo, sem banco."""

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

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
    """Troca os passos do pipeline por gravadores; devolve a lista de chamadas."""
    calls: list[tuple[Any, ...]] = []

    def extract_to_raw(spec: Any, when: Any) -> str:
        calls.append(("extract_to_raw", spec, when))
        return "raw/x/"

    def convert_to_staging(spec: Any, raw_prefix: str) -> str:
        calls.append(("convert_to_staging", spec, raw_prefix))
        return "staging/x/latest/"

    monkeypatch.setattr(module.steps, "extract_to_raw", extract_to_raw)
    monkeypatch.setattr(module.steps, "convert_to_staging", convert_to_staging)
    return calls
