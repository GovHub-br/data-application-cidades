"""Carrega um arquivo de DAG como o Airflow faz: import do módulo, sem banco."""

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pyarrow.parquet as pq
import pytest

from ingestion.converters import Table
from ingestion.layout import staging_key
from ingestion.pipeline import NothingNew, steps
from ingestion.storage import StorageBackend, StorageFactory

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


def same_as_main(
    spec: Any,
    raw_key: str,
    main: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entries: Any = None,
    ignore: tuple[str, ...] = ("dt_ingest",),
) -> list[str]:
    """Converte a raw da main com a DAG nova e compara com a staging da main.

    A raw é copiada do lake da main (só leitura) para um lake local, onde o
    `convert_to_staging` grava; cada Parquet gerado tem de ter as mesmas colunas,
    na mesma ordem, e os mesmos valores, linha a linha, que o da main com a mesma
    chave. `ignore` são as colunas que mudam a cada execução (o `dt_ingest`), que só
    precisam existir. Devolve as chaves comparadas.
    """
    root = tmp_path / "lake"
    monkeypatch.setenv("INGESTION_STORAGE_BACKEND", "local")
    monkeypatch.setenv("INGESTION_LOCAL_ROOT", str(root))
    monkeypatch.setenv("LAKE_TMPDIR", str(tmp_path / "tmp"))
    local = StorageFactory.create("local", root=root)
    copied = [raw_key]
    if isinstance(spec.converter, Table) and spec.converter.accumulate:
        copied.append(staging_key(raw_key))
    for key in copied:
        main.get_file(key, tmp_path / "main" / key)
        local.put_file(key, tmp_path / "main" / key)

    [dataset] = [spec.resolve(entry).name for entry in (entries or [{}])]
    staged = steps.convert_to_staging(spec, {dataset: [raw_key]}, entries)

    for key in staged[dataset]:
        main.get_file(key, tmp_path / "main" / key)
        expected = pq.read_table(tmp_path / "main" / key)
        actual = pq.read_table(root / key)
        assert actual.column_names == expected.column_names, key
        assert actual.num_rows == expected.num_rows, key
        for column in actual.column_names:
            if column not in ignore:
                assert (
                    actual[column].to_pylist() == expected[column].to_pylist()
                ), f"{key}: coluna {column}"
    return staged[dataset]
