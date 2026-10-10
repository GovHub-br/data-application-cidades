"""DAG do INCC-M: o DatasetSpec declarado e as duas tasks, que só chamam os passos."""

from pathlib import Path
from typing import Any

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.converters import Table
from ingestion.storage import StorageBackend
from tests.ingestion.dags.conftest import (
    RUN_AFTER,
    load_dag_module,
    nothing_new,
    record_steps,
    run_task,
    same_as_main,
)


@pytest.fixture
def module() -> Any:
    return load_dag_module("data_ingest/fgv/incc_m_ingest_dag.py")


def test_two_declared_tasks(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "incc_m_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert dag.default_args["owner"] == "Gustavo"
    assert {"fgv", "incc_m", "conjuntura", "ingestion"} <= set(dag.tags)
    assert set(dag.task_dict) == {"extract_to_raw", "convert_to_staging"}
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_the_dataset_spec_is_declared_in_the_dag(module: Any) -> None:
    spec = module.INCC_M

    assert (spec.domain, spec.dataset) == ("fgv", "incc_m")
    config = spec.extractor_config()
    [request] = config.requests
    assert (config.source, config.base_url) == ("http_file", "https://sindusconpr.com.br")
    assert request.endpoint.endswith("/9547-serie-historica-incc-m-fgv/")
    assert "Mozilla" in request.headers["User-Agent"]
    assert request.filename == "incc_m.xlsx"
    assert spec.converter == Table(module.incc_m, fallback_json=True)


def test_tasks_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    raw = run_task(dag, "extract_to_raw")
    latest = run_task(dag, "convert_to_staging", raw)

    assert (raw, latest) == ("raw/x/", "staging/x/")
    assert calls == [
        ("extract_to_raw", module.INCC_M, RUN_AFTER),
        ("convert_to_staging", module.INCC_M, "raw/x/"),
    ]


def test_nothing_new_is_a_skip(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    nothing_new(module, monkeypatch)

    with pytest.raises(AirflowSkipException):
        run_task(module.dag_instance, "extract_to_raw")


@pytest.mark.integration
def test_the_staging_is_the_same_as_the_main(
    module: Any,
    main_lake: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keys = same_as_main(
        module.INCC_M, "raw/fgv/incc_m.xlsx", main_lake, tmp_path, monkeypatch
    )

    assert keys == ["staging/fgv/incc_m.parquet"]
