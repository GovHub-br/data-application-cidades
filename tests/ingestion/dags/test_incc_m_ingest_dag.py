"""DAG do INCC-M: o DatasetSpec declarado e as duas tasks, que só chamam os passos."""

from typing import Any

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.converters import ConverterConfig
from ingestion.loaders import LoadMode
from ingestion.sources import fgv
from tests.ingestion.dags.conftest import (
    RUN_AFTER,
    load_dag_module,
    nothing_new,
    record_steps,
    run_task,
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
    assert spec.extractor_config().source == "http_file"
    assert spec.extractor_config().base_url == fgv.SINDUSCON_PR
    assert spec.extractor_config().requests == (fgv.INCC_M,)
    assert spec.converter == ConverterConfig(header_row=3)
    assert spec.load_mode is LoadMode.OVERWRITE


def test_tasks_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    raw = run_task(dag, "extract_to_raw")
    latest = run_task(dag, "convert_to_staging", raw)

    assert (raw, latest) == ("raw/x/", "staging/x/latest/")
    assert calls == [
        ("extract_to_raw", module.INCC_M, RUN_AFTER),
        ("convert_to_staging", module.INCC_M, "raw/x/"),
    ]


def test_nothing_new_is_a_skip(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    nothing_new(module, monkeypatch)

    with pytest.raises(AirflowSkipException):
        run_task(module.dag_instance, "extract_to_raw")
