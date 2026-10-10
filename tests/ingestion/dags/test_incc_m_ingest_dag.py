"""DAG do INCC-M no pipeline novo: só liga os passos, toda a configuração é literal."""

from typing import Any

import pytest

from ingestion.loaders import LoadMode
from tests.ingestion.dags.conftest import (
    RUN_AFTER,
    load_dag_module,
    record_steps,
    run_task,
)


@pytest.fixture
def module() -> Any:
    return load_dag_module("data_ingest/fgv/incc_m_ingest_dag.py")


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "incc_m_ingest_dag"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert dag.schedule == "0 6 * * *"
    assert {"fgv", "incc_m", "conjuntura", "ingestion"} <= set(dag.tags)
    assert set(dag.task_dict) == {"extract_to_raw", "convert_to_staging"}
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_dataset_spec(module: Any) -> None:
    spec = module.DATASET
    [request] = spec.extractor_config().requests

    assert (spec.domain, spec.dataset, spec.load_mode) == (
        "fgv",
        "incc_m",
        LoadMode.OVERWRITE,
    )
    assert spec.extractor_config().source == "http_file"
    assert spec.extractor_config().base_url == "https://sindusconpr.com.br"
    assert request.endpoint.endswith("/9547-serie-historica-incc-m-fgv/")
    assert "Mozilla" in request.headers["User-Agent"]
    assert spec.converter.header_row == 3


def test_tasks_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    raw = run_task(dag, "extract_to_raw")
    latest = run_task(dag, "convert_to_staging", raw)

    assert (raw, latest) == ("raw/x/", "staging/x/latest/")
    assert calls == [
        ("extract_to_raw", module.DATASET, RUN_AFTER),
        ("convert_to_staging", module.DATASET, "raw/x/"),
    ]
