"""DAG do crédito imobiliário/PIB (BACEN Olinda) no pipeline novo."""

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
    return load_dag_module("data_ingest/bacen/credito_pib_ingest_dag.py")


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "bacen_credito_pib_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {"bacen", "credito_pib", "conjuntura", "ingestion"} <= set(dag.tags)
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_dataset_spec_filters_with_the_space_encoded(module: Any) -> None:
    spec = module.DATASET
    config = spec.extractor_config()
    [request] = config.requests

    assert (spec.domain, spec.dataset, spec.load_mode) == (
        "bacen",
        "credito_imobiliario_pib",
        LoadMode.OVERWRITE,
    )
    assert (config.source, config.conn_id) == ("api", "http_bacen_olinda")
    # O OData do Olinda recusa o espaço como "+" (HTTP 400): vai no endpoint.
    assert "$filter=Info%20eq%20'indices_imobiliario_pib_br'" in request.endpoint
    assert not request.params
    assert spec.converter.record_path == "value.item"


def test_tasks_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    raw = run_task(dag, "extract_to_raw")
    run_task(dag, "convert_to_staging", raw)

    assert calls == [
        ("extract_to_raw", module.DATASET, RUN_AFTER),
        ("convert_to_staging", module.DATASET, "raw/x/"),
    ]
