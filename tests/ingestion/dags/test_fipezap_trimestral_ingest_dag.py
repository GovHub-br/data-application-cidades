"""DAG do FipeZap no pipeline novo: a planilha da FIPE, só a aba do índice."""

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
    return load_dag_module("data_ingest/fipe/fipezap_trimestral_ingest_dag.py")


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "fipezap_trimestral_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {"fipezap", "locacao", "conjuntura", "ingestion"} <= set(dag.tags)
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_dataset_spec_reads_only_the_index_sheet(module: Any) -> None:
    spec = module.DATASET
    config = spec.extractor_config()
    [request] = config.requests

    assert (spec.domain, spec.dataset, spec.load_mode) == (
        "fipe",
        "indice_locacao",
        LoadMode.OVERWRITE,
    )
    assert (config.source, config.conn_id) == ("http_file", "http_fipe")
    assert request.endpoint == "/indices/fipezap/fipezap-serieshistoricas.xlsx"
    # O bronze em overwrite lê todo Parquet do latest/: uma aba só.
    assert (spec.converter.sheet, spec.converter.header_row) == ("Índice FipeZAP", 4)


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
