"""DAG do ICST no pipeline novo: login no FGVDados e CSV da série para a raw."""

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
    return load_dag_module("data_ingest/fgv/icst_ingest_dag.py")


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "icst_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {"fgv", "icst", "conjuntura", "ingestion"} <= set(dag.tags)
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_dataset_spec_logs_in_with_the_variables(module: Any) -> None:
    spec = module.DATASET
    config = spec.extractor_config()

    assert (spec.domain, spec.dataset, spec.load_mode) == (
        "fgv",
        "icst",
        LoadMode.OVERWRITE,
    )
    assert config.source == "fgvdados"
    assert config.fgvdados is not None
    assert (
        config.fgvdados.series,
        config.fgvdados.email_variable,
        config.fgvdados.password_variable,
    ) == ("ICST", "dados_fgv_email", "dados_fgv_password")
    assert (spec.converter.encoding, spec.converter.delimiter) == ("latin-1", ";")


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
