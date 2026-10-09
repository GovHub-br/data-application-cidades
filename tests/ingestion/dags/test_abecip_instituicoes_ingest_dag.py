"""DAG da ABECIP por instituição no pipeline novo: consolida o raw de outro time."""

import re
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
    return load_dag_module("data_ingest/abecip/abecip_instituicoes_ingest_dag.py")


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "abecip_instituicoes_ingest_dag"
    assert dag.schedule == "0 7 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {"abecip", "conjuntura", "ingestion"} <= set(dag.tags)
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_dataset_spec_copies_every_competencia(module: Any) -> None:
    spec = module.DATASET
    config = spec.extractor_config()
    query = config.objects

    assert (spec.domain, spec.dataset) == ("abecip", "financiamentos_por_instituicao")
    # Todas as competências a cada execução: a ABECIP revisa meses publicados.
    assert spec.load_mode is LoadMode.OVERWRITE
    assert config.source == "object_storage"
    assert query is not None and query.prefix == "raw/abecip/"
    pattern = re.compile(query.pattern)
    key = "raw/abecip/2026-07/financiamentos_por_instituicao.json"
    assert pattern.fullmatch(key)
    assert pattern.sub(query.rename or "", key) == "2026-07.json"
    assert not pattern.fullmatch("raw/abecip/2026-07/recursos_livres.json")


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
