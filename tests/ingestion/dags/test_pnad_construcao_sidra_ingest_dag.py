"""DAG do PNAD-C construção via SIDRA no pipeline novo: dois datasets na mesma DAG."""

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
    return load_dag_module("data_ingest/ibge/pnad_construcao_sidra_ingest_dag.py")


def test_dag_identity_and_one_task_group_per_dataset(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "ibge_pnad_construcao_sidra_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {"ibge", "pnad", "sidra", "conjuntura", "ingestion"} <= set(dag.tags)
    for dataset in ("pnad_construcao_ocupados", "pnad_construcao_rendimento"):
        convert = dag.task_dict[f"{dataset}.convert_to_staging"]
        assert convert.upstream_task_ids == {f"{dataset}.extract_to_raw"}


def test_datasets_query_the_last_twelve_quarters_by_merge(module: Any) -> None:
    specs = {spec.dataset: spec for spec in module.DATASETS}
    ocupados = specs["pnad_construcao_ocupados"]
    [request] = ocupados.extractor_config().requests

    assert set(specs) == {"pnad_construcao_ocupados", "pnad_construcao_rendimento"}
    assert ocupados.domain == "ibge"
    assert ocupados.extractor_config().base_url == "https://apisidra.ibge.gov.br"
    assert request.endpoint == (
        "/values/t/6323/n1/all/v/4090/p/last%2012/c888/47946,47949"
    )
    # A SIDRA devolve só a janela pedida: o histórico se acumula pelo merge.
    assert ocupados.load_mode is LoadMode.MERGE
    assert ocupados.keys == ("D3C", "D4C")


def test_tasks_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    for spec in module.DATASETS:
        raw = run_task(dag, f"{spec.dataset}.extract_to_raw")
        run_task(dag, f"{spec.dataset}.convert_to_staging", raw)

    assert calls == [
        call
        for spec in module.DATASETS
        for call in (
            ("extract_to_raw", spec, RUN_AFTER),
            ("convert_to_staging", spec, "raw/x/"),
        )
    ]
