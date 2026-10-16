"""DAG do Novo CAGED: uma DAG, um dataset por recorte, consultas do Power BI."""

from datetime import datetime
from typing import Any

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.layout import TIMEZONE
from ingestion.loaders import LoadMode
from tests.ingestion.dags.conftest import (
    RUN_AFTER,
    load_dag_module,
    nothing_new,
    record_steps,
    run_task,
)


@pytest.fixture
def module() -> Any:
    return load_dag_module("data_ingest/novo_caged/novo_caged_ingest_dag.py")


def test_two_declared_tasks(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "novo_caged_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.default_args["owner"] == "Milena Rocha"
    assert {"novo_caged", "conjuntura", "ingestion"} <= set(dag.tags)
    assert set(dag.task_dict) == {"extract_to_raw", "convert_to_staging"}
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_three_cuts_with_the_datasets_the_bronzes_read(module: Any) -> None:
    assert [r["dataset"] for r in module.RECORTES] == [
        "saldo_estoque_construcao_edificios",
        "saldo_estoque_servicos_especializados_construcao",
        "saldo_estoque_total_construcao",
    ]
    spec = module.RECORTE
    assert spec.domain == "novo_caged"
    assert spec.load_mode is LoadMode.OVERWRITE
    assert spec.converter.format == "powerbi_dsr"


def test_one_query_per_month_since_2024(module: Any) -> None:
    spec = module.RECORTE.resolve(module.RECORTES[0])
    config = spec.extractor_config()
    now = datetime.now(TIMEZONE)
    months = (now.year - 2024) * 12 + now.month

    assert spec.dataset == "saldo_estoque_construcao_edificios"
    assert config.base_url == "https://wabi-brazil-south-api.analysis.windows.net"
    assert len(config.requests) == months
    first, last = config.requests[0], config.requests[-1]
    assert (first.name, last.name) == ("2024-01", f"{now:%Y-%m}")
    assert first.method == "POST"
    assert first.endpoint == "/public/reports/querydata?synchronous=true"
    assert "X-PowerBI-ResourceKey" in first.headers
    where = first.json["queries"][0]["Query"]["Commands"][0][
        "SemanticQueryDataShapeCommand"
    ]["Query"]["Where"]
    assert "'janeiro'" in str(where) and "2024L" in str(where)
    assert "'Construção de Edifícios'" in str(where)


def test_the_total_has_no_cnae_division_filter(module: Any) -> None:
    config = module.RECORTE.resolve(module.RECORTES[2]).extractor_config()

    assert "CNAE 2.0 Divisão" not in str(config.requests[0].json)


def test_tasks_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    run_task(dag, "convert_to_staging", run_task(dag, "extract_to_raw"))

    assert calls == [
        ("extract_to_raw", module.RECORTE, RUN_AFTER, module.RECORTES),
        ("convert_to_staging", module.RECORTE, "raw/x/", module.RECORTES),
    ]


def test_nothing_new_is_a_skip(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    nothing_new(module, monkeypatch)

    with pytest.raises(AirflowSkipException):
        run_task(module.dag_instance, "extract_to_raw")
