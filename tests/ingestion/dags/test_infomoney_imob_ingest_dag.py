"""DAG do índice IMOB (Alpha Vantage) no pipeline novo: merge por pregão."""

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
    return load_dag_module("data_ingest/infomoney/imob_ingest_dag.py")


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "infomoney_imob"
    assert dag.schedule == "0 6 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {"infomoney", "imob", "conjuntura", "ingestion"} <= set(dag.tags)
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_dataset_spec_accumulates_by_trading_day(module: Any) -> None:
    spec = module.DATASET

    assert (spec.domain, spec.dataset) == ("infomoney", "acoes_imob")
    # A API devolve só os últimos ~100 pregões: o histórico se acumula pelo merge.
    assert spec.load_mode is LoadMode.MERGE
    assert spec.keys == ("data_pregao",)
    assert spec.converter.record_path == "Time Series (Daily)"
    assert spec.converter.key_column == "data_pregao"


def test_symbol_and_key_come_from_the_variable_inside_the_task(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    read: list[str] = []

    def variable(name: str, **kwargs: Any) -> dict[str, str]:
        read.append(name)
        return {"acao": "IMOB.SA", "api_key": "chave"}

    monkeypatch.setattr(module.Variable, "get", variable)

    config = module.DATASET.extractor_config()
    [request] = config.requests

    assert read == ["api_key_alphavantage"]
    assert config.base_url == "https://www.alphavantage.co"
    assert (request.name, request.endpoint) == ("IMOB.SA", "/query")
    assert dict(request.params) == {
        "function": "TIME_SERIES_DAILY",
        "symbol": "IMOB.SA",
        "outputsize": "compact",
        "apikey": "chave",
    }


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
