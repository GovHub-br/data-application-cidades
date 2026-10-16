"""DAG do índice IMOB (Alpha Vantage): merge por pregão, chave lida na task 1."""

from typing import Any

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.loaders import LoadMode
from tests.ingestion.dags.conftest import (
    RUN_AFTER,
    load_dag_module,
    nothing_new,
    record_steps,
    run_task,
)

ALPHA = {"acao": "IMOB11.SAO", "api_key": "segredo"}


class FakeVariable:
    reads: list[str] = []

    @classmethod
    def get(cls, key: str, **kwargs: Any) -> Any:
        cls.reads.append(key)
        return ALPHA


@pytest.fixture
def module(monkeypatch: pytest.MonkeyPatch) -> Any:
    loaded = load_dag_module("data_ingest/infomoney/imob_ingest_dag.py")
    monkeypatch.setattr(FakeVariable, "reads", [])
    monkeypatch.setattr(loaded, "Variable", FakeVariable)
    return loaded


def test_two_declared_tasks(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "infomoney_imob"
    assert dag.schedule == "0 6 * * *"
    assert dag.default_args["owner"] == "Milena Rocha"
    assert {"infomoney", "imob", "conjuntura", "ingestion"} <= set(dag.tags)
    assert set(dag.task_dict) == {"extract_to_raw", "convert_to_staging"}
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_the_dataset_spec_accumulates_by_trading_day(module: Any) -> None:
    spec = module.ACOES_IMOB

    assert FakeVariable.reads == []
    assert (spec.domain, spec.dataset) == ("infomoney", "acoes_imob")
    assert spec.load_mode is LoadMode.MERGE and spec.keys == ("data_pregao",)
    assert spec.converter.record_path == "Time Series (Daily)"
    assert spec.converter.key_column == "data_pregao"


def test_one_daily_series_call_with_the_symbol_and_key_of_the_variable(
    module: Any,
) -> None:
    [request] = module.cotacao_diaria(ALPHA)
    config = module.ACOES_IMOB.resolve(ALPHA).extractor_config()

    assert (config.source, config.base_url) == ("api", "https://www.alphavantage.co")
    assert (request.name, request.endpoint) == ("IMOB11.SAO", "/query")
    assert dict(request.params) == {
        "function": "TIME_SERIES_DAILY",
        "symbol": "IMOB11.SAO",
        "outputsize": "compact",
        "apikey": "segredo",
    }


def test_tasks_read_the_variable_and_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    run_task(dag, "convert_to_staging", run_task(dag, "extract_to_raw"))

    assert FakeVariable.reads == ["api_key_alphavantage"]
    assert calls == [
        ("extract_to_raw", module.ACOES_IMOB, RUN_AFTER, [ALPHA]),
        ("convert_to_staging", module.ACOES_IMOB, "raw/x/"),
    ]


def test_nothing_new_is_a_skip(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    nothing_new(module, monkeypatch)

    with pytest.raises(AirflowSkipException):
        run_task(module.dag_instance, "extract_to_raw")
