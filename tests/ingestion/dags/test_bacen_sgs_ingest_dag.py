"""DAG do BACEN SGS no pipeline novo: séries da Variable lidas só dentro da task."""

from typing import Any

import pytest

from ingestion.loaders import LoadMode
from tests.ingestion.dags.conftest import (
    RUN_AFTER,
    load_dag_module,
    record_steps,
    run_task,
)


class FakeVariable:
    value: Any = None
    reads: list[str] = []

    @classmethod
    def get(cls, key: str, **kwargs: Any) -> Any:
        cls.reads.append(key)
        return cls.value


@pytest.fixture
def module(monkeypatch: pytest.MonkeyPatch) -> Any:
    loaded = load_dag_module("data_ingest/bacen/bacen_sgs_ingest_dag.py")
    monkeypatch.setattr(FakeVariable, "reads", [])
    monkeypatch.setattr(loaded, "Variable", FakeVariable)
    return loaded


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "bacen_sgs_ingest_dag"
    assert {"bacen", "sgs", "conjuntura", "ingestion"} <= set(dag.tags)
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_series_are_not_read_at_parse_time(module: Any) -> None:
    assert FakeVariable.reads == []
    assert module.DATASET.load_mode is LoadMode.OVERWRITE
    assert (module.DATASET.domain, module.DATASET.dataset) == (
        "bacen",
        "financiamentos_imobiliarios",
    )


@pytest.mark.parametrize(
    "value",
    [
        {"selic_meta": 432, "ipca": 433},
        [{"selic_meta": 432, "ipca": 433}],  # formato que a Variable também aceita hoje
    ],
)
def test_one_request_per_series_named_after_the_type(module: Any, value: Any) -> None:
    FakeVariable.value = value

    config = module.DATASET.extractor_config()

    assert FakeVariable.reads == ["BACEN_SERIES"]
    assert (config.source, config.conn_id) == ("api", "http_bacen")
    assert [(r.name, r.endpoint, dict(r.params)) for r in config.requests] == [
        ("selic_meta", "/dados/serie/bcdata.sgs.432/dados", {"formato": "json"}),
        ("ipca", "/dados/serie/bcdata.sgs.433/dados", {"formato": "json"}),
    ]


def test_empty_variable_is_an_error(module: Any) -> None:
    FakeVariable.value = {}

    with pytest.raises(ValueError, match="BACEN_SERIES"):
        module.DATASET.extractor_config()


def test_tasks_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    run_task(dag, "convert_to_staging", run_task(dag, "extract_to_raw"))

    assert calls == [
        ("extract_to_raw", module.DATASET, RUN_AFTER),
        ("convert_to_staging", module.DATASET, "raw/x/"),
    ]
