"""DAG do BACEN SGS: séries da Variable lidas na task 1, uma chamada por série."""

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

SERIES = {"selic_meta": 432, "ipca": 433}


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
    monkeypatch.setattr(FakeVariable, "value", SERIES)
    monkeypatch.setattr(loaded, "Variable", FakeVariable)
    return loaded


def test_two_declared_tasks(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "bacen_sgs_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.default_args["owner"] == "Mateus"
    assert {"bacen", "sgs", "conjuntura", "ingestion"} <= set(dag.tags)
    assert set(dag.task_dict) == {"extract_to_raw", "convert_to_staging"}
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_the_dataset_spec_is_declared_and_reads_nothing_at_parse(module: Any) -> None:
    spec = module.SGS

    assert FakeVariable.reads == []
    assert (spec.domain, spec.dataset) == ("bacen", "financiamentos_imobiliarios")
    assert spec.load_mode is LoadMode.OVERWRITE


@pytest.mark.parametrize(
    "value",
    [SERIES, [SERIES]],  # a Variable também aceita a lista com um dict
)
def test_one_request_per_series_named_after_the_type(module: Any, value: Any) -> None:
    requests = module.series_sgs(value)

    assert [(r.name, r.endpoint, dict(r.params)) for r in requests] == [
        ("selic_meta", "/dados/serie/bcdata.sgs.432/dados", {"formato": "json"}),
        ("ipca", "/dados/serie/bcdata.sgs.433/dados", {"formato": "json"}),
    ]
    extractor = module.SGS.resolve(SERIES).extractor_config()
    assert (extractor.source, extractor.base_url) == ("api", "https://api.bcb.gov.br")


def test_empty_variable_is_an_error(module: Any) -> None:
    with pytest.raises(ValueError, match="BACEN_SERIES"):
        module.series_sgs({})


def test_tasks_read_the_variable_and_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    run_task(dag, "convert_to_staging", run_task(dag, "extract_to_raw"))

    assert FakeVariable.reads == ["BACEN_SERIES"]
    assert calls == [
        ("extract_to_raw", module.SGS, RUN_AFTER, [SERIES]),
        ("convert_to_staging", module.SGS, "raw/x/"),
    ]


def test_nothing_new_is_a_skip(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    nothing_new(module, monkeypatch)

    with pytest.raises(AirflowSkipException):
        run_task(module.dag_instance, "extract_to_raw")
