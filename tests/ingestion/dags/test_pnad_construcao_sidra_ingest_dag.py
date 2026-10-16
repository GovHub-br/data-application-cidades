"""DAG do PNAD-C pela SIDRA: as tabelas vêm da Variable SIDRA_TABELAS."""

import json
from pathlib import Path
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

REPO = Path(__file__).resolve().parents[3]
TABELAS = json.loads((REPO / "infra/airflow/variables/SIDRA_TABELAS.json").read_text())
# As chamadas que a DAG fazia antes da Variable nova.
CHAMADAS = json.loads(
    (Path(__file__).parent / "fixtures/sidra_chamadas.json").read_text()
)


class FakeVariable:
    reads: list[str] = []

    @classmethod
    def get(cls, key: str, **kwargs: Any) -> Any:
        cls.reads.append(key)
        return TABELAS


@pytest.fixture
def module(monkeypatch: pytest.MonkeyPatch) -> Any:
    loaded = load_dag_module("data_ingest/ibge/pnad_construcao_sidra_ingest_dag.py")
    monkeypatch.setattr(FakeVariable, "reads", [])
    monkeypatch.setattr(loaded, "Variable", FakeVariable)
    return loaded


def test_two_declared_tasks(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "ibge_pnad_construcao_sidra_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.default_args["owner"] == "Lucas Bottino"
    assert {"ibge", "pnad", "sidra", "ingestion"} <= set(dag.tags)
    assert set(dag.task_dict) == {"extract_to_raw", "convert_to_staging"}
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_the_dataset_spec_merges_by_quarter_and_category(module: Any) -> None:
    spec = module.TABELA

    assert FakeVariable.reads == []
    assert spec.domain == "ibge"
    assert (spec.load_mode, spec.keys) == (LoadMode.MERGE, ("D3C", "D4C"))


def test_each_entry_is_the_same_sidra_call_as_before(module: Any) -> None:
    assert [t["dataset"] for t in TABELAS] == list(CHAMADAS)
    for entrada in TABELAS:
        spec = module.TABELA.resolve(entrada)
        config = spec.extractor_config()
        [request] = config.requests

        assert spec.dataset == entrada["dataset"]
        assert (config.source, config.base_url) == ("api", "https://apisidra.ibge.gov.br")
        assert request.endpoint == CHAMADAS[entrada["dataset"]]["endpoint"]
        assert dict(request.headers) == CHAMADAS[entrada["dataset"]]["headers"]


def test_a_wrong_entry_is_an_error(module: Any) -> None:
    with pytest.raises(ValueError, match="variavel"):
        module.tabela_sidra({"dataset": "x", "tabela": 1, "periodos": 12})


def test_tasks_read_the_variable_and_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    run_task(dag, "convert_to_staging", run_task(dag, "extract_to_raw"))

    assert FakeVariable.reads == ["SIDRA_TABELAS", "SIDRA_TABELAS"]
    assert calls == [
        ("extract_to_raw", module.TABELA, RUN_AFTER, TABELAS),
        ("convert_to_staging", module.TABELA, "raw/x/", TABELAS),
    ]


def test_nothing_new_is_a_skip(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    nothing_new(module, monkeypatch)

    with pytest.raises(AirflowSkipException):
        run_task(module.dag_instance, "extract_to_raw")
