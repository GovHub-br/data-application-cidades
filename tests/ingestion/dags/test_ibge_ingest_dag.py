"""DAG do IBGE (API v3 de agregados): a lista vem da Variable IBGE_AGREGADOS."""

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
AGREGADOS = json.loads((REPO / "infra/airflow/variables/IBGE_AGREGADOS.json").read_text())
# As chamadas que a DAG fazia antes da Variable nova (mesmo endpoint, mesmos params).
CHAMADAS = json.loads(
    (Path(__file__).parent / "fixtures/ibge_chamadas_v3.json").read_text()
)


class FakeVariable:
    reads: list[str] = []

    @classmethod
    def get(cls, key: str, **kwargs: Any) -> Any:
        cls.reads.append(key)
        return AGREGADOS


@pytest.fixture
def module(monkeypatch: pytest.MonkeyPatch) -> Any:
    loaded = load_dag_module("data_ingest/ibge/ibge_ingest_dag.py")
    monkeypatch.setattr(FakeVariable, "reads", [])
    monkeypatch.setattr(loaded, "Variable", FakeVariable)
    return loaded


def test_two_declared_tasks(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "ibge_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.default_args["owner"] == "Mateus"
    assert {"ibge", "conjuntura", "ingestion"} <= set(dag.tags)
    assert set(dag.task_dict) == {"extract_to_raw", "convert_to_staging"}
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_the_dataset_spec_merges_by_the_flattened_keys(module: Any) -> None:
    spec = module.AGREGADO

    assert FakeVariable.reads == []  # nada lido no parse
    assert spec.domain == "ibge"
    assert spec.load_mode is LoadMode.MERGE
    assert spec.keys == (
        "variavel_id",
        "localidade_id",
        "classificacao_id",
        "categoria_id",
        "periodo",
    )
    assert spec.converter.nested == ("resultados", "series")
    assert spec.converter.explode_keys == "serie"


def test_each_entry_is_the_same_v3_call_as_before(module: Any) -> None:
    assert [e["dataset"] for e in AGREGADOS] == list(CHAMADAS)
    for entrada in AGREGADOS:
        spec = module.AGREGADO.resolve(entrada)
        config = spec.extractor_config()
        [request] = config.requests

        assert spec.dataset == entrada["dataset"]
        assert (config.source, config.base_url) == (
            "api",
            "https://servicodados.ibge.gov.br",
        )
        assert {"endpoint": request.endpoint, "params": dict(request.params)} == (
            CHAMADAS[entrada["dataset"]]
        )


@pytest.mark.parametrize(
    ("entrada", "erro"),
    [
        (
            {
                "dataset": "x",
                "agregado": 1,
                "variaveis": [1],
                "periodos": 5,
                "classificacao": {"1": "all", "2": "all"},
            },
            "uma classificação",
        ),
        ({"dataset": "x", "agregado": 1, "periodos": 5}, "variaveis"),
        (
            {"dataset": "x", "agregado": 1, "variaveis": [1], "periodos": 5, "a": 1},
            "desconhecido",
        ),
    ],
)
def test_a_wrong_entry_is_an_error(module: Any, entrada: Any, erro: str) -> None:
    with pytest.raises(ValueError, match=erro):
        module.agregado(entrada)


def test_tasks_read_the_variable_and_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    run_task(dag, "convert_to_staging", run_task(dag, "extract_to_raw"))

    assert FakeVariable.reads == ["IBGE_AGREGADOS", "IBGE_AGREGADOS"]
    assert calls == [
        ("extract_to_raw", module.AGREGADO, RUN_AFTER, AGREGADOS),
        ("convert_to_staging", module.AGREGADO, "raw/x/", AGREGADOS),
    ]


def test_nothing_new_is_a_skip(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    nothing_new(module, monkeypatch)

    with pytest.raises(AirflowSkipException):
        run_task(module.dag_instance, "extract_to_raw")
