"""DAG da ABECIP (poupança) no pipeline novo: link achado na página."""

from typing import Any

import pytest

from ingestion.extractors import ExtractionError
from ingestion.loaders import LoadMode
from tests.ingestion.dags.conftest import (
    RUN_AFTER,
    load_dag_module,
    record_steps,
    run_task,
)


@pytest.fixture
def module() -> Any:
    return load_dag_module("data_ingest/abecip/abecip_poupanca_trimestral_ingest_dag.py")


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "abecip_poupanca_trimestral_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {"abecip", "poupanca", "conjuntura", "ingestion"} <= set(dag.tags)
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_dataset_spec_resolves_the_link_in_the_page(module: Any) -> None:
    spec = module.DATASET
    config = spec.extractor_config()
    [request] = config.requests

    assert (spec.domain, spec.dataset, spec.load_mode) == (
        "abecip",
        "poupanca_sbpe_mensal",
        LoadMode.OVERWRITE,
    )
    assert (config.source, config.conn_id) == ("http_file", "http_abecip")
    assert request.endpoint == "/credito-imobiliario/indicadores/caderneta-de-poupanca"
    assert request.resolve is not None
    # O bronze em overwrite lê todo Parquet do latest/: uma aba só.
    assert (spec.converter.sheet, spec.converter.header_row) == ("SBPE_Mensal", 5)


def test_resolve_picks_the_link_with_the_pattern(module: Any) -> None:
    [request] = module.DATASET.extractor_config().requests

    class Page:
        status_code = 200
        url = "https://www.abecip.org.br/credito-imobiliario/indicadores/caderneta-de-poupanca"
        text = '<a href="/x.pdf">x</a><a href="/download?file=cp-historico-1.xlsx">y</a>'

    class Hook:
        def run(self, endpoint: str, **kwargs: Any) -> Page:
            assert endpoint == "/credito-imobiliario/indicadores/caderneta-de-poupanca"
            return Page()

    assert request.resolve is not None
    assert request.resolve(Hook()) == (
        "https://www.abecip.org.br/download?file=cp-historico-1.xlsx"
    )

    Page.text = "<html></html>"
    with pytest.raises(ExtractionError, match="cp-historico"):
        request.resolve(Hook())


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
