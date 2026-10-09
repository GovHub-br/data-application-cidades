"""DAG da MRV no pipeline novo: a Planilha Interativa do RI, via catálogo da MZ."""

from datetime import datetime
from typing import Any

import pytest

from ingestion.layout import TIMEZONE
from ingestion.loaders import LoadMode
from tests.ingestion.dags.conftest import (
    RUN_AFTER,
    load_dag_module,
    record_steps,
    run_task,
)


@pytest.fixture
def module() -> Any:
    return load_dag_module("data_ingest/mrv/planilha_interativa_ingest_dag.py")


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "mrv_planilha_interativa_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {"mrv", "ingestion"} <= set(dag.tags)
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_dataset_spec_resolves_the_latest_quarter_in_the_catalog(module: Any) -> None:
    spec = module.DATASET
    config = spec.extractor_config()
    [request] = config.requests

    assert (spec.domain, spec.dataset, spec.load_mode) == (
        "mrv",
        "planilha_interativa",
        LoadMode.OVERWRITE,
    )
    assert (config.source, config.base_url) == (
        "http_file",
        "https://apicatalog.mziq.com",
    )
    assert request.resolve is not None
    # A aba muda de sufixo entre edições ("Oper.Data", "Oper.D").
    assert spec.converter.include == r"^Dados Oper\."
    assert spec.converter.header_row == 2


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


def test_catalog_query_tries_this_year_and_the_two_before(module: Any) -> None:
    [request] = module.DATASET.extractor_config().requests
    calls: list[dict[str, Any]] = []
    year = datetime.now(TIMEZONE).year

    class Response:
        status_code = 200

        def __init__(self, body: dict[str, Any]) -> None:
            self.body = body

        def json(self) -> dict[str, Any]:
            documents = (
                [{"internal_name": module.CATEGORIA, "file_quarter": 2, "permalink": "p2"}]
                if self.body["year"] == str(year - 1)
                else []
            )
            return {"data": {"document_metas": documents}}

    class Session:
        def request(self, method: str, url: str, **kwargs: Any) -> Response:
            calls.append({"method": method, "url": url, **kwargs})
            return Response(kwargs["json"])

    class Hook:
        base_url = "https://apicatalog.mziq.com"

        def get_conn(self, headers: Any = None) -> Session:
            return Session()

    assert request.resolve is not None
    assert request.resolve(Hook()) == "p2"
    assert [c["json"]["year"] for c in calls] == [str(year), str(year - 1)]
    assert calls[0]["method"] == "POST"
    assert calls[0]["url"].endswith("/filter/categories/year/meta")
    assert calls[0]["json"]["categories"] == [module.CATEGORIA]
