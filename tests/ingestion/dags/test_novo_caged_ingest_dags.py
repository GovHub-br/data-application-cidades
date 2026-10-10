"""Novo CAGED (Power BI público) no pipeline novo: três recortes de CNAE."""

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

DAG_IDS = {
    "novo_caged_construcao_edificios": "saldo_estoque_construcao_edificios",
    "novo_caged_servicos_especializados_construcao": (
        "saldo_estoque_servicos_especializados_construcao"
    ),
    "novo_caged_total_construcao": "saldo_estoque_total_construcao",
}


@pytest.fixture
def module() -> Any:
    return load_dag_module("data_ingest/novo_caged/novo_caged_ingest_dags.py")


def test_three_dags_keep_the_ids_the_conjuntura_triggers(module: Any) -> None:
    assert set(module.DAGS) == set(DAG_IDS)
    for dag_id, dag in module.DAGS.items():
        assert dag.dag_id == dag_id
        assert dag.schedule == "0 6 * * *"
        assert dag.catchup is False and dag.max_active_runs == 1
        assert {"novo_caged", "conjuntura", "ingestion"} <= set(dag.tags)
        convert = dag.task_dict["convert_to_staging"]
        assert convert.upstream_task_ids == {"extract_to_raw"}


def test_one_query_per_month_since_2024(module: Any) -> None:
    spec = module.DATASETS["novo_caged_construcao_edificios"]
    config = spec.extractor_config()
    now = datetime.now(TIMEZONE)
    months = (now.year - 2024) * 12 + now.month

    assert (spec.domain, spec.dataset) == (
        "novo_caged",
        DAG_IDS["novo_caged_construcao_edificios"],
    )
    assert spec.load_mode is LoadMode.OVERWRITE
    assert spec.converter.format == "powerbi_dsr"
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


def test_total_has_no_cnae_division_filter(module: Any) -> None:
    config = module.DATASETS["novo_caged_total_construcao"].extractor_config()

    assert "CNAE 2.0 Divisão" not in str(config.requests[0].json)


def test_tasks_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)

    for dag_id, dag in module.DAGS.items():
        raw = run_task(dag, "extract_to_raw")
        run_task(dag, "convert_to_staging", raw)

    assert calls == [
        call
        for dag_id in module.DAGS
        for call in (
            ("extract_to_raw", module.DATASETS[dag_id], RUN_AFTER),
            ("convert_to_staging", module.DATASETS[dag_id], "raw/x/"),
        )
    ]
