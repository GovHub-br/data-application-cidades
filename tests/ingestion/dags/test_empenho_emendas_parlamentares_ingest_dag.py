"""DAG dos empenhos de emendas parlamentares (Tesouro Gerencial) no pipeline novo."""

import re
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
    return load_dag_module(
        "data_ingest/tesouro_gerencial/mcid/empenho_emendas_parlamentares_ingest_dag.py"
    )


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "empenho_emendas_parlamentares_ingest_dag"
    assert dag.schedule == "0 9 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {"email", "tesouro", "emendas", "ingestion"} <= set(dag.tags)
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_dataset_spec_reads_the_zip_of_the_day(module: Any) -> None:
    spec = module.DATASET
    config = spec.extractor_config()
    mail = config.mail

    assert (spec.domain, spec.dataset) == (
        "siafi-tesouro-gerencial",
        "notas_empenho_emendas_parlamentares_mcid",
    )
    assert spec.load_mode is LoadMode.OVERWRITE
    assert config.source == "email"
    assert mail.subject == "notas_empenho_emendas_parlamentares_mcid"
    assert mail.credentials_variable == "email_credentials"
    assert re.match(mail.attachment_pattern, "notas_empenho.zip")
    assert not re.match(mail.attachment_pattern, "assinatura.png")
    # CSV UTF-16 com aspas; 9 linhas de preâmbulo antes do cabeçalho.
    assert (
        spec.converter.encoding,
        spec.converter.delimiter,
        spec.converter.skip_rows,
    ) == ("utf-16", ",", 9)


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
