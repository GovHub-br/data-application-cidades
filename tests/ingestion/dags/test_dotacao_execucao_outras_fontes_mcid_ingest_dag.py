"""DAG da dotação e execução (Tesouro Gerencial, MCid) no pipeline novo."""

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

DAG_FILE = (
    "data_ingest/tesouro_gerencial/mcid/dotacao_execucao_outras_fontes_mcid_ingest_dag.py"
)


@pytest.fixture
def module() -> Any:
    return load_dag_module(DAG_FILE)


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "dotacao_execucao_outras_fontes_mcid_ingest_dag"
    assert {"email", "mcid", "tesouro", "conjuntura", "ingestion"} <= set(dag.tags)
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_dataset_spec_reads_the_zip_of_the_day_with_credentials_from_variable(
    module: Any,
) -> None:
    spec = module.DATASET
    config = spec.extractor_config()
    mail = config.mail

    assert (spec.domain, spec.dataset) == (
        "siafi-tesouro-gerencial",
        "dotacao_execucao_outras_fontes_mcid",
    )
    assert spec.load_mode is LoadMode.OVERWRITE
    assert (config.source, config.conn_id) == ("email", "imap_tesouro")
    assert mail.subject == "dotacao_execucao_outras_fontes_mcid"
    assert mail.credentials_variable == "email_credentials"
    assert re.match(mail.attachment_pattern, "relatorio_dotacao.zip")
    assert not re.match(mail.attachment_pattern, "assinatura.png")
    assert (spec.converter.encoding, spec.converter.delimiter) == ("utf-16", "\t")


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
