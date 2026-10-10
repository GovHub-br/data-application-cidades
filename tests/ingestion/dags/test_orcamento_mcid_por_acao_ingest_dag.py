"""DAG do orçamento do MCid por ação (Tesouro Gerencial) no pipeline novo."""

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
        "data_ingest/tesouro_gerencial/mcid/orcamento_mcid_por_acao_ingest_dag.py"
    )


def test_dag_identity_and_wiring(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "orcamento_mcid_por_acao_ingest_dag"
    assert dag.schedule == "0 12 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {"email", "tesouro", "orcamento", "ingestion"} <= set(dag.tags)
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_dataset_spec_reads_the_zip_of_the_day(module: Any) -> None:
    spec = module.DATASET
    config = spec.extractor_config()
    mail = config.mail

    assert (spec.domain, spec.dataset) == (
        "siafi-tesouro-gerencial",
        "orcamento_mcid_por_acao",
    )
    assert spec.load_mode is LoadMode.OVERWRITE
    assert config.source == "email"
    assert mail.subject == "orcamento_mcid_por_acao"
    assert mail.credentials_variable == "email_credentials"
    assert re.match(mail.attachment_pattern, "orcamento.zip")
    assert not re.match(mail.attachment_pattern, "assinatura.png")
    # TSV UTF-16; 5 linhas de preâmbulo antes do cabeçalho.
    assert (
        spec.converter.encoding,
        spec.converter.delimiter,
        spec.converter.skip_rows,
    ) == ("utf-16", "\t", 5)


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
