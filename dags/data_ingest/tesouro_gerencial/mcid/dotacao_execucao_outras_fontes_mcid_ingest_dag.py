"""Dotação e execução de outras fontes do MCid (relatório do Tesouro Gerencial).

Fonte: e-mail diário do Tesouro Gerencial com o relatório em anexo, um ZIP com um
TSV em UTF-16. A credencial do IMAP e o remetente vêm da Variable
`email_credentials`, lida só dentro da task. O ZIP vai para a raw como chegou; a
conversão abre o membro com o encoding, o delimitador e o preâmbulo daqui.

LoadMode: overwrite. Cada e-mail traz o relatório completo do exercício, então a
última ingestão é a verdade. Sem e-mail no dia, a task é pulada (skip), como antes.

O bronze é `bronze_siafi_dotacao_execucao` (dbt, sobre o `latest/`); renomear as
colunas do relatório e tratar os valores em pt-BR é da prata.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, MailQuery
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps

DATASET = DatasetSpec(
    domain="siafi-tesouro-gerencial",
    dataset="dotacao_execucao_outras_fontes_mcid",
    extractor=ExtractorConfig(
        source="email",
        conn_id="imap_tesouro",
        mail=MailQuery(
            subject="dotacao_execucao_outras_fontes_mcid",
            attachment_pattern=r".*\.zip$",
            credentials_variable="email_credentials",
        ),
    ),
    # Provisório: o preâmbulo do relatório é conferido no anexo real na validação
    # da Fase 5 (a DAG antiga pulava 12 linhas e não lia cabeçalho).
    converter=ConverterConfig(encoding="utf-16", delimiter="\t", skip_rows=11),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="dotacao_execucao_outras_fontes_mcid_ingest_dag",
    # Provisório: o cron real vem da Variable dynamic_schedules na validação da Fase 5.
    schedule="0 6 * * *",
    start_date=datetime(2026, 3, 27),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Lucas",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["email", "mcid", "tesouro", "dotacao", "execucao", "conjuntura", "ingestion"],
)
def dotacao_execucao_outras_fontes_mcid_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> str:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefix: str) -> str:
        return steps.convert_to_staging(DATASET, raw_prefix)

    convert_to_staging(extract_to_raw())


dag_instance = dotacao_execucao_outras_fontes_mcid_ingest_dag()
