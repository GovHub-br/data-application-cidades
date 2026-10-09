"""Orçamento do MCid por ação (Tesouro Gerencial): dotação, empenho e pagamento.

Fonte: e-mail do Tesouro Gerencial (SERPRO) com o relatório em anexo, um ZIP com
um TSV em UTF-16. A credencial do IMAP vem da Variable `email_credentials`, lida
só dentro da task. O ZIP vai para a raw como chegou.

O relatório chegou só duas vezes no último ano (23/03 e 27/03/2026, às 11:32 e
09:03); sem e-mail no dia, a task é pulada (skip). Se voltar a chegar, entra.

LoadMode: overwrite. Cada e-mail traz o relatório inteiro do exercício.

Estrutura do TSV (conferida no anexo de 27/03/2026): 5 linhas de preâmbulo
(título e filtros), cabeçalho na linha 6 (pares código/nome, o segundo sem
título) e duas linhas de subcabeçalho das colunas de valor antes dos dados. A
DAG antiga pulava 10 linhas, o que nesse arquivo descartaria o cabeçalho e as
primeiras linhas de dado. Ainda não há prata (nenhum modelo lê este relatório).
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
    dataset="orcamento_mcid_por_acao",
    extractor=ExtractorConfig(
        source="email",
        conn_id="imap_tesouro",
        mail=MailQuery(
            subject="orcamento_mcid_por_acao",
            attachment_pattern=r".*\.zip$",
            credentials_variable="email_credentials",
        ),
    ),
    converter=ConverterConfig(encoding="utf-16", delimiter="\t", skip_rows=5),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="orcamento_mcid_por_acao_ingest_dag",
    # 12:00: os dois e-mails conhecidos chegaram às 09:03 e às 11:32.
    schedule="0 12 * * *",
    start_date=datetime(2026, 3, 25),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Lucas",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["email", "orcamento", "tesouro", "mcid", "ingestion"],
)
def orcamento_mcid_por_acao_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> str:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefix: str) -> str:
        return steps.convert_to_staging(DATASET, raw_prefix)

    convert_to_staging(extract_to_raw())


dag_instance = orcamento_mcid_por_acao_ingest_dag()
