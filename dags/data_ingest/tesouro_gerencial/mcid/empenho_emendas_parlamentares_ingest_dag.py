"""Notas de empenho de emendas parlamentares do MCid (Tesouro Gerencial).

Fonte: e-mail do Tesouro Gerencial (SERPRO) com o relatório em anexo, um ZIP com
um CSV em UTF-16. A credencial do IMAP vem da Variable `email_credentials`, lida
só dentro da task. O ZIP vai para a raw como chegou.

LoadMode: overwrite. Cada e-mail traz o relatório do ano de lançamento inteiro,
então a última ingestão é a verdade. Sem e-mail no dia, a task é pulada (skip).

Estrutura do CSV: 9 linhas de preâmbulo (título e filtros do relatório),
cabeçalho na linha 10 (as colunas de código e nome vêm em pares, a segunda sem
título) e duas linhas de subcabeçalho das colunas de valor antes dos dados. A
staging guarda tudo; ainda não há prata (nenhum modelo lê este relatório).
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
    dataset="notas_empenho_emendas_parlamentares_mcid",
    extractor=ExtractorConfig(
        source="email",
        conn_id="imap_tesouro",
        mail=MailQuery(
            subject="notas_empenho_emendas_parlamentares_mcid",
            attachment_pattern=r".*\.zip$",
            credentials_variable="email_credentials",
        ),
    ),
    converter=ConverterConfig(encoding="utf-16", delimiter=",", skip_rows=9),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="empenho_emendas_parlamentares_ingest_dag",
    # 09:00: o relatório chega de terça a sábado, de madrugada (como a dotação).
    schedule="0 9 * * *",
    start_date=datetime(2026, 3, 25),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Lucas",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["email", "empenhos", "tesouro", "emendas", "mcid", "ingestion"],
)
def empenho_emendas_parlamentares_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> list[str]:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefixes: list[str]) -> str:
        return steps.convert_to_staging(DATASET, raw_prefixes)

    convert_to_staging(extract_to_raw())


dag_instance = empenho_emendas_parlamentares_ingest_dag()
