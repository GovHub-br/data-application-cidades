import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from schedule_loader import get_dynamic_schedule


@dag(
    schedule_interval=get_dynamic_schedule("sftp_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sftp", "minio", "mcid", "cidades", "data-lake"],
)
def sftp_ingest_dag() -> None:
    """Pipeline do data lake do MCid: SFTP -> raw/ -> mascaramento PII -> staging/ -> DW.

    Uma task por etapa, encadeadas. Cada etapa é idempotente/incremental (tabelas de
    controle no schema `sftp` do Postgres), então execuções agendadas só tocam o novo.

    Os scripts (montados em /opt/airflow/scripts) fazem o I/O via os clientes de plugins/
    (ClienteSftp, ClienteMinio). O mascaramento sobrescreve raw/ in-place — roda c/ apply.
    """

    @task
    def ingestao_sftp_para_minio() -> None:
        """Baixa os arquivos novos do SFTP e sobe para raw/ no MinIO."""
        from sftp_para_minio import run as run_ingest

        sucesso, erro = run_ingest()
        logging.info("[sftp_ingest_dag] SFTP->raw: %d ok, %d erro(s)", sucesso, erro)

    @task
    def mascaramento_pii() -> None:
        """Mascara PII (CPF/NIS/nome/endereço) nos objetos novos de raw/, in-place."""
        from mascarar_minio import run as run_mask

        contagem = run_mask(apply=True)
        logging.info("[sftp_ingest_dag] mascaramento concluído: %s", contagem)

    @task
    def raw_para_staging() -> None:
        """Converte os objetos de raw/ para Parquet em staging/."""
        from raw_para_staging import run as run_staging

        contagem = run_staging(apply=True)
        logging.info("[sftp_ingest_dag] raw->staging concluído: %s", contagem)

    @task
    def staging_para_db() -> None:
        """Materializa os parquets de staging/ como tabelas no DW (Postgres)."""
        from staging_para_db import run as run_db

        contagem = run_db(apply=True)
        logging.info("[sftp_ingest_dag] staging->DW concluído: %s", contagem)

    (
        ingestao_sftp_para_minio()
        >> mascaramento_pii()
        >> raw_para_staging()
        >> staging_para_db()
    )


dag_instance = sftp_ingest_dag()
