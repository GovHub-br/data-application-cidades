import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from schedule_loader import get_dynamic_schedule


@dag(
    schedule=get_dynamic_schedule("sftp_ingest_dag", default="0 0 * * *"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    params={"reingest_deleted": False},
    tags=["sftp", "minio", "mcid", "cidades", "data-lake", "ingest"],
)
def sftp_ingest_dag() -> None:
    """Cadeia diária: SFTP -> raw -> staging -> Bronze/Prata/Gold -> painel.

    Idempotente/incremental pela tabela de controle lake._ingest_minio_log: execuções
    agendadas só baixam arquivos novos. O I/O usa os clientes de plugins/
    (ClienteSftp/ClienteMinio). Cada etapa seguinte é aguardada; dbt só inicia
    após staging leve e CadÚnico estarem consistentes.
    """

    @task
    def ingestao_sftp_para_minio() -> None:
        """Baixa os arquivos novos do SFTP e sobe para raw/ no MinIO.

        Por padrão não reingere arquivos já ingeridos (mesmo deletados). Para reingerir os
        que foram removidos do MinIO, dispare a DAG com config {"reingest_deleted": true}.
        """
        from sftp_para_minio import run as run_ingest

        params = get_current_context()["params"]
        reingest_deleted = bool(params.get("reingest_deleted", False))
        sucesso, erro = run_ingest(reingest_deleted=reingest_deleted)
        logging.info(
            "[sftp_ingest_dag] SFTP->raw (reingest_deleted=%s): %d ok, %d erro(s)",
            reingest_deleted,
            sucesso,
            erro,
        )

    ingestao = ingestao_sftp_para_minio()
    transformar_lake = TriggerDagRunOperator(
        task_id="transformar_raw_em_staging",
        trigger_dag_id="minio_transform_dag",
        wait_for_completion=True,
        reset_dag_run=False,
    )
    transformar_cadunico = TriggerDagRunOperator(
        task_id="transformar_cadunico_em_staging",
        trigger_dag_id="cadunico_transform_dag",
        wait_for_completion=True,
        reset_dag_run=False,
    )
    materializar_gold = TriggerDagRunOperator(
        task_id="materializar_bronze_prata_gold",
        trigger_dag_id="mcid_cosmos_dag",
        wait_for_completion=False,
        reset_dag_run=False,
    )
    ingestao >> transformar_lake >> transformar_cadunico >> materializar_gold


dag_instance = sftp_ingest_dag()
