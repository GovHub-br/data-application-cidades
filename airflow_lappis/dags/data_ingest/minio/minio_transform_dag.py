import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from schedule_loader import get_dynamic_schedule


@dag(
    schedule_interval=get_dynamic_schedule("minio_transform_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["minio", "mcid", "cidades", "data-lake", "transform"],
)
def minio_transform_dag() -> None:
    """Pipeline de transformação do data lake: raw/ -> mascaramento PII -> staging/ -> DW.

    Roda sobre o que estiver em raw/ no MinIO, INDEPENDENTE de quem carregou (SFTP hoje,
    outras fontes no futuro). Desacoplada da ingestão: schedule próprio, sem referência
    às DAGs de fonte.

    Três etapas encadeadas, cada uma idempotente/incremental (tabelas de controle no
    schema `sftp` do Postgres), então execuções agendadas só processam o que é novo.
    `max_active_runs=1` porque o mascaramento sobrescreve raw/ in-place — duas execuções
    concorrentes seriam corrida destrutiva. O I/O usa os clientes de plugins/.
    """

    @task
    def mascaramento_pii() -> None:
        """Mascara PII (CPF/NIS/nome/endereço) nos objetos novos de raw/, in-place."""
        from mascarar_minio import run as run_mask

        contagem = run_mask(apply=True)
        logging.info("[minio_transform_dag] mascaramento concluído: %s", contagem)

    @task
    def raw_para_staging() -> None:
        """Converte os objetos de raw/ para Parquet em staging/."""
        from raw_para_staging import run as run_staging

        contagem = run_staging(apply=True)
        logging.info("[minio_transform_dag] raw->staging concluído: %s", contagem)

    @task
    def staging_para_db() -> None:
        """Materializa os parquets de staging/ como tabelas no DW (Postgres)."""
        from staging_para_db import run as run_db

        contagem = run_db(apply=True)
        logging.info("[minio_transform_dag] staging->DW concluído: %s", contagem)

    mascaramento_pii() >> raw_para_staging() >> staging_para_db()


dag_instance = minio_transform_dag()
