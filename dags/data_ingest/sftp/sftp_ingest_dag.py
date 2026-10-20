import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk import get_current_context
from schedule_loader import get_dynamic_schedule


@dag(
    schedule=get_dynamic_schedule("sftp_ingest_dag"),
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
    """Ingestão do SFTP do MCid para a camada raw/ do data lake (MinIO).

    Idempotente/incremental pela tabela de controle lake._ingest_minio_log: execuções
    agendadas só baixam arquivos novos. O I/O usa os clientes de plugins/
    (ClienteSftp/ClienteMinio).

    Depois da ingestão, cada arquivo pousado é comparado com a entrega anterior da
    mesma série (drift estrutural, `scripts/sftp_drift.py`): colunas, volume e
    linhas novas/removidas/alteradas, sem ler as colunas de PII. O relatório vai
    para drift/sftp/ e o drift só gera aviso.
    """

    @task
    def ingestao_sftp_para_minio() -> list[str]:
        """Baixa os arquivos novos do SFTP e sobe para raw/ no MinIO.

        Por padrão não reingere arquivos já ingeridos (mesmo deletados). Para reingerir os
        que foram removidos do MinIO, dispare a DAG com config {"reingest_deleted": true}.
        """
        from sftp_para_minio import run as run_ingest

        params = get_current_context()["params"]
        reingest_deleted = bool(params.get("reingest_deleted", False))
        sucesso, erro, pousadas = run_ingest(reingest_deleted=reingest_deleted)
        logging.info(
            "[sftp_ingest_dag] SFTP->raw (reingest_deleted=%s): %d ok, %d erro(s)",
            reingest_deleted,
            sucesso,
            erro,
        )
        return pousadas

    @task
    def detectar_drift(pousadas: list[str]) -> list[str]:
        """Compara cada arquivo pousado com a entrega anterior da série."""
        from sftp_drift import detectar

        return detectar(pousadas)

    detectar_drift(ingestao_sftp_para_minio())


dag_instance = sftp_ingest_dag()
