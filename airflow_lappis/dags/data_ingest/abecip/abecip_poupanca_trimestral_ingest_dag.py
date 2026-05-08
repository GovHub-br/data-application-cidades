import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException

from cliente_abecip import ClienteAbecip
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

logger = logging.getLogger(__name__)

default_args = {
    "owner": "Lucas Bottino",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="abecip_poupanca_trimestral_ingest_dag",
    schedule_interval=get_dynamic_schedule("abecip_poupanca_trimestral"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["abecip", "poupanca"],
)
def abecip_poupanca_trimestral_ingest_dag() -> None:
    """
    DAG de ingestão incremental trimestral da Caderneta de Poupança ABECIP.
    A cada execução baixa o XLSX completo e faz upsert de toda a série —
    garantindo que revisões retroativas sejam capturadas automaticamente.
    """

    @task
    def fetch_and_store() -> None:
        logger.info(
            "[abecip_poupanca_trimestral_dag] Iniciando ingestão trimestral"
        )

        try:
            cliente = ClienteAbecip()
            df = cliente.fetch_and_transform_poupanca()

            if df is None:
                raise AirflowFailException(
                    "[abecip_poupanca_trimestral_dag] ClienteAbecip falhou "
                    "ao baixar ou processar o XLSX de poupança."
                )

            if df.empty:
                raise AirflowSkipException(
                    "[abecip_poupanca_trimestral_dag] DataFrame retornado "
                    "está vazio — XLSX pode estar indisponível."
                )

            registros = df.to_dict(orient="records")
            for r in registros:
                r["fonte"] = "ABECIP"

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            logger.info(
                "[abecip_poupanca_trimestral_dag] Inserindo %d registros "
                "em abecip.poupanca_sbpe_mensal",
                len(registros),
            )

            db.insert_data(
                registros,
                table_name="poupanca_sbpe_mensal",
                schema="abecip",
                conflict_fields=["data_referencia"],
                primary_key=["data_referencia"],
            )

            logger.info(
                "[abecip_poupanca_trimestral_dag] Ingestão trimestral concluída"
            )

        except (AirflowFailException, AirflowSkipException):
            raise
        except Exception as e:
            logger.error(
                "[abecip_poupanca_trimestral_dag] Erro inesperado: %s", e
            )
            raise AirflowException(
                f"[abecip_poupanca_trimestral_dag] Erro inesperado: {e}"
            ) from e

    fetch_and_store()


dag_instance = abecip_poupanca_trimestral_ingest_dag()
