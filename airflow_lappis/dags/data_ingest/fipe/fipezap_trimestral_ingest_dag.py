import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException

from cliente_fipe import ClienteFipeZap
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
    dag_id="fipezap_trimestral_ingest_dag",
    schedule_interval=get_dynamic_schedule("fipezap_trimestral"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fipezap", "locacao"],
)
def fipezap_trimestral_ingest_dag() -> None:
    """
    DAG de ingestão incremental trimestral do Índice FipeZAP.
    A cada execução baixa o XLSX completo e faz upsert de toda a série —
    garantindo que revisões retroativas sejam capturadas automaticamente.
    """

    @task
    def fetch_and_store() -> None:
        logger.info("[fipezap_trimestral_dag] Iniciando ingestão trimestral FipeZAP")

        try:
            cliente = ClienteFipeZap()
            df = cliente.fetch_and_transform()

            if df is None:
                raise AirflowFailException(
                    "[fipezap_trimestral_dag] ClienteFipeZap falhou ao "
                    "baixar ou processar o XLSX."
                )

            if df.empty:
                raise AirflowSkipException(
                    "[fipezap_trimestral_dag] DataFrame retornado está vazio — "
                    "XLSX pode estar indisponível ou sem dados."
                )

            registros = df.to_dict(orient="records")
            for r in registros:
                r["fonte"] = "FIPEZAP"

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            logger.info(
                "[fipezap_trimestral_dag] Inserindo %d registros em "
                "fipezap.indice_locacao",
                len(registros),
            )

            db.insert_data(
                registros,
                table_name="indice_locacao",
                schema="fipe",
                conflict_fields=["data_referencia"],
                primary_key=["data_referencia"],
            )

            logger.info(
                "[fipezap_trimestral_dag] Ingestão trimestral concluída com sucesso"
            )

        except (AirflowFailException, AirflowSkipException):
            raise
        except Exception as e:
            logger.error(
                "[fipezap_trimestral_dag] Erro inesperado na ingestão: %s", e
            )
            raise AirflowException(
                f"[fipezap_trimestral_dag] Erro inesperado: {e}"
            ) from e

    fetch_and_store()


dag_instance = fipezap_trimestral_ingest_dag()
