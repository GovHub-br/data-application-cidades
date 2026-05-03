import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException

from cliente_abecip import ClienteAbecip
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn

logger = logging.getLogger(__name__)

default_args = {
    "owner": "Lucas Bottino",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="abecip_poupanca_historico_ingest_dag",
    schedule_interval=None,  # Execução manual — carga histórica única
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["abecip", "poupanca", "historico"],
)
def abecip_poupanca_historico_ingest_dag() -> None:
    """
    DAG de carga histórica da Caderneta de Poupança ABECIP.
    Execução manual — deve ser rodada uma única vez para popular
    a tabela base antes de ativar a DAG incremental trimestral.
    """

    @task
    def fetch_and_store_historico() -> None:
        logger.info(
            "[abecip_poupanca_historico_dag] Iniciando carga histórica"
        )

        try:
            cliente = ClienteAbecip()
            df = cliente.fetch_and_transform_poupanca()

            if df is None:
                raise AirflowFailException(
                    "[abecip_poupanca_historico_dag] ClienteAbecip falhou "
                    "ao baixar ou processar o XLSX de poupança."
                )

            if df.empty:
                raise AirflowSkipException(
                    "[abecip_poupanca_historico_dag] DataFrame retornado "
                    "está vazio — nenhum dado disponível no XLSX."
                )

            registros = df.to_dict(orient="records")
            for r in registros:
                r["fonte"] = "ABECIP"

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            logger.info(
                "[abecip_poupanca_historico_dag] Inserindo %d registros "
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
                "[abecip_poupanca_historico_dag] Carga histórica concluída"
            )

        except (AirflowFailException, AirflowSkipException):
            raise
        except Exception as e:
            logger.error(
                "[abecip_poupanca_historico_dag] Erro inesperado: %s", e
            )
            raise AirflowException(
                f"[abecip_poupanca_historico_dag] Erro inesperado: {e}"
            ) from e

    fetch_and_store_historico()


dag_instance = abecip_poupanca_historico_ingest_dag()
