import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException

from cliente_fipe import ClienteFipeZap
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn

logger = logging.getLogger(__name__)

default_args = {
    "owner": "Lucas Bottino",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="fipezap_historico_ingest_dag",
    schedule_interval=None,  # Execução manual — carga histórica única
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fipezap", "locacao", "historico"],
)
def fipezap_historico_ingest_dag() -> None:
    """
    DAG de carga histórica do Índice FipeZAP — execução manual.
    Baixa a série histórica completa do XLSX e carrega no PostgreSQL.
    Deve ser rodada uma única vez antes de ativar a DAG incremental.

    Atenção: a série pode sofrer revisões retroativas a cada divulgação.
    Em caso de divergência, rodar esta DAG novamente regrava toda a série.
    """

    @task
    def fetch_and_store_historico() -> None:
        logger.info("[fipezap_historico_dag] Iniciando carga histórica FipeZAP")

        try:
            cliente = ClienteFipeZap()
            df = cliente.fetch_and_transform()

            if df is None:
                raise AirflowFailException(
                    "[fipezap_historico_dag] ClienteFipeZap falhou ao "
                    "baixar ou processar o XLSX."
                )

            if df.empty:
                raise AirflowSkipException(
                    "[fipezap_historico_dag] DataFrame retornado está vazio — "
                    "nenhum dado disponível no XLSX."
                )

            registros = df.to_dict(orient="records")
            for r in registros:
                r["fonte"] = "FIPEZAP"

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            logger.info(
                "[fipezap_historico_dag] Inserindo %d registros em "
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
                "[fipezap_historico_dag] Carga histórica concluída com sucesso"
            )

        except (AirflowFailException, AirflowSkipException):
            raise
        except Exception as e:
            logger.error(
                "[fipezap_historico_dag] Erro inesperado na carga histórica: %s", e
            )
            raise AirflowException(
                f"[fipezap_historico_dag] Erro inesperado: {e}"
            ) from e

    fetch_and_store_historico()


dag_instance = fipezap_historico_ingest_dag()
