import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_fgv import ClienteFGV
from cliente_postgres import ClientPostgresDB

@dag(
    schedule_interval=get_dynamic_schedule("incc_m_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fgv", "incc_m", "construcao", "custos"],
)
def incc_m_ingest_dag() -> None:
    """DAG para ingestão de dados do INCC-M da FGV no PostgreSQL."""

    @task
    def setup_schema() -> None:
        """
        Cria o schema da FGV antes do processamento.
        """
        import psycopg2
        postgres_conn_str = get_postgres_conn()
        schema = "fgv"

        with psycopg2.connect(postgres_conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.commit()

        logging.info(f"Schema '{schema}' garantido com sucesso.")

    @task
    def fetch_and_store_incc() -> None:
        """
        Baixa o arquivo do INCC, trata os dados via Pandas e faz upsert do Postgres.
        """
        logging.info("Iniciando processamento do INCC-M")

        api = ClienteFGV()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        tabela = "incc_m"

        registros = api.fetch_and_transform_incc()

        if registros:
            logging.info(f"Inserindo {len(registros)} registros em fgv.{tabela}")

            db.insert_data(
                data=registros,
                table_name=tabela,
                conflict_fields=["mes"],
                primary_key=["mes"],
                schema="fgv",
            )
            logging.info(f"Ingestão de {tabela} concluída com sucesso.")
        else:
            logging.warning("Nenhum registro extraído para INCC-M da FGV.")


    setup = setup_schema()
    ingest_incc = fetch_and_store_incc()

    setup >> ingest_incc

    
dag_instance = incc_m_ingest_dag()

