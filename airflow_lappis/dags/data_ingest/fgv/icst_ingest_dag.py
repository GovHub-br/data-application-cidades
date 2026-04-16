import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from cliente_fgv import ClienteFGVDados
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule


@dag(
    schedule_interval=get_dynamic_schedule("icst_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fgv", "icst", "construcao", "confianca"],
)
def icst_ingest_dag() -> None:
    """
    DAG para ingestão de dados históricos do ICST da FGV no PostgreSQL.
    """

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
    def fetch_and_store_icst() -> None:
        """
        Baixa o CSV do ICST autenticado e faz upsert no Postgres.
        """
        logging.info("Iniciando processamento do ICST Histórico")

        # Busca as credenciais de forma segura usando Variables do Airflow
        email_fgv = Variable.get("dados_fgv_email")
        senha_fgv = Variable.get("dados_fgv_password")

        api = ClienteFGVDados(email=email_fgv, password=senha_fgv)
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        tabela = "icst"

        registros = api.fetch_icst_historico()

        if registros:
            logging.info(f"Inserindo {len(registros)} registros em fgv.{tabela}")

            db.insert_data(
                data=registros,
                table_name=tabela,
                conflict_fields=["mes"],
                primary_key=["mes"],
                schema="fgv",
            )

            logging.info(f"Ingestão da {tabela} concluída com sucesso.")
        else:
            logging.warning("Nenhum registro extraído para ICST Histórico da FGV.")

    setup = setup_schema()
    ingest_icst = fetch_and_store_icst()

    setup >> ingest_icst


dag_instance = icst_ingest_dag()
