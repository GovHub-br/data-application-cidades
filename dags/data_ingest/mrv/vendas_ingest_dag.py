import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_mrv import ClienteMRV
from cliente_postgres import ClientPostgresDB
from cliente_minio import upload_raw_json
from base_file_parser import registros_para_staging_parquet


@dag(
    schedule_interval=get_dynamic_schedule("vendas_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mrv", "vendas", "operacionais", "custos"],
)
def vendas_ingest_dag() -> None:
    """DAG para ingestão de dados das Vendas Líquidas da Empresa MRV
    no PostgreSQL.
    """

    @task
    def fetch_and_store_vendas() -> None:
        """
        Baixa o arquivo mais recente das Vendas Líquidas, trata os dados via Pandas
        e faz upsert do Postgres.
        """
        logging.info("Iniciando processamento das Vendas...")

        api = ClienteMRV()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        tabela = "vendas_liquidas"

        registros = api.fetch_dados_vendas()

        if registros:
            logging.info(f"Inserindo {len(registros)} registros em mrv.{tabela}")

            # Postgres: upsert por periodo -> preserva histórico (trimestral).
            db.insert_data(
                data=registros,
                table_name=tabela,
                conflict_fields=["periodo"],
                primary_key=["periodo"],
                schema="mrv",
            )

            # Lake (full-refresh) para o conjuntura contínuo: raw + parquet tipado.
            upload_raw_json("mrv", tabela, registros)
            registros_para_staging_parquet("mrv", tabela, registros)

            logging.info(f"Ingestão de {tabela} concluída com sucesso.")
        else:
            logging.warning("Nenhum registro extraído para Vendas Líquidas da MRV.")

    fetch_and_store_vendas()


dag_instance = vendas_ingest_dag()
