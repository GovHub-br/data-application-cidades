import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_siconv import ClienteSiconv
from tabelas_siconv import TABELAS_SICONV


@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "Luana",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["siconv", "data_ingest"],
)
def siconv_ingestao_dag() -> None:

    @task
    def baixar_siconv() -> str:
        cliente = ClienteSiconv()
        cliente.baixar_zip()
        return cliente.ZIP_PATH

    @task
    def ingerir_tabela(zip_path: str, nome_tabela: str, nome_csv: str, conflict_fields: list, primary_key: list, skip_rows: int, colunas: list) -> None:
        logging.info(f"[siconv_ingest_dag.py] Iniciando ingestão da tabela {nome_tabela}")
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        cliente = ClienteSiconv()
        registros = cliente.ler_csv(nome_csv, skip_rows, colunas_esperadas=colunas)

        if not registros:
            logging.warning(f"[siconv_ingest_dag.py] Nenhum registro encontrado em {nome_csv}")
            return

        db.insert_data(
            registros,
            nome_tabela,
            conflict_fields=conflict_fields,
            primary_key=primary_key,
            schema="siconv",
        )

        logging.info(f"[siconv_ingest_dag.py] {len(registros)} registros inseridos em {nome_tabela}")

    zip_path = baixar_siconv()
    for tabela in TABELAS_SICONV:
        ingerir_tabela(
            zip_path=zip_path,
            nome_tabela=tabela["nome_tabela"],
            nome_csv=tabela["nome_csv"],
            conflict_fields=tabela["conflict_fields"],
            primary_key=tabela["primary_key"],
            skip_rows=tabela["skip_rows"],
            colunas=tabela["colunas"],
        )


siconv_ingestao_dag()