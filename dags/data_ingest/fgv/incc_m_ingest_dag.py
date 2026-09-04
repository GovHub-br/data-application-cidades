import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_fgv import ClienteSinduscon
from cliente_postgres import ClientPostgresDB
from cliente_minio import upload_raw_bytes, upload_fallback_json
from ingestor_lake import registros_para_staging_parquet
import pandas as pd


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
    def fetch_and_store_incc() -> None:
        """
        Baixa o arquivo do INCC, trata os dados via Pandas e faz upsert do Postgres.
        """
        logging.info("Iniciando processamento do INCC-M")

        api = ClienteSinduscon()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        tabela = "incc_m"

        registros = api.fetch_and_transform_incc()

        if registros:
            logging.info(f"Inserindo {len(registros)} registros em fgv.{tabela}")

            # Postgres: upsert por mes -> preserva histórico (trimestral).
            db.insert_data(
                data=registros,
                table_name=tabela,
                conflict_fields=["mes"],
                primary_key=["mes"],
                schema="fgv",
            )

            # Raw nativo (XLSX) + fallback json + parquet tipado (full-refresh).
            # A série histórica traz '...' nas variações antigas -> to_numeric
            # coage p/ NaN e evita coluna object mista no parquet.
            raw_xlsx = getattr(api, "ultimo_conteudo_xlsx", None)
            if raw_xlsx:
                upload_raw_bytes("fgv", tabela, raw_xlsx, ext="xlsx")
            upload_fallback_json("fgv", tabela, registros)
            registros_para_staging_parquet(
                "fgv",
                tabela,
                registros,
                typers={
                    "mes": lambda s: pd.to_datetime(s, errors="coerce"),
                    "indice": lambda s: pd.to_numeric(s, errors="coerce"),
                    "var_mes": lambda s: pd.to_numeric(s, errors="coerce"),
                    "var_ano": lambda s: pd.to_numeric(s, errors="coerce"),
                    "var_12_meses": lambda s: pd.to_numeric(s, errors="coerce"),
                },
            )

            logging.info(f"Ingestão de {tabela} concluída com sucesso.")
        else:
            logging.warning("Nenhum registro extraído para INCC-M da FGV.")

    fetch_and_store_incc()


dag_instance = incc_m_ingest_dag()
