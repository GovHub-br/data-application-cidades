import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_bacen_imobiliario import ClienteBacenImobiliario
from cliente_postgres import ClientPostgresDB
from cliente_minio import upload_raw_json
from base_file_parser import registros_para_staging_parquet


@dag(
    dag_id="bacen_credito_pib_ingest_dag",
    schedule_interval=get_dynamic_schedule(
        "bacen_credito_pib_ingest_dag", default="@monthly"
    ),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bacen", "imobiliario", "credito_pib", "conjuntura"],
)
def bacen_credito_pib_ingest_dag() -> None:
    """Ingestão do Crédito Imobiliário / PIB (BCB Olinda MercadoImobiliario).

    Página 4 do boletim. Série mensal em % do PIB.
    """

    @task
    def fetch_and_store() -> None:
        api = ClienteBacenImobiliario()
        db = ClientPostgresDB(get_postgres_conn())
        tabela = "credito_imobiliario_pib"

        registros = api.obter_credito_pib()
        if not registros:
            logging.warning("Nenhum dado retornado (crédito/PIB BCB).")
            return

        # Postgres: upsert por data -> preserva histórico (trimestral).
        db.insert_data(
            registros,
            tabela,
            conflict_fields=["data"],
            primary_key=["data"],
            schema="bacen",
        )

        # Lake (full-refresh): raw = json da API; parquet tipado.
        upload_raw_json("bacen", tabela, registros)
        registros_para_staging_parquet("bacen", tabela, registros)
        logging.info(f"Crédito/PIB: {len(registros)} pontos ingeridos.")

    fetch_and_store()


dag_instance = bacen_credito_pib_ingest_dag()
