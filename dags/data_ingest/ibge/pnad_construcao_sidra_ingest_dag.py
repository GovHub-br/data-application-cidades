import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from clientes.schedule_loader import get_dynamic_schedule
from helpers.postgres_helpers import get_postgres_conn
from clientes.cliente_ibge_sidra import ClienteIbgeSidra
from clientes.cliente_postgres import ClientPostgresDB
from clientes.cliente_minio import upload_raw_json
from clientes.ingestor_lake import registros_para_staging_parquet
import pandas as pd

# PNAD-C por grupamento de atividade (classificação 888):
#   categorias 47946 = Total, 47949 = Construção.
CLASSIFICACAO = 888
CATEGORIAS = [47946, 47949]

CONFIGS = [
    {"tabela": "pnad_construcao_ocupados", "agregado": 6323, "variavel": 4090},
    {"tabela": "pnad_construcao_rendimento", "agregado": 6391, "variavel": 5932},
]


@dag(
    dag_id="ibge_pnad_construcao_sidra_ingest_dag",
    schedule=get_dynamic_schedule(
        "ibge_pnad_construcao_sidra_ingest_dag", default="@monthly"
    ),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ibge", "pnad", "construcao", "sidra", "conjuntura"],
)
def ibge_pnad_construcao_sidra_ingest_dag() -> None:
    """PNAD-C construção (ocupados + rendimento médio real) via SIDRA — pág. 3.

    Usa a API SIDRA porque o endpoint /dados do IBGE v3 está retornando HTTP 500
    para os agregados 6323/6391. Categorias 47946 (Total) e 47949 (Construção).
    """

    @task
    def fetch_and_store(config: dict) -> None:
        tabela = config["tabela"]
        api = ClienteIbgeSidra()
        db = ClientPostgresDB(get_postgres_conn())

        registros = api.obter(
            config["agregado"],
            config["variavel"],
            CLASSIFICACAO,
            CATEGORIAS,
            periodos="last 12",
        )
        if not registros:
            logging.warning(f"Nenhum dado SIDRA para ibge.{tabela}")
            return

        # Postgres: upsert por (periodo, categoria_id) -> preserva histórico.
        db.insert_data(
            registros,
            tabela,
            conflict_fields=["periodo", "categoria_id"],
            primary_key=["periodo", "categoria_id"],
            schema="ibge",
        )

        # Lake (full-refresh): raw = json da API SIDRA; parquet tipado.
        upload_raw_json("ibge", tabela, registros)
        registros_para_staging_parquet(
            "ibge",
            tabela,
            registros,
            typers={"valor": lambda s: pd.to_numeric(s, errors="coerce")},
        )
        logging.info(f"SIDRA {tabela}: {len(registros)} registros ingeridos.")

    fetch_and_store.expand(config=CONFIGS)


dag_instance = ibge_pnad_construcao_sidra_ingest_dag()
