import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from clientes.schedule_loader import get_dynamic_schedule
from helpers.postgres_helpers import get_postgres_conn
from clientes.cliente_novo_caged import ClienteNovoCaged
from clientes.cliente_postgres import ClientPostgresDB
from clientes.cliente_minio import upload_raw_json
from clientes.ingestor_lake import registros_para_staging_parquet


@dag(
    schedule=get_dynamic_schedule(
        "novo_caged_construcao_edificios", default="@monthly"
    ),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Milena Rocha",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["cidades", "novo_caged", "construcao", "saldo", "estoque"],
)
def novo_caged_construcao_edificios() -> None:
    """DAG para buscar e armazenar dados de saldo e estoque na construção
    de edifícios (Novo Caged)."""

    @task
    def fetch_and_store_caged() -> None:
        logging.info(
            "[saldo_estoque_construcao_edificios.py] Iniciando extração (Novo Caged)"
        )

        api = ClienteNovoCaged()
        db = ClientPostgresDB(get_postgres_conn())

        target_table = "saldo_estoque_construcao_edificios"
        schema = "novo_caged"

        caged_data = api.obter_historico()

        if caged_data:
            for record in caged_data:
                record["dt_ingest"] = datetime.now().isoformat()

            logging.info(
                f"[saldo_estoque_construcao_edificios.py] Inserindo "
                f"{len(caged_data)} registros no schema {schema} na tabela {target_table}"
                f"schema dos dados {caged_data[0]}"
            )

            db.insert_data(
                caged_data,
                target_table,
                conflict_fields=["ano", "mes"],
                primary_key=["ano", "mes"],
                schema=schema,
            )

            # Lake (full-refresh): raw = json da API; parquet tipado.
            upload_raw_json("novo_caged", target_table, caged_data)
            registros_para_staging_parquet("novo_caged", target_table, caged_data)

            logging.info(
                f"[saldo_estoque_construcao_edificios.py] Concluído. "
                f"Total de {len(caged_data)} registros processados."
            )
        else:
            logging.warning(
                "[saldo_estoque_construcao_edificios.py] Nenhum dado retornado "
                "da API do Caged."
            )

    fetch_and_store_caged()


novo_caged_construcao_edificios()
