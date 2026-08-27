import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from clientes.schedule_loader import get_dynamic_schedule
from helpers.postgres_helpers import get_postgres_conn
from clientes.cliente_bacen import ClienteBacen
from clientes.cliente_postgres import ClientPostgresDB
from clientes.cliente_minio import upload_raw_json
from clientes.ingestor_lake import registros_para_staging_parquet
import pandas as pd


@dag(
    schedule_interval=get_dynamic_schedule("bacen_sgs_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Mateus",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bacen", "sgs", "financiamento_imobiliario"],
)
def bacen_sgs_ingest_dag() -> None:
    """DAG para ingestão de séries temporais do SGS/BACEN no PostgreSQL.

    Itera sequencialmente sobre cada série configurada em BACEN_SERIES
    (Airflow Variable), fazendo uma requisição por vez e inserindo na tabela
    única `bacen.financiamentos_imobiliarios`. A coluna `tipo` diferencia cada
    série e compõe a chave primária junto com `data`.
    """

    @task
    def fetch_and_store_all_series() -> None:
        """Busca e armazena todas as séries SGS do BACEN sequencialmente.

        O loop serial evita race condition no CREATE TABLE IF NOT EXISTS,
        que ocorreria com múltiplas tasks paralelas escrevendo na mesma tabela.
        """
        api = ClienteBacen()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        # Lê o JSON do Airflow Variable e monta a lista de configs.
        # Deslocado para dentro da task para evitar parse frequente pelo
        # Top-Level do Scheduler.
        BACEN_SERIES_RAW = Variable.get(
            "BACEN_SERIES", deserialize_json=True, default_var={}
        )
        if isinstance(BACEN_SERIES_RAW, list):
            BACEN_SERIES_RAW = BACEN_SERIES_RAW[0] if len(BACEN_SERIES_RAW) > 0 else {}
        CONFIGURACOES = [{"tipo": k, "codigo": v} for k, v in BACEN_SERIES_RAW.items()]

        todas_series: list[dict] = []
        raw_por_serie: dict = {}

        for config in CONFIGURACOES:
            tipo = config["tipo"]
            codigo = config["codigo"]

            logging.info(f"Iniciando ingestão: tipo={tipo}, codigo={codigo}")

            dados = api.get_serie(codigo=codigo, ultimos=13)

            if not dados:
                logging.warning(f"Nenhum dado retornado da API BACEN para tipo={tipo}")
                continue

            # Raw nativo (API -> json): guarda o payload cru por série.
            raw_por_serie[tipo] = dados

            registros = [
                {
                    "tipo": tipo,
                    "data": registro["data"],
                    "valor": registro["valor"],
                    "dt_ingest": datetime.now().isoformat(),
                }
                for registro in dados
            ]

            logging.info(
                f"Inserindo {len(registros)} registros em "
                f"bacen.financiamentos_imobiliarios (tipo={tipo})"
            )

            # Postgres: upsert por (tipo, data) -> preserva histórico (trimestral).
            db.insert_data(
                registros,
                "financiamentos_imobiliarios",
                conflict_fields=["tipo", "data"],
                primary_key=["tipo", "data"],
                schema="bacen",
            )

            todas_series.extend(registros)
            logging.info(f"Ingestão de tipo={tipo} concluída com sucesso.")

        # Lake (full-refresh): raw = payload cru da API (json); parquet tipado.
        if todas_series:
            upload_raw_json("bacen", "financiamentos_imobiliarios", raw_por_serie)
            registros_para_staging_parquet(
                "bacen",
                "financiamentos_imobiliarios",
                todas_series,
                typers={
                    "data": lambda s: pd.to_datetime(s, dayfirst=True, errors="coerce"),
                    "valor": lambda s: pd.to_numeric(s, errors="coerce"),
                },
            )

    fetch_and_store_all_series()


dag_instance = bacen_sgs_ingest_dag()
