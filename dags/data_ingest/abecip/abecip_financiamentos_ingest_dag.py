import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException

from cliente_abecip import ClienteAbecip
from cliente_postgres import ClientPostgresDB
from cliente_minio import upload_raw_bytes, upload_fallback_json
from ingestor_lake import registros_para_staging_parquet
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

logger = logging.getLogger(__name__)

default_args = {
    "owner": "Lucas Bottino",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="abecip_financiamentos_ingest_dag",
    schedule_interval=get_dynamic_schedule("abecip_financiamentos"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["abecip", "financiamentos", "sbpe", "conjuntura"],
)
def abecip_financiamentos_ingest_dag() -> None:
    """Ingestão da série mensal de financiamentos SBPE por modalidade (ABECIP).

    Origem do indicador "Financiamentos Habitacionais (UH) — SBPE Const." do
    boletim de conjuntura, que até então era preenchido à mão.

    A ABECIP republica a série inteira a cada divulgação (e revisa meses
    anteriores), então a carga é sempre da série completa, com upsert por
    `data_referencia` — não incremental.
    """

    @task
    def fetch_and_store() -> None:
        logger.info("[abecip_financiamentos] Iniciando ingestão")

        try:
            cliente = ClienteAbecip()
            df = cliente.fetch_and_transform_financiamentos()

            if df is None:
                raise AirflowFailException(
                    "[abecip_financiamentos] ClienteAbecip falhou ao baixar ou "
                    "processar o XLSX de financiamentos."
                )

            if df.empty:
                raise AirflowSkipException(
                    "[abecip_financiamentos] DataFrame vazio — XLSX pode estar "
                    "indisponível ou sem dados."
                )

            registros = df.to_dict(orient="records")

            db = ClientPostgresDB(get_postgres_conn())
            logger.info(
                "[abecip_financiamentos] Inserindo %d registros em "
                "abecip.financiamentos_modalidade",
                len(registros),
            )

            # Upsert por data_referencia: a ABECIP revisa meses já publicados.
            db.insert_data(
                registros,
                table_name="financiamentos_modalidade",
                schema="abecip",
                conflict_fields=["data_referencia"],
                primary_key=["data_referencia"],
            )

            # Lake: raw nativo (xlsx) + fallback json + parquet tipado.
            bruto = getattr(cliente, "ultimo_conteudo_xlsx_financiamentos", None)
            if bruto:
                upload_raw_bytes("abecip", "financiamentos_modalidade", bruto, ext="xlsx")
            upload_fallback_json("abecip", "financiamentos_modalidade", registros)
            registros_para_staging_parquet("abecip", "financiamentos_modalidade", registros)

            logger.info("[abecip_financiamentos] Ingestão concluída com sucesso")

        except (AirflowFailException, AirflowSkipException):
            raise
        except Exception as e:
            logger.error("[abecip_financiamentos] Erro inesperado: %s", e)
            raise AirflowException(f"[abecip_financiamentos] Erro inesperado: {e}") from e

    fetch_and_store()


dag_instance = abecip_financiamentos_ingest_dag()
