import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException

from cliente_bacen import ClienteBacen
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

logger = logging.getLogger(__name__)

default_args = {
    "owner": "Lucas Bottino",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SERIES_BACEN = {
    20704: "pf_concessoes_rs_mi",
    20774: "pf_taxa_juros_aa",
    21151: "pf_inadimplencia_pct",
    20692: "pj_concessoes_rs_mi",
    20763: "pj_taxa_juros_aa",
    21139: "pj_inadimplencia_pct",
}


@dag(
    dag_id="bacen_financiamentos_imobiliarios_dag",
    schedule_interval=get_dynamic_schedule("bacen_financiamentos_imobiliarios"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bacen", "financiamentos", "imobiliarios"],
)
def bacen_financiamentos_imobiliarios_dag() -> None:
    """
    DAG de ingestão incremental trimestral do BACEN SGS.
    Puxa os 3 meses do trimestre anterior à data de execução.
    Depende da carga histórica prévia via
    bacen_financiamentos_imobiliarios_historico_dag.
    """

    @task
    def fetch_and_store() -> None:
        logger.info("[bacen_dag] Iniciando ingestão trimestral BACEN")

        try:
            exec_date = datetime.now()

            # Identifica o trimestre anterior completo
            mes_atual = exec_date.month
            ano_atual = exec_date.year

            primeiro_mes_trimestre_atual = ((mes_atual - 1) // 3) * 3 + 1

            ultimo_dia_trim_anterior = datetime(
                ano_atual, primeiro_mes_trimestre_atual, 1
            ) - timedelta(days=1)

            primeiro_mes_trim_anterior = ((ultimo_dia_trim_anterior.month - 1) // 3) * 3 + 1
            primeiro_dia_trim_anterior = datetime(
                ultimo_dia_trim_anterior.year, primeiro_mes_trim_anterior, 1
            )

            data_ini = primeiro_dia_trim_anterior.strftime("%d/%m/%Y")
            data_fim = ultimo_dia_trim_anterior.strftime("%d/%m/%Y")

            logger.info(
                "[bacen_dag] Buscando trimestre: %s até %s",
                data_ini,
                data_fim,
            )

            cliente = ClienteBacen()
            df = cliente.fetch_and_transform(
                SERIES_BACEN,
                data_ini,
                data_fim,
            )

            if df is None:
                raise AirflowFailException(
                    f"[bacen_dag] ClienteBacen falhou ao buscar séries. "
                    f"Verifique os códigos SGS: {list(SERIES_BACEN.keys())}"
                )

            if df.empty:
                raise AirflowSkipException(
                    f"[bacen_dag] Nenhum dado retornado pelo BACEN para o período "
                    f"{data_ini} até {data_fim} — publicação pode estar pendente."
                )

            registros = df.to_dict(orient="records")
            dt_ingest = datetime.now().isoformat()
            for r in registros:
                r["dt_ingest"] = dt_ingest
                r["fonte"] = "BACEN_SGS"

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            logger.info(
                "[bacen_dag] Inserindo %d registros em bacen.financiamentos_imobiliarios",
                len(registros),
            )

            db.insert_data(
                registros,
                table_name="financiamentos_imobiliarios",
                schema="bacen",
                conflict_fields=["data_referencia"],
                primary_key=["data_referencia"],
            )

            logger.info("[bacen_dag] Ingestão trimestral concluída com sucesso")

        except (AirflowFailException, AirflowSkipException):
            raise
        except Exception as e:
            logger.error("[bacen_dag] Erro inesperado na ingestão BACEN: %s", e)
            raise AirflowException(
                f"[bacen_dag] Erro inesperado na ingestão BACEN: {e}"
            ) from e

    fetch_and_store()


dag_instance = bacen_financiamentos_imobiliarios_dag()
