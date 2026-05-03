import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowSkipException,
)

from cliente_bacen import ClienteBacen
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn

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

# Série histórica disponível a partir de 01/03/2011 (conforme SGS)
DATA_INICIO_HISTORICO = "01/03/2011"


@dag(
    dag_id="bacen_financiamentos_imobiliarios_historico_dag",
    schedule_interval=None,  # Execução manual — carga histórica única
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bacen", "financiamentos", "imobiliarios", "historico"],
)
def bacen_financiamentos_imobiliarios_historico_dag() -> None:
    """
    DAG de carga histórica do BACEN SGS — execução manual (schedule_interval=None).
    Puxa toda a série disponível desde 01/03/2011 até o mês anterior à execução.
    Deve ser rodada uma única vez para popular a tabela base antes
    de ativar a DAG incremental trimestral.
    """

    @task
    def fetch_and_store_historico() -> None:
        logger.info("[bacen_historico_dag] Iniciando carga histórica BACEN")

        try:
            exec_date = datetime.now()
            primeiro_dia_mes_atual = exec_date.replace(day=1)
            ultimo_dia_mes_anterior = primeiro_dia_mes_atual - timedelta(days=1)
            data_fim = ultimo_dia_mes_anterior.strftime("%d/%m/%Y")

            logger.info(
                "[bacen_historico_dag] Janela histórica: %s até %s",
                DATA_INICIO_HISTORICO,
                data_fim,
            )

            cliente = ClienteBacen()
            df = cliente.fetch_and_transform(
                SERIES_BACEN,
                DATA_INICIO_HISTORICO,
                data_fim,
            )

            if df is None:
                raise AirflowFailException(
                    "[bacen_historico_dag] ClienteBacen falhou buscando séries históricas."
                    f"Verifique os códigos SGS: {list(SERIES_BACEN.keys())}"
                )

            if df.empty:
                raise AirflowSkipException(
                    "[bacen_historico_dag] DataFrame retornado está vazio — "
                    "nenhum dado disponível para o período histórico solicitado."
                )

            registros = df.to_dict(orient="records")
            dt_ingest = datetime.now().isoformat()
            for r in registros:
                r["dt_ingest"] = dt_ingest
                r["fonte"] = "BACEN_SGS"

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            logger.info(
                "[bacen_historico_dag] Inserindo %d registros históricos em "
                "bacen.financiamentos_imobiliarios",
                len(registros),
            )

            db.insert_data(
                registros,
                table_name="financiamentos_imobiliarios",
                schema="bacen",
                conflict_fields=["data_referencia"],
                primary_key=["data_referencia"],
            )

            logger.info("[bacen_historico_dag] Carga histórica concluída com sucesso")
        except AirflowSkipException:
            raise
        except AirflowFailException:
            raise
        except Exception as e:
            logger.error("[bacen_historico_dag] Erro na carga histórica: %s", e)
            raise AirflowException(
                f"[bacen_historico_dag] Erro inesperado na carga histórica: {e}"
            ) from e

    fetch_and_store_historico()


dag_instance = bacen_financiamentos_imobiliarios_historico_dag()
