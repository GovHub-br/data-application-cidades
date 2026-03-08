import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_bacen import ClienteBacen
from cliente_postgres import ClientPostgresDB

# Lê o JSON {tipo: codigo_sgs} do Airflow Variable e monta a lista de configs.
# O prefixo AIRFLOW_VAR_ no .env registra a variável automaticamente no Airflow.
# Exemplo: {"pf_concessoes_rs_mi": 20704, "pf_taxa_juros_aa": 20774, ...}
BACEN_SERIES = Variable.get("BACEN_SERIES", deserialize_json=True, default_var={})
CONFIGURACOES = [{"tipo": k, "codigo": v} for k, v in BACEN_SERIES.items()]


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

        for config in CONFIGURACOES:
            tipo = config["tipo"]
            codigo = config["codigo"]

            logging.info(f"Iniciando ingestão: tipo={tipo}, codigo={codigo}")

            dados = api.get_serie(codigo=codigo, ultimos=13)

            if not dados:
                logging.warning(f"Nenhum dado retornado da API BACEN para tipo={tipo}")
                continue

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

            db.insert_data(
                registros,
                "financiamentos_imobiliarios",
                conflict_fields=["tipo", "data"],
                primary_key=["tipo", "data"],
                schema="bacen",
            )

            logging.info(f"Ingestão de tipo={tipo} concluída com sucesso.")

    fetch_and_store_all_series()


dag_instance = bacen_sgs_ingest_dag()
