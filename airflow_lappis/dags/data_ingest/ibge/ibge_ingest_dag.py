import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_ibge import ClienteIBGE
from cliente_postgres import ClientPostgresDB
import psycopg2

CONFIGURACOES = Variable.get("IBGE_CONFIGURACOES", deserialize_json=True, default_var=[])


@dag(
    schedule_interval=get_dynamic_schedule("ibge_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Mateus",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ibge", "pib_construcao", "sinapi"],
)
def ibge_ingest_dag() -> None:
    """DAG para ingestão de dados do IBGE no PostgreSQL.

    Usa dynamic task mapping para criar uma task paralela
    para cada configuração de agregado definida em CONFIGURACOES.
    """

    @task
    def setup_schema() -> None:
        """
        Cria o schema do IBGE antes do processamento paralelo.
        Tratando o UniqueViolation em alta concorrência do Airflow.
        """
        postgres_conn_str = get_postgres_conn()
        schema = "ibge"
        try:
            with psycopg2.connect(postgres_conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                conn.commit()
            logging.info(f"Schema '{schema}' garantido com sucesso.")
        except psycopg2.errors.UniqueViolation:
            logging.warning(f"Schema '{schema}' já estava sendo criado (UniqueViolation mitigado).")

    @task
    def fetch_and_store_mapped(config: dict) -> None:
        logging.info(f"Iniciando ingestão: {config['tabela']}")
        
        agregado=config["agregado"]
        variaveis=config["variaveis"]
        tabela=config["tabela"]
        periodos=config.get("periodos", "-20")
        classificacao_id=config.get("classificacao_id")
        categoria=config.get("categoria")

        api = ClienteIBGE()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        dados_api = api.get_dados_agregados(
            agregado=agregado,
            variaveis=variaveis,
            periodos=periodos,
            classificacao_id=classificacao_id,
            categoria=categoria,
        )

        if not dados_api:
            logging.warning(f"Nenhum dado retornado da API IBGE para tabela {tabela}")
            return

        registros = ClienteIBGE.transformar_resposta(dados_api)

        if registros:
            logging.info(
                f"Inserindo {len(registros)} registros em ibge.{tabela}"
            )
            db.insert_data(
                registros,
                tabela,
                conflict_fields=["variavel_id", "localidade_id", "periodo", "classificacao_id", "categoria_id"],
                primary_key=["variavel_id", "localidade_id", "periodo", "classificacao_id", "categoria_id"],
                schema="ibge",
            )
            logging.info(f"Ingestão de {tabela} concluída")
        else:
            logging.warning(f"Nenhum registro extraído dos dados da API para {tabela}")

    setup = setup_schema()
    setup >> fetch_and_store_mapped.expand(config=CONFIGURACOES)


dag_instance = ibge_ingest_dag()
