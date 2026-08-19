import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_ibge import ClienteIBGE
from cliente_postgres import ClientPostgresDB
from cliente_minio import upload_raw_json, download_raw_json
from base_file_parser import registros_para_staging_parquet
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
            logging.warning(
                f"Schema '{schema}' já estava sendo criado (UniqueViolation mitigado)."
            )

    @task
    def fetch_and_store_mapped(config: dict) -> None:
        logging.info(f"Iniciando ingestão: {config['tabela']}")

        agregado = config["agregado"]
        variaveis = config["variaveis"]
        tabela = config["tabela"]
        periodos = config.get("periodos", "-20")
        classificacao_id = config.get("classificacao_id")
        categoria = config.get("categoria")

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

        # 1.1 Extração p/ MinIO (raw, full-refresh): payload cru da API.
        upload_raw_json("ibge", tabela, dados_api)

        registros = ClienteIBGE.transformar_resposta(dados_api)

        if registros:
            logging.info(f"Inserindo {len(registros)} registros em ibge.{tabela}")
            db.insert_data(
                registros,
                tabela,
                conflict_fields=[
                    "variavel_id",
                    "localidade_id",
                    "periodo",
                    "classificacao_id",
                    "categoria_id",
                ],
                primary_key=[
                    "variavel_id",
                    "localidade_id",
                    "periodo",
                    "classificacao_id",
                    "categoria_id",
                ],
                schema="ibge",
            )
            logging.info(f"Ingestão de {tabela} concluída")
        else:
            logging.warning(f"Nenhum registro extraído dos dados da API para {tabela}")

    @task(trigger_rule="all_done")
    def gera_parquet_staging(config: dict) -> None:
        """1.2 Transformação → parquet (texto) na staging do MinIO.

        Lê o raw json do MinIO e sobe o parquet como texto — sem tipagem
        Python pelo meio. A tipagem final (numeric/date) fica por conta do dbt
        (camada silver), que lê a staging via `read_parquet('s3://...')`.

        trigger_rule=all_done + o guard abaixo garantem que uma tabela cujo
        fetch falhou (ex.: HTTP 500 do IBGE) NÃO bloqueie o parquet das demais:
        cada config é independente — a que não tem raw é apenas pulada.
        """
        tabela = config["tabela"]

        try:
            dados_api = download_raw_json("ibge", tabela)
        except Exception as exc:  # noqa: BLE001 - raw ausente = fetch falhou
            logging.warning(
                f"Sem raw para ibge.{tabela} (fetch pode ter falhado): {exc}. "
                f"Pulando geração de parquet."
            )
            return

        registros = ClienteIBGE.transformar_resposta(dados_api)
        if not registros:
            logging.warning(f"Sem registros para gerar parquet: ibge.{tabela}")
            return

        registros_para_staging_parquet("ibge", tabela, registros)

    setup = setup_schema()
    fetch = fetch_and_store_mapped.expand(config=CONFIGURACOES)
    parquet = gera_parquet_staging.expand(config=CONFIGURACOES)
    setup >> fetch >> parquet


dag_instance = ibge_ingest_dag()
