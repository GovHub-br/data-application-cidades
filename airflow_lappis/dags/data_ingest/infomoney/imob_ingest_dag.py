import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable

from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_infomoney import ClienteInfomoney

# Configurações padrão
DEFAULT_ARGS = {
    "owner": "Milena Rocha",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="infomoney_imob",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["cidades","infomoney", "imob", "cotações","conjuntura"],
)
def infomoney_imob_dag() -> None:
    """
    DAG para extração de séries temporais do índice IMOB.SA
    via Alpha Vantage e carga no Postgres (schema infomoney).
    """

    @task
    def fetch_and_load_imob():
        logging.info("Iniciando extração Alpha Vantage (IMOB.SA)...")
        
  
        config = Variable.get("api_key_alphavantage", deserialize_json=True)
        API_KEY = config.get("api_key")
        SYMBOL = config.get("acao")
        
     
        api = ClienteInfomoney(api_key=API_KEY)
        db = ClientPostgresDB(get_postgres_conn())
        
      
        dados_imob = api.get_daily_series(SYMBOL)
        
        if not dados_imob:
            logging.warning(f"Nenhum dado retornado para o símbolo {SYMBOL}. Verifique a API Key ou o limite de requisições.")
            return

        dt_ingest = datetime.now().isoformat()
        for item in dados_imob:
            item["dt_ingest"] = dt_ingest

        logging.info(f"Processados {len(dados_imob)} registros. Iniciando carga no banco...")

        db.insert_data(
            dados_imob,
            table_name="acoes_imob",
            schema="infomoney",
            conflict_fields=["symbol", "data_pregao"],
            primary_key=["symbol", "data_pregao"]
        )
        
        logging.info("Carga finalizada com sucesso no schema infomoney.")

    
    fetch_and_load_imob()


infomoney_imob_dag()