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
        logging.info("Iniciando extração Infomoney (IMOB.SA)...")
        
  
        config = Variable.get("api_key_alphavantage", deserialize_json=True)
        API_KEY = config.get("api_key")
        SYMBOL = config.get("acao")
        
     
        api = ClienteInfomoney(api_key=API_KEY)
        db = ClientPostgresDB(get_postgres_conn())
        
      
        dados_imob_raw = api.get_daily_series(SYMBOL)

        if not dados_imob_raw:
            logging.warning(f"Nenhum dado retornado para o símbolo {SYMBOL}.")
            return

        dt_ingest = datetime.now().isoformat()

        dados_imob = []

        for data_pregao, valores in dados_imob_raw.items():
            if data_pregao >= "2024-01-01":
                registro = {
                    "symbol": SYMBOL,
                    "data_pregao": data_pregao,
                    "open": float(valores["1. open"]),
                    "high": float(valores["2. high"]),
                    "low": float(valores["3. low"]),
                    "close": float(valores["4. close"]),
                    "volume": int(valores["5. volume"]),
                    "dt_ingest": dt_ingest
                }
    
                dados_imob.append(registro)

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