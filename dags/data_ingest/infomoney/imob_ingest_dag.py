import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable

from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_infomoney import ClienteInfomoney
from cliente_minio import upload_raw_json
from ingestor_lake import registros_para_staging_parquet

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

        # Postgres: upsert por (symbol, data_pregao) -> preserva histórico.
        db.insert_data(
            dados_imob,
            table_name="acoes_imob",
            schema="infomoney",
            conflict_fields=["symbol", "data_pregao"],
            primary_key=["symbol", "data_pregao"]
        )

        # Lake (full-refresh): raw = payload cru da API (json) do dia; parquet
        # = histórico COMPLETO acumulado no Postgres (upsert), não só o lote
        # "compact" do dia (a API só devolve ~100 pregões por vez) -- senão o
        # parquet nunca cresce além disso e quebra qualquer gold que precise
        # de mais de ~5 meses de histórico (ex.: variação vs mesmo mês do ano
        # anterior). Parquet e bronze permanecem textuais; a silver normaliza
        # os formatos pt-BR e US antes de qualquer cálculo.
        upload_raw_json("infomoney", "acoes_imob", dados_imob_raw)

        colunas = ["symbol", "data_pregao", "open", "high", "low", "close", "volume", "dt_ingest"]
        linhas = db.execute_query(
            f"SELECT {', '.join(colunas)} FROM infomoney.acoes_imob ORDER BY data_pregao"
        )
        dados_imob_completo = []
        for linha in linhas:
            registro = dict(zip(colunas, linha))
            dados_imob_completo.append(registro)

        registros_para_staging_parquet("infomoney", "acoes_imob", dados_imob_completo)

        logging.info("Carga finalizada com sucesso no schema infomoney.")

    
    fetch_and_load_imob()


infomoney_imob_dag()
