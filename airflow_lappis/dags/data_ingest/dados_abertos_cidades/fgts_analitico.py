from helpers.utils import normalize_column_name, process_and_clean_data
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_snhis import ClienteSnhis
import logging
import tempfile
import pandas as pd

DEFAULT_ARGS = {
    "owner": "Milena Rocha",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="dados_abertos_fgts_analitico",
    schedule_interval="@monthly",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cidades","FGTS", "FAR", "dados_abertos"],
)
def dados_abertos_mcmv_fgts_analitico():

    @task
    def fetch_and_store_fgts_analitico(**kwargs):
        logging.info("[FGTS Analítico] Iniciando extração")
        
        api = ClienteSnhis()
        db = ClientPostgresDB(get_postgres_conn())
        
        # O uso do 'ts' do Airflow garante que sabemos quando a execução deveria ter ocorrido
        execution_date = kwargs.get('ts')

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = api.download_and_extract_fgts(tmpdir)
            
            # Processamento otimizado com Pandas dividindo o CSV gigante em blocos 
            # para evitar esgotamento de memória no Docker ("Zombie Job" / Código -9)
            if file_path.lower().endswith(".csv"):
                chunk_iterator = pd.read_csv(file_path, sep=";", encoding="utf-8", on_bad_lines='skip', chunksize=50000)
                
                inserted_count = 0
                for i, df_chunk in enumerate(chunk_iterator):
                    if df_chunk.empty:
                        continue
                    
                    df_chunk.columns = [normalize_column_name(col) for col in df_chunk.columns]
                    df_chunk["dt_ingest"] = execution_date
                    df_chunk["  arquivo_origem"] = api.get_latest_fgts_url().split("/")[-1]
                    
                    data = df_chunk.to_dict(orient="records")
                    logging.info(f"Otimização em lotes de {len(data)} registros. Inserindo lote {i+1} no Postgres...")
                    
                    db.insert_data(
                        data=data,
                        table_name="mcmv_fgts_analitico",
                        schema="dados_abertos_cidades"
                    )
                    inserted_count += len(data)
                
                logging.info(f"[FGTS Analítico] Processamento do CSV concluído: Total de {inserted_count} inseridos.")
                    
            else:
                df = pd.read_excel(file_path)

                if df.empty:
                    logging.warning("Nenhum dado encontrado")
                    return

                # Normalização de colunas via Pandas (mais rápido que loop em dict)
                df.columns = [normalize_column_name(col) for col in df.columns]
                df["dt_ingest"] = execution_date

                # Conversão para lista de dicionários apenas se o seu db.insert_data exigir
                data = df.to_dict(orient="records")

                logging.info(f"Inserindo {len(data)} registros (Excel)")
                db.insert_data(
                    data=data,
                    table_name="mcmv_fgts_analitico",
                    schema="dados_abertos_cidades"
                )

    # Invocação da task para registrar no grafo
    fetch_and_store_fgts_analitico()

# Instanciação da DAG
dados_abertos_mcmv_fgts_analitico()