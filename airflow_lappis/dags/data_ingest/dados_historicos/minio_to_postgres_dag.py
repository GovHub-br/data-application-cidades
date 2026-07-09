import os
import logging
import zipfile
import shutil
from datetime import datetime
from contextlib import closing
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import psycopg2
import boto3
import duckdb
from helpers.postgres_helpers import get_postgres_conn
import sys
sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "plugins"))
from cliente_sftp import ClienteSFTP

# ====================== CONFIGURAÇÕES ======================
SCHEMA = Variable.get("sftp_schema", default_var="sftp_v2")
MINIO_LOG_TABLE = "_ingest_minio_log"
DUCKDB_LOG_TABLE = "_ingest_log"
TEMP_DIR = "/tmp/airflow_duckdb_ingest"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

default_args = {
    "owner": "airflow",
    "retries": 1,
}

# ====================== FUNÇÕES AUXILIARES ======================
def _garantir_tabela_log(conn_str: str):
    """Cria a tabela de log original no Postgres se ela não existir."""
    with closing(psycopg2.connect(conn_str)) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.{DUCKDB_LOG_TABLE} (
                    id              SERIAL PRIMARY KEY,
                    sftp_path       TEXT NOT NULL,
                    file_name       TEXT NOT NULL,
                    file_size       BIGINT,
                    file_mtime      TIMESTAMPTZ,
                    file_hash       TEXT,
                    target_table    TEXT,
                    status          TEXT DEFAULT 'pending',
                    rows_inserted   INTEGER,
                    error_message   TEXT,
                    started_at      TIMESTAMPTZ DEFAULT NOW(),
                    finished_at     TIMESTAMPTZ,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (sftp_path, file_hash)
                );
            """)
            conn.commit()
    logging.info(f"Tabela de log {SCHEMA}.{DUCKDB_LOG_TABLE} verificada/criada.")

def _obter_arquivos_pendentes(conn_str: str) -> list:
    """Busca arquivos que estão no MinIO, mas não foram carregados no Postgres."""
    with closing(psycopg2.connect(conn_str)) as conn:
        with conn.cursor() as cur:
            # Pega com sucesso no MinIO que não estão com sucesso no DuckDB (cruzando por sftp_path)
            query = f"""
                SELECT m.minio_key, m.file_name, m.sftp_path, m.file_hash, m.file_size, m.file_mtime
                FROM {SCHEMA}.{MINIO_LOG_TABLE} m
                LEFT JOIN {SCHEMA}.{DUCKDB_LOG_TABLE} d
                  ON m.sftp_path = d.sftp_path AND d.status = 'success'
                WHERE m.status = 'success'
                  AND d.id IS NULL
            """
            cur.execute(query)
            pendentes = [{"minio_key": row[0], "file_name": row[1], "sftp_path": row[2], "file_hash": row[3], "file_size": row[4], "file_mtime": row[5]} for row in cur.fetchall()]
    return pendentes

def _registrar_duckdb_log(conn_str, sftp_path, file_name, file_hash, file_size, file_mtime, target_table, status, rows_inserted=0, error_message=None):
    with closing(psycopg2.connect(conn_str)) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {SCHEMA}.{DUCKDB_LOG_TABLE}
                    (sftp_path, file_name, file_hash, file_size, file_mtime, target_table, status, rows_inserted, error_message, finished_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (sftp_path, file_hash) 
                DO UPDATE SET
                    status = EXCLUDED.status,
                    target_table = EXCLUDED.target_table,
                    file_size = EXCLUDED.file_size,
                    file_mtime = EXCLUDED.file_mtime,
                    rows_inserted = EXCLUDED.rows_inserted,
                    error_message = EXCLUDED.error_message,
                    finished_at = NOW()
            """, (sftp_path, file_name, file_hash, file_size, file_mtime, target_table, status, rows_inserted, error_message))
            conn.commit()

# ====================== TASKS ======================
def descobrir_pendentes(**context):
    """Encontra arquivos para processar."""
    conn_str = get_postgres_conn()
    _garantir_tabela_log(conn_str)
    
    pendentes = _obter_arquivos_pendentes(conn_str)
    logging.info(f"Encontrados {len(pendentes)} arquivos pendentes para o DuckDB.")
    

    context['ti'].xcom_push(key='pendentes', value=pendentes)

def processar_arquivos(**context):
    """Baixa do MinIO, processa no DuckDB e apaga."""
    pendentes = context['ti'].xcom_pull(key='pendentes', task_ids='descobrir_pendentes')
    if not pendentes:
        logging.info("Nada a processar.")
        return

    conn_str = get_postgres_conn()
    
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    s3 = boto3.client(
        's3',
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    total = len(pendentes)
    for idx, arquivo in enumerate(pendentes, 1):
        minio_key = arquivo['minio_key']
        file_name = arquivo['file_name']
        sftp_path = arquivo['sftp_path']
        file_hash = arquivo['file_hash']
        file_size = arquivo['file_size']
        file_mtime = arquivo['file_mtime']
        local_zip_path = os.path.join(TEMP_DIR, file_name)
        
        logging.info("=" * 60)
        logging.info(f"[{idx}/{total}] Processando {minio_key} ({file_size} bytes)")
        
        # Verifica se o arquivo já foi processado (útil caso a task do Airflow sofra retry após queda de energia)
        ja_processado = False
        with closing(psycopg2.connect(conn_str)) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT status FROM {SCHEMA}.{DUCKDB_LOG_TABLE} WHERE sftp_path = %s AND file_hash = %s", (sftp_path, file_hash))
                row = cur.fetchone()
                if row and row[0] == 'success':
                    ja_processado = True
        
        if ja_processado:
            logging.info("  -> Arquivo já processado com sucesso em uma tentativa anterior. Pulando...")
            continue
        
        _registrar_duckdb_log(conn_str, sftp_path, file_name, file_hash, file_size, file_mtime, None, "processing")
        
        try:
            #Download do MinIO para /tmp
            logging.info(f"  -> Baixando {minio_key} para {local_zip_path}")
            s3.download_file(MINIO_BUCKET, minio_key, local_zip_path)
            

            ext_dir = os.path.join(TEMP_DIR, f"extracted_{file_name}")
            os.makedirs(ext_dir, exist_ok=True)
            
            arquivos_extraidos = []
            if local_zip_path.lower().endswith('.zip'):
                logging.info(f"  -> Extraindo ZIP...")
                with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(ext_dir)
                arquivos_extraidos = [os.path.join(ext_dir, f) for f in os.listdir(ext_dir)]
            else:
                # Se não for zip, só move pra pasta extraida
                shutil.move(local_zip_path, os.path.join(ext_dir, file_name))
                arquivos_extraidos = [os.path.join(ext_dir, file_name)]

            logging.info("  -> Iniciando DuckDB e conectando no PostgreSQL...")
            
            pg_host = os.getenv("DB_DW_HOST_MCID")
            pg_db = os.getenv("DB_DW_DBNAME_MCID")
            pg_user = os.getenv("DB_DW_USER_MCID")
            pg_pass = os.getenv("DB_DW_PASSWORD_MCID")
            pg_port = os.getenv("DB_DW_PORT_MCID")
            
            duck_conn = duckdb.connect(':memory:')
            duck_conn.execute("INSTALL postgres;")
            duck_conn.execute("LOAD postgres;")
            
    
            duck_conn.execute(f"ATTACH 'dbname={pg_db} user={pg_user} host={pg_host} password={pg_pass} port={pg_port}' AS pg (TYPE POSTGRES);")
            
            linhas_totais = 0
            for ext_file in arquivos_extraidos:
                is_excel = ext_file.lower().endswith('.xlsx') or ext_file.lower().endswith('.xls')
                is_csv = ext_file.lower().endswith('.csv') or ext_file.lower().endswith('.txt')
                
                if is_excel or is_csv:
                    nome_base = os.path.basename(ext_file)
                    tabela_alvo = ClienteSFTP.gerar_nome_tabela(nome_base)
                    
                    if is_excel:
                        import pandas as pd
                        logging.info(f"  -> Convertendo Excel para CSV temporário: {nome_base}")
                        # Converte a planilha lendo os dados brutos sem assumir cabeçalho
                        df = pd.read_excel(ext_file, dtype=str, header=None)
                        
                        # Limpa colunas e linhas 100% vazias
                        df = df.dropna(axis=1, how='all')
                        df = df.dropna(axis=0, how='all')
                        df = df.reset_index(drop=True)
                        
                        if not df.empty:
                            # Heurística: encontra o cabeçalho real pulando títulos mesclados no topo
                            head_search = df.head(30)
                            filled_counts = head_search.notna().sum(axis=1)
                            max_filled = filled_counts.max()
                            
                            # O cabeçalho real costuma ser a primeira linha com pelo menos 80% do preenchimento máximo
                            best_header_idx = filled_counts[filled_counts >= (max_filled * 0.8)].index[0]
                            
                            raw_headers = df.iloc[best_header_idx].tolist()
                            clean_headers = []
                            for i, val in enumerate(raw_headers):
                                val_str = str(val).strip() if pd.notna(val) else ""
                                clean_headers.append(val_str if val_str else f"unnamed_{i}")
                            
                            df.columns = clean_headers
                            df = df.iloc[best_header_idx + 1:]
                            
                            # Desduplica nomes de colunas (Postgres não suporta colunas com mesmo nome)
                            cols = pd.Series(df.columns)
                            for dup in cols[cols.duplicated()].unique():
                                dup_indices = cols[cols == dup].index.tolist()
                                for i, idx in enumerate(dup_indices):
                                    if i != 0:
                                        cols.iat[idx] = f"{dup}_{i}"
                            df.columns = cols

                        csv_path = ext_file + ".csv"
                        df.to_csv(csv_path, index=False, sep=",")
                        ext_file = csv_path # Aponta o DuckDB para o CSV recém criado
                    
                    logging.info(f"  -> Inserindo {ext_file} na tabela pg.{SCHEMA}.{tabela_alvo}")
                    
                    # Cria schema caso não exista
                    duck_conn.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{SCHEMA};")
                    # Evita silenciosamente pular a inserção se tiver nomes duplicados na mesma rodada
                    duck_conn.execute(f"DROP TABLE IF EXISTS pg.{SCHEMA}.{tabela_alvo};")
            
                    query = f"""
                        CREATE TABLE pg.{SCHEMA}.{tabela_alvo} AS 
                        SELECT 
                            *, 
                            '{sftp_path}' AS arquivo_origem,
                            current_timestamp AS dt_ingest
                        FROM read_csv_auto('{ext_file}', ignore_errors=true, normalize_names=true);
                    """
                    duck_conn.execute(query)

                    res_linhas = duck_conn.execute(f"SELECT COUNT(*) FROM pg.{SCHEMA}.{tabela_alvo}").fetchone()
                    linhas_totais += res_linhas[0]
                    
                    res_cols = duck_conn.execute(f"DESCRIBE pg.{SCHEMA}.{tabela_alvo}").fetchall()
                    num_colunas = len(res_cols)
                    
                    logging.info(f"  -> Tabela: {SCHEMA}.{tabela_alvo} | Colunas: {num_colunas} | Linhas inseridas: {res_linhas[0]}")
                    
            # Registra sucesso no final
            _registrar_duckdb_log(conn_str, sftp_path, file_name, file_hash, file_size, file_mtime, f"{SCHEMA}.{tabela_alvo}", "success", rows_inserted=linhas_totais)
            logging.info(f"  ✔ Sucesso! {linhas_totais} linhas processadas no total.")
            
        except Exception as e:
            logging.error(f"  ✘ ERRO: {str(e)}")
            _registrar_duckdb_log(conn_str, sftp_path, file_name, file_hash, file_size, file_mtime, None, "error", error_message=str(e))
            
        finally:
            logging.info("  -> Limpando arquivos temporários...")
            if os.path.exists(local_zip_path):
                try: os.remove(local_zip_path)
                except: pass
            if 'ext_dir' in locals() and os.path.exists(ext_dir):
                shutil.rmtree(ext_dir, ignore_errors=True)

# ========================== DAGS ========================================
with DAG(
    dag_id="minio_to_postgres_duckdb",
    default_args=default_args,
    description="Lê logs do MinIO, baixa arquivos pesados p/ /tmp, e usa DuckDB para carga no Postgres",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["minio", "duckdb", "postgres"],
) as dag:

    task_descobrir = PythonOperator(
        task_id="descobrir_pendentes",
        python_callable=descobrir_pendentes,
    )

    task_processar = PythonOperator(
        task_id="processar_arquivos",
        python_callable=processar_arquivos,
    )

    task_descobrir >> task_processar
