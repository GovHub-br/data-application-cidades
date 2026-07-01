"""
DAG de ingestão histórica dos dados do SFTP.

Percorre os diretórios do SFTP (/home/fabrica, /home/caixa), identifica
arquivos suportados (CSV, TXT, XLSX, ZIP), e insere os dados no PostgreSQL
no schema 'sftp'. Cada arquivo gera uma tabela separada.

Controle de reprocessamento via tabela sftp._ingest_log.

Feature #90 — Estruturar ingestão e monitoramento dos dados do SFTP.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.models import Variable

import logging
import psycopg2

from cliente_sftp import ClienteSFTP, DIRS_RAIZ
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

SFTP_CONN_ID = "mcid_sftp"
SCHEMA = Variable.get("SFTP_SCHEMA", default_var="sftp_v2") # Colocar sftp depois dos testes
LOG_TABLE = "_ingest_log"

default_args = {
    "owner": "dados_historicos",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Funções auxiliares para a tabela de log
# ---------------------------------------------------------------------------

def _criar_schema_e_log(conn_str: str) -> None:
    """Cria o schema sftp e a tabela _ingest_log se não existirem.

    Se a tabela existir mas com colunas antigas, recria-a.
    Compatível com o DDL de scripts/init_sftp_schema.sql.
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")

            # Verifica se a tabela existe e tem a coluna correta
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (SCHEMA, LOG_TABLE))
            colunas_existentes = {row[0] for row in cur.fetchall()}

            if colunas_existentes and "sftp_path" not in colunas_existentes:
                # Tabela existe mas com schema antigo — recria
                logging.warning(
                    "Tabela %s.%s existe com colunas antigas. Recriando...",
                    SCHEMA, LOG_TABLE,
                )
                cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{LOG_TABLE};")

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.{LOG_TABLE} (
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
                    started_at      TIMESTAMPTZ,
                    finished_at     TIMESTAMPTZ,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (sftp_path, file_hash)
                );
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_ingest_log_sftp_path
                ON {SCHEMA}.{LOG_TABLE} (sftp_path);
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_ingest_log_status
                ON {SCHEMA}.{LOG_TABLE} (status);
            """)
            conn.commit()
    logging.info("Schema '%s' e tabela '%s' garantidos.", SCHEMA, LOG_TABLE)


def _obter_arquivos_ja_processados(conn_str: str) -> set:
    """Retorna conjunto de caminhos SFTP já processados com sucesso."""
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT sftp_path FROM {SCHEMA}.{LOG_TABLE}
                WHERE status = 'success'
            """)
            processados = set()
            for row in cur.fetchall():
                path = row[0]
                if "@" in path:
                    processados.add(path.split("@")[0])
                else:
                    processados.add(path)
            return processados


def _registrar_ingest(
    conn_str: str,
    sftp_path: str,
    file_name: str,
    target_table: str,
    file_size: int,
    rows_inserted: int,
    status: str,
    error_message: str = None,
    file_mtime: int = None,
    file_hash: str = None,
) -> None:
    """Registra uma entrada no _ingest_log."""
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SCHEMA}.{LOG_TABLE}
                    (sftp_path, file_name, file_size, target_table,
                     status, rows_inserted, error_message,
                     started_at, finished_at, file_mtime, file_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), to_timestamp(%s), %s)
                ON CONFLICT (sftp_path, file_hash) 
                DO UPDATE SET
                    status = EXCLUDED.status,
                    target_table = EXCLUDED.target_table,
                    file_size = EXCLUDED.file_size,
                    rows_inserted = EXCLUDED.rows_inserted,
                    error_message = EXCLUDED.error_message,
                    finished_at = NOW()
                """,
                (
                    sftp_path,
                    file_name,
                    file_size,
                    target_table,
                    status,
                    rows_inserted,
                    error_message,
                    file_mtime,
                    file_hash,
                ),
            )
            conn.commit()


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

def listar_arquivos(**context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Task 1: Lista arquivos novos no SFTP (não presentes no _ingest_log).

    Retorna via XCom uma lista de dicts:
        [{"caminho": "/home/.../file.csv", "tamanho": 12345}, ...]
    """
    conn_str = get_postgres_conn()

    # Garante que schema e tabela de log existem
    _criar_schema_e_log(conn_str)

    # Obtém arquivos já processados
    ja_processados = _obter_arquivos_ja_processados(conn_str)
    logging.info(
        "Arquivos já processados no _ingest_log: %d", len(ja_processados)
    )

    # Conecta ao SFTP e lista todos os arquivos suportados
    hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
    todos_arquivos = []

    with hook.get_conn() as sftp:
        cliente = ClienteSFTP(sftp)

        for dir_raiz in DIRS_RAIZ:
            logging.info("Varrendo diretório: %s", dir_raiz)
            try:
                arquivos = cliente.listar_arquivos_recursivo(dir_raiz)
                logging.info(
                    "  → %d arquivos suportados em %s", len(arquivos), dir_raiz
                )
                todos_arquivos.extend(arquivos)
            except Exception as e:
                logging.error("Erro ao varrer %s: %s", dir_raiz, e)

    logging.info("Total de arquivos suportados no SFTP: %d", len(todos_arquivos))

    # Filtra apenas os novos (suportando workers que ainda tenham o plugin antigo em memória)
    arquivos_novos = []
    for item in todos_arquivos:
        if len(item) == 3:
            caminho, tamanho, mtime = item
        else:
            caminho, tamanho = item
            mtime = None
            
        if caminho not in ja_processados:
            arquivos_novos.append({"caminho": caminho, "tamanho": tamanho, "mtime": mtime})

    logging.info(
        "Arquivos NOVOS para processar: %d (de %d total)",
        len(arquivos_novos),
        len(todos_arquivos),
    )

    # Log dos primeiros 20 para visibilidade
    for arq in arquivos_novos[:20]:
        logging.info("  → %s (%d bytes)", arq["caminho"], arq["tamanho"])
    if len(arquivos_novos) > 20:
        logging.info("  ... e mais %d arquivos", len(arquivos_novos) - 20)

    return arquivos_novos


def processar_e_inserir(**context: Dict[str, Any]) -> Dict[str, int]:
    """Task 2: Baixa, processa e insere cada arquivo novo no Postgres.

    Para cada arquivo:
        1. Download do SFTP
        2. Parse com pandas (auto-detecta formato)
        3. Adiciona colunas de metadados
        4. Cria tabela no schema sftp (drop + recreate)
        5. Insere os dados
        6. Registra no _ingest_log

    Returns:
        Dict com contadores: {"sucesso": N, "erro": N, "ignorado": N}
    """
    ti = context["ti"]
    arquivos_novos = ti.xcom_pull(task_ids="listar_arquivos")

    if not arquivos_novos:
        logging.info("Nenhum arquivo novo para processar.")
        return {"sucesso": 0, "erro": 0, "ignorado": 0}

    conn_str = get_postgres_conn()
    db = ClientPostgresDB(conn_str)
    contadores = {"sucesso": 0, "erro": 0, "ignorado": 0}
    total = len(arquivos_novos)

    hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)

    with hook.get_conn() as sftp:
        cliente = ClienteSFTP(sftp)

        for idx, arq_info in enumerate(arquivos_novos, 1):
            caminho = arq_info["caminho"]
            tamanho = arq_info["tamanho"]
            mtime = arq_info.get("mtime")
            nome_arquivo = caminho.rsplit("/", 1)[-1]

            logging.info(
                "=" * 60 + "\n[%d/%d] Processando: %s (%d bytes)",
                idx, total, caminho, tamanho,
            )

            # Evita OOM (Out of Memory) pulando arquivos > 100MB
            # ZIPs muito grandes expandem para GBs na memória (Pandas)
            if tamanho > 100 * 1024 * 1024:
                msg = f"Ignorado: arquivo excede o limite de 100MB para processamento em memória ({tamanho / 1024 / 1024:.1f} MB)"
                logging.warning(msg)
                _registrar_ingest(
                    conn_str, caminho, nome_arquivo, None,
                    tamanho, 0, "skipped", msg,
                    file_mtime=mtime,
                )
                contadores["ignorado"] += 1
                continue

            try:
                # Download + parse
                retorno = cliente.baixar_e_ler(caminho)
                
                # Suporte para workers com a versão antiga do plugin em memória
                if isinstance(retorno, tuple) and len(retorno) == 2 and isinstance(retorno[0], str):
                    file_hash, resultados_parse = retorno
                else:
                    file_hash = None
                    resultados_parse = retorno

                if not resultados_parse:
                    logging.warning("Nenhum dado extraído de %s", caminho)
                    _registrar_ingest(
                        conn_str, caminho, nome_arquivo, None,
                        tamanho, 0, "skipped",
                        "Nenhum dado extraído",
                        file_mtime=mtime, file_hash=file_hash,
                    )
                    contadores["ignorado"] += 1
                    continue

                # Cada resultado (pode ser múltiplo para ZIPs)
                for nome_base, df in resultados_parse:
                    # Para ZIPs, o path no log fica arquivo.zip@arquivo_interno
                    path_log = f"{caminho}@{nome_base}" if caminho.lower().endswith('.zip') else caminho

                    if df.empty:
                        logging.warning(
                            "DataFrame vazio para %s (de %s)", nome_base, caminho
                        )
                        _registrar_ingest(
                            conn_str, path_log, nome_arquivo, None,
                            tamanho, 0, "skipped",
                            f"DataFrame vazio: {nome_base}",
                            file_mtime=mtime, file_hash=file_hash,
                        )
                        contadores["ignorado"] += 1
                        continue

                    # Gera nome da tabela
                    nome_tabela = ClienteSFTP.gerar_nome_tabela(nome_base)

                    # Sanitiza nomes das colunas
                    colunas_sanitizadas = []
                    vistos = set()
                    for col in df.columns:
                        nome_limpo = ClienteSFTP.gerar_nome_tabela(str(col))
                        if not nome_limpo:
                            nome_limpo = "coluna_vazia"
                        
                        # Resolve duplicatas (ex: duas colunas viram "id")
                        if nome_limpo in vistos:
                            i = 1
                            while f"{nome_limpo}_{i}" in vistos:
                                i += 1
                            nome_limpo = f"{nome_limpo}_{i}"
                        
                        vistos.add(nome_limpo)
                        colunas_sanitizadas.append(nome_limpo)
                    
                    df.columns = colunas_sanitizadas

                    # Adiciona metadados
                    df["dt_ingest"] = datetime.now().isoformat()
                    df["arquivo_origem"] = caminho

                    qtd_registros = len(df)
                    logging.info(
                        "  Tabela: %s.%s | Colunas: %d | Linhas: %d",
                        SCHEMA, nome_tabela, len(df.columns), qtd_registros,
                    )

                    # Converte para lista de dicts e insere
                    data = df.to_dict(orient="records")

                    # Drop + recreate (como insert_csv_data faz)
                    db.drop_table_if_exists(nome_tabela, schema=SCHEMA)
                    db.insert_data(
                        data,
                        nome_tabela,
                        primary_key=None,
                        schema=SCHEMA,
                    )

                    # Registra sucesso no log
                    _registrar_ingest(
                        conn_str, path_log, nome_arquivo,
                        f"{SCHEMA}.{nome_tabela}",
                        tamanho, qtd_registros, "success",
                        file_mtime=mtime, file_hash=file_hash,
                    )
                    contadores["sucesso"] += 1
                    logging.info(
                        "  ✔ Inserido %s.%s (%d registros)", SCHEMA, nome_tabela, qtd_registros
                    )

            except Exception as e:
                logging.error(
                    "  ✘ ERRO ao processar %s: %s", caminho, str(e)
                )
                _registrar_ingest(
                    conn_str, caminho, nome_arquivo, None,
                    tamanho, 0, "error", str(e)[:500],
                    file_mtime=mtime,
                )
                contadores["erro"] += 1

    # Resumo final
    logging.info("=" * 60)
    logging.info("RESUMO DA INGESTÃO")
    logging.info("=" * 60)
    logging.info("  Sucesso:  %d", contadores["sucesso"])
    logging.info("  Erro:     %d", contadores["erro"])
    logging.info("  Ignorado: %d", contadores["ignorado"])
    logging.info("  Total:    %d", total)

    return contadores


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="sftp_ingest_historico",
    default_args=default_args,
    description="Ingestão histórica de arquivos do SFTP para o schema sftp no Postgres",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sftp", "dados_historicos", "ingest"],
) as dag:

    listar_task = PythonOperator(
        task_id="listar_arquivos",
        python_callable=listar_arquivos,
    )

    processar_task = PythonOperator(
        task_id="processar_e_inserir",
        python_callable=processar_e_inserir,
    )

    listar_task >> processar_task
