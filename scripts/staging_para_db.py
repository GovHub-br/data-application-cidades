# scripts/staging_para_db.py

"""
Ingestão Staging (Parquet no MinIO) -> Postgres, via DuckDB.

Quarta e última etapa do pipeline do data lake. Lê cada parquet de staging/ direto do MinIO
(httpfs) e materializa uma tabela no Postgres usando a extensão `postgres` do DuckDB
(ATTACH + CREATE TABLE AS), sem passar os dados pelo Python.

Decisões do projeto:
  - Todas as colunas como TEXT — mantém a decisão do staging (tudo string); a tipagem
    (datas/números) fica para o dbt. Carga nunca falha por inferência de tipo errada.
  - Replace por arquivo: cada parquet vira/substitui uma tabela (DROP + CREATE TABLE AS
    dentro de uma transação, então leitores enxergam a tabela antiga até o commit).
  - Colunas de linhagem (`_source_file`, `_ingested_at`, `_source_hash`) do staging são
    preservadas na tabela final.

IMPORTANTE (ordem no pipeline): rode DEPOIS do `raw_para_staging.py --apply`, que é quem
popula staging/.

Idempotência: tabela de controle sftp._staging_to_dw_log com UNIQUE(staging_key,
source_hash); parquets já carregados (mesmo hash) são pulados. Use --force para recarregar.
"""

import argparse
import hashlib
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import pandas as pd
import psycopg2
import pyarrow.parquet as pq
from dotenv import load_dotenv

from lake_utils import format_size, norm_header

load_dotenv()

MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET     = os.environ["MINIO_BUCKET"]

PG_HOST     = os.environ["DB_DW_HOST_MCID"]
PG_PORT     = int(os.environ.get("DB_DW_PORT_MCID", 5432))
PG_USER     = os.environ["DB_DW_USER_MCID"]
PG_PASSWORD = os.environ["DB_DW_PASSWORD_MCID"]
PG_DBNAME   = os.environ["DB_DW_DBNAME_MCID"]

SCHEMA        = os.environ.get("SFTP_SCHEMA", "sftp")
CONTROL_TABLE = "_staging_to_dw_log"

STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "staging/")
AUDIT_PREFIX   = "audit/db/"

TMPDIR = os.environ.get("LAKE_TMPDIR") or os.environ.get("MASKING_TMPDIR") or None
if TMPDIR:
    os.makedirs(TMPDIR, exist_ok=True)

# Postgres trunca identificadores em 63 bytes; truncar aqui (com sufixo determinístico)
# evita colisão silenciosa entre nomes longos que compartilham o mesmo prefixo.
PG_MAX_IDENT = 63

_LOG_FILE = (
    Path(__file__).parent
    / f"staging_para_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logging.root.setLevel(logging.INFO)
for _h in (logging.FileHandler(_LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stderr)):
    _h.setFormatter(_formatter)
    logging.root.addHandler(_h)
log = logging.getLogger(__name__)


# Infra: conexões / controle
def _conn_str() -> str:
    return (
        f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )


def _criar_control_table(conn_str: str) -> None:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.{CONTROL_TABLE} (
                    id            SERIAL PRIMARY KEY,
                    execution_id  TEXT,
                    staging_key   TEXT NOT NULL,
                    target_table  TEXT,
                    source_file   TEXT,
                    source_format TEXT,
                    source_hash   TEXT,
                    n_linhas      BIGINT,
                    n_colunas     INT,
                    status        TEXT,
                    error_message TEXT,
                    created_at    TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (staging_key, source_hash)
                );
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_staging_to_dw_log_status
                ON {SCHEMA}.{CONTROL_TABLE} (status);
            """)
            conn.commit()
    log.info("Tabela de controle %s.%s garantida.", SCHEMA, CONTROL_TABLE)


def _carregar_carregados(conn_str: str) -> set:
    """(staging_key, source_hash) já materializados no Postgres (status 'loaded').

    Só o `--apply` (status 'loaded') conta para idempotência; dry-runs não bloqueiam.
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT staging_key, source_hash FROM {SCHEMA}.{CONTROL_TABLE}
                WHERE status = 'loaded' AND source_hash IS NOT NULL
            """)
            return {(r[0], r[1]) for r in cur.fetchall()}


def _registrar_control(conn_str: str, rec: dict) -> None:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SCHEMA}.{CONTROL_TABLE}
                    (execution_id, staging_key, target_table, source_file, source_format,
                     source_hash, n_linhas, n_colunas, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (staging_key, source_hash) DO UPDATE SET
                    execution_id  = EXCLUDED.execution_id,
                    target_table  = EXCLUDED.target_table,
                    source_file   = EXCLUDED.source_file,
                    source_format = EXCLUDED.source_format,
                    n_linhas      = EXCLUDED.n_linhas,
                    n_colunas     = EXCLUDED.n_colunas,
                    status        = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    created_at    = NOW()
                """,
                (
                    rec["execution_id"], rec["staging_key"], rec["target_table"],
                    rec["source_file"], rec["source_format"], rec["source_hash"],
                    rec["n_linhas"], rec["n_colunas"], rec["status"], rec["error_message"],
                ),
            )
            conn.commit()


# Identificadores Postgres
def _truncar_ident(nome: str) -> str:
    """Trunca em 63 bytes preservando unicidade via hash do nome completo."""
    if len(nome.encode("utf-8")) <= PG_MAX_IDENT:
        return nome
    h = hashlib.md5(nome.encode("utf-8")).hexdigest()[:6]
    return f"{nome[: PG_MAX_IDENT - 7]}_{h}"


def _nome_tabela(staging_key: str) -> str:
    """staging/<sub>/<nome>.parquet -> <sub>_<nome> (snake_case ASCII, <=63 bytes).

    O caminho inteiro entra no nome porque staging/ espelha o layout de raw/, e dois
    arquivos de subpastas diferentes podem ter o mesmo nome de arquivo.
    """
    rel = staging_key
    if rel.startswith(STAGING_PREFIX):
        rel = rel[len(STAGING_PREFIX):]
    if rel.lower().endswith(".parquet"):
        rel = rel.rsplit(".", 1)[0]
    partes = [norm_header(p) for p in rel.split("/") if norm_header(p)]
    nome = "_".join(partes) or "tabela_sem_nome"
    if re.match(r"^\d", nome):  # identificador não pode começar com dígito
        nome = f"t_{nome}"
    return _truncar_ident(nome)


def _colunas_postgres(nomes: List[str]) -> Tuple[List[str], bool]:
    """Trunca nomes de coluna p/ 63 bytes e deduplica. Retorna (finais, houve_mudanca)."""
    finais: List[str] = []
    usados: set = set()
    mudou = False
    for nome in nomes:
        base = _truncar_ident(nome)
        if base != nome:
            mudou = True
        final = base
        n = 2
        while final in usados:
            sufixo = f"_{n}"
            final = _truncar_ident(base[: PG_MAX_IDENT - len(sufixo)] + sufixo)
            n += 1
            mudou = True
        usados.add(final)
        finais.append(final)
    return finais, mudou


# Leitura do parquet (só o footer, via HTTP Range)
def _fs_minio():
    from pyarrow.fs import S3FileSystem

    endpoint = MINIO_ENDPOINT
    scheme = "http"
    if endpoint.startswith("http://"):
        endpoint = endpoint[len("http://"):]
    elif endpoint.startswith("https://"):
        endpoint, scheme = endpoint[len("https://"):], "https"
    return S3FileSystem(
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        endpoint_override=endpoint,
        scheme=scheme,
    )


def _ler_metadados(fs, staging_key: str) -> Tuple[int, List[str], Optional[str], Optional[str]]:
    """Retorna (n_linhas, colunas, source_hash, source_file) lendo só o footer do parquet."""
    pf = pq.ParquetFile(f"{MINIO_BUCKET}/{staging_key}", filesystem=fs)
    n_linhas = pf.metadata.num_rows
    colunas = list(pf.schema_arrow.names)
    meta = pf.schema_arrow.metadata or {}

    def _get(chave: bytes) -> Optional[str]:
        return meta[chave].decode("utf-8") if chave in meta else None

    return n_linhas, colunas, _get(b"source_hash"), _get(b"source_file")


# DuckDB
def _conectar_duckdb(memory_limit: str, threads: int):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"SET memory_limit='{memory_limit}';")
    con.execute(f"SET threads={threads};")
    if TMPDIR:
        con.execute(f"SET temp_directory='{TMPDIR}';")

    endpoint = MINIO_ENDPOINT
    use_ssl = endpoint.startswith("https://")
    for pref in ("http://", "https://"):
        if endpoint.startswith(pref):
            endpoint = endpoint[len(pref):]
    con.execute("SET s3_region='us-east-1';")
    con.execute("SET s3_url_style='path';")  # MinIO usa path-style, não virtual-host
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    return con


def _attach_postgres(con) -> None:
    dsn = (
        f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )
    con.execute(f"ATTACH '{dsn}' AS pg (TYPE POSTGRES);")


def _select_texto(colunas_parquet: List[str], colunas_pg: List[str]) -> str:
    """SELECT com todas as colunas explicitamente CAST para VARCHAR (-> TEXT no Postgres)."""
    partes = [
        f'CAST("{orig}" AS VARCHAR) AS "{final}"'
        for orig, final in zip(colunas_parquet, colunas_pg)
    ]
    return ", ".join(partes)


def _carregar_tabela(
    con, staging_key: str, tabela: str, colunas_parquet: List[str], colunas_pg: List[str],
) -> int:
    """DROP + CREATE TABLE AS na mesma transação; retorna nº de linhas na tabela final."""
    uri = f"s3://{MINIO_BUCKET}/{staging_key}"
    select = _select_texto(colunas_parquet, colunas_pg)
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute(f'DROP TABLE IF EXISTS pg.{SCHEMA}."{tabela}";')
        con.execute(
            f'CREATE TABLE pg.{SCHEMA}."{tabela}" AS '
            f"SELECT {select} FROM read_parquet('{uri}');"
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise
    return con.execute(f'SELECT count(*) FROM pg.{SCHEMA}."{tabela}";').fetchone()[0]


# Processamento de um objeto
def _novo_registro(execution_id: str, staging_key: str) -> dict:
    return {
        "execution_id": execution_id,
        "staging_key": staging_key,
        "target_table": None,
        "source_file": None,
        # de qual formato de origem (csv/txt/xlsx) este parquet saiu
        "source_format": None,
        "source_hash": None,
        "n_linhas": 0,
        "n_colunas": 0,
        "status": None,
        "error_message": None,
    }


def processar_objeto(
    con, fs, staging_key: str, execution_id: str, apply: bool, carregados: set,
) -> dict:
    t0 = time.time()
    rec = _novo_registro(execution_id, staging_key)
    try:
        n_linhas, colunas_parquet, source_hash, source_file = _ler_metadados(fs, staging_key)
        rec["source_hash"] = source_hash
        rec["source_file"] = source_file
        if source_file:
            rec["source_format"] = os.path.splitext(source_file)[1].lower().lstrip(".") or None
        rec["n_linhas"] = n_linhas
        rec["n_colunas"] = len(colunas_parquet)

        if not colunas_parquet:
            rec["status"] = "skipped_empty"
            return rec

        tabela = _nome_tabela(staging_key)
        rec["target_table"] = f"{SCHEMA}.{tabela}"

        if source_hash is not None and (staging_key, source_hash) in carregados:
            rec["status"] = "skipped_already"
            return rec

        colunas_pg, mudou = _colunas_postgres(colunas_parquet)
        if mudou:
            log.warning("%s — nomes de coluna truncados/dedup p/ limite do Postgres", staging_key)

        if not apply:
            rec["status"] = "dry_run"
            return rec

        n_pg = _carregar_tabela(con, staging_key, tabela, colunas_parquet, colunas_pg)
        if n_pg != n_linhas:
            raise ValueError(f"nº de linhas divergem: postgres={n_pg} != parquet={n_linhas}")
        rec["status"] = "loaded"
        return rec

    except Exception as e:  # noqa: BLE001
        rec["status"] = "error"
        rec["error_message"] = str(e)[:500]
        log.error("✗ %s: %s", staging_key, e)
        return rec
    finally:
        rec["_segundos"] = round(time.time() - t0, 2)


# S3 helpers (listagem via boto3, como nos demais scripts)
def _listar_objetos(s3, prefix: str):
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"]


# Auditoria (parquet)
def _gravar_auditoria(s3, execution_id: str, registros: List[dict]) -> None:
    import io as _io

    df = pd.DataFrame(registros)
    buf = _io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    key = f"{AUDIT_PREFIX}execution_id={execution_id}/part-0.parquet"
    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buf.getvalue())
    local = Path(__file__).parent / f"auditoria_db_{execution_id}.parquet"
    df.to_parquet(local, engine="pyarrow", index=False)
    log.info("Auditoria: s3://%s/%s (cópia local: %s)", MINIO_BUCKET, key, local)


# Main
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingestão Staging (Parquet/MinIO) -> Postgres via DuckDB."
    )
    parser.add_argument("--apply", action="store_true",
                        help="Cria/substitui as tabelas no Postgres. Sem esta flag roda em dry-run.")
    parser.add_argument("--force", action="store_true",
                        help="Recarrega parquets já materializados.")
    parser.add_argument("--limit", type=int, default=0, help="Processa no máximo N objetos.")
    parser.add_argument("--pattern", default="", help="Filtra por substring na key.")
    parser.add_argument("--prefix", default=STAGING_PREFIX,
                        help="Prefixo a varrer (default staging/).")
    parser.add_argument("--max-size-mb", type=int, default=0,
                        help="Pula objetos maiores que N MB (0 = sem limite).")
    parser.add_argument("--memory-limit", default=os.environ.get("DUCKDB_MEMORY_LIMIT", "4GB"),
                        help="Limite de memória do DuckDB (default 4GB).")
    parser.add_argument("--threads", type=int, default=int(os.environ.get("DUCKDB_THREADS", 4)),
                        help="Threads do DuckDB (default 4).")
    args = parser.parse_args()

    execution_id = uuid.uuid4().hex
    apply = args.apply

    log.info("=" * 70)
    log.info("Execução %s | modo=%s | prefixo=%s", execution_id,
             f"APPLY (Postgres {SCHEMA}.*)" if apply else "DRY-RUN (nada gravado)", args.prefix)
    log.info("=" * 70)

    conn_str = _conn_str()
    _criar_control_table(conn_str)
    carregados = set() if args.force else _carregar_carregados(conn_str)

    from lake_utils import criar_cliente_minio

    s3 = criar_cliente_minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    fs = _fs_minio()
    con = _conectar_duckdb(args.memory_limit, args.threads)
    if apply:
        _attach_postgres(con)

    registros: List[dict] = []
    contagem: Dict[str, int] = {}
    processados = 0

    try:
        for key, size in _listar_objetos(s3, args.prefix):
            if not key.lower().endswith(".parquet"):
                continue
            if args.pattern and args.pattern not in key:
                continue
            if args.max_size_mb and size > args.max_size_mb * 1024 * 1024:
                continue
            if args.limit and processados >= args.limit:
                break
            processados += 1

            rec = processar_objeto(con, fs, key, execution_id, apply, carregados)
            registros.append(rec)
            contagem[rec["status"]] = contagem.get(rec["status"], 0) + 1
            if rec["source_hash"] is not None and rec["status"] != "skipped_already":
                _registrar_control(conn_str, rec)

            icone = {"loaded": "✓", "dry_run": "◐", "error": "✗"}.get(rec["status"], "·")
            log.info(
                "%s [%d] %s (%s) — %s | tabela=%s | linhas=%d | cols=%d | %.1fs",
                icone, processados, key, format_size(size), rec["status"],
                rec["target_table"], rec["n_linhas"], rec["n_colunas"], rec["_segundos"],
            )
    finally:
        con.close()

    if registros:
        _gravar_auditoria(s3, execution_id, registros)

    log.info("=" * 70)
    log.info("Concluído. Objetos: %d", processados)
    for status, n in sorted(contagem.items()):
        log.info("  %-20s %d", status, n)
    if not apply:
        log.info("DRY-RUN — nenhuma tabela criada. Use --apply para gravar no Postgres.")
    log.info("Log: %s", _LOG_FILE)


if __name__ == "__main__":
    main()
