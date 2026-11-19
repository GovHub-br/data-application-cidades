"""Tabela de controle lake._staging_log e parquet de auditoria de cada execução."""

import json
import logging
from typing import List

import pandas as pd
import psycopg2
from psycopg2.extras import Json

from cliente_minio import ClienteMinio

from .config import (
    AUDIT_PREFIX,
    CONTROL_TABLE,
    MINIO_BUCKET,
    PG_DBNAME,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    SCHEMA,
    SCRIPTS_DIR,
    _LOCAL_ARTIFACTS,
)

log = logging.getLogger(__name__)


def _conn_str() -> str:
    return f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} user={PG_USER} password={PG_PASSWORD}"


def _criar_control_table(conn_str: str) -> None:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.{CONTROL_TABLE} (
                    id            SERIAL PRIMARY KEY,
                    execution_id  TEXT,
                    raw_key       TEXT NOT NULL,
                    staging_key   TEXT,
                    source_hash   TEXT,
                    n_linhas      BIGINT,
                    n_colunas     INT,
                    n_bad_lines   BIGINT,
                    encoding      TEXT,
                    delimiter     TEXT,
                    column_map    JSONB,
                    status        TEXT,
                    error_message TEXT,
                    created_at    TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (raw_key, staging_key, source_hash)
                );
            """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_staging_log_status
                ON {SCHEMA}.{CONTROL_TABLE} (status);
            """
            )
            conn.commit()
    log.info("Tabela de controle %s.%s garantida.", SCHEMA, CONTROL_TABLE)


def _carregar_convertidos(conn_str: str) -> set:
    """(raw_key, source_hash) já convertidos e gravados em staging/ (status 'converted').

    Só o `--apply` (status 'converted') conta para idempotência; execuções dry-run
    (status 'dry_run') não bloqueiam reprocessamento.
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT raw_key, source_hash FROM {SCHEMA}.{CONTROL_TABLE}
                WHERE status = 'converted' AND source_hash IS NOT NULL
            """
            )
            return {(r[0], r[1]) for r in cur.fetchall()}


def _registrar_control(conn_str: str, rec: dict) -> None:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SCHEMA}.{CONTROL_TABLE}
                    (execution_id, raw_key, staging_key, source_hash, n_linhas, n_colunas,
                     n_bad_lines, encoding, delimiter, column_map, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (raw_key, staging_key, source_hash) DO UPDATE SET
                    execution_id  = EXCLUDED.execution_id,
                    staging_key   = EXCLUDED.staging_key,
                    n_linhas      = EXCLUDED.n_linhas,
                    n_colunas     = EXCLUDED.n_colunas,
                    n_bad_lines   = EXCLUDED.n_bad_lines,
                    encoding      = EXCLUDED.encoding,
                    delimiter     = EXCLUDED.delimiter,
                    column_map    = EXCLUDED.column_map,
                    status        = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    created_at    = NOW()
                """,
                (
                    rec["execution_id"],
                    rec["raw_key"],
                    rec["staging_key"],
                    rec["source_hash"],
                    rec["n_linhas"],
                    rec["n_colunas"],
                    rec["n_bad_lines"],
                    rec["encoding"],
                    rec["delimiter"],
                    Json(rec["column_map"]),
                    rec["status"],
                    rec["error_message"],
                ),
            )
            conn.commit()


# Auditoria (parquet)
def _gravar_auditoria(minio: ClienteMinio, execution_id: str, registros: List[dict]) -> None:
    df = pd.DataFrame(registros)
    if "column_map" in df.columns:
        df["column_map"] = df["column_map"].apply(lambda v: json.dumps(v, ensure_ascii=False))
    import io as _io

    buf = _io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    key = f"{AUDIT_PREFIX}execution_id={execution_id}/part-0.parquet"
    minio.put_object(key, buf.getvalue())
    if _LOCAL_ARTIFACTS:
        local = SCRIPTS_DIR / f"auditoria_staging_{execution_id}.parquet"
        df.to_parquet(local, engine="pyarrow", index=False)
        log.info("Auditoria: s3://%s/%s (cópia local: %s)", MINIO_BUCKET, key, local)
    else:
        log.info("Auditoria: s3://%s/%s", MINIO_BUCKET, key)
