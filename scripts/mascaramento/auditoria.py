"""Registro de cada arquivo em lake._masking_log e parquet de auditoria."""

import io
import json
import logging
from typing import List

import pandas as pd
import psycopg2
from psycopg2.extras import Json

from cliente_minio import ClienteMinio

from .config import (
    SCRIPTS_DIR,
    AUDIT_PREFIX,
    CONTROL_TABLE,
    MINIO_BUCKET,
    PG_DBNAME,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    SCHEMA,
    _LOCAL_ARTIFACTS,
)

log = logging.getLogger(__name__)


# Infra: conexões
def _conn_str() -> str:
    return (
        f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )


def _criar_control_table(conn_str: str) -> None:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.{CONTROL_TABLE} (
                    id                  SERIAL PRIMARY KEY,
                    execution_id        TEXT,
                    minio_key           TEXT NOT NULL,
                    file_name           TEXT,
                    source_hash         TEXT,
                    masked_hash         TEXT,
                    masked_columns      JSONB,
                    registros_total     BIGINT,
                    registros_alterados BIGINT,
                    status              TEXT,
                    error_message       TEXT,
                    created_at          TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (minio_key, source_hash)
                );
            """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_masking_log_status
                ON {SCHEMA}.{CONTROL_TABLE} (status);
            """
            )
            conn.commit()
    log.info("Tabela de controle %s.%s garantida.", SCHEMA, CONTROL_TABLE)


def _carregar_masked_hashes(conn_str: str) -> set:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT masked_hash FROM {SCHEMA}.{CONTROL_TABLE}
                WHERE status = 'masked' AND masked_hash IS NOT NULL
            """
            )
            return {row[0] for row in cur.fetchall()}


def _registrar_control(conn_str: str, row: dict) -> None:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SCHEMA}.{CONTROL_TABLE}
                    (execution_id, minio_key, file_name, source_hash, masked_hash,
                     masked_columns, registros_total, registros_alterados, status,
                     error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (minio_key, source_hash) DO UPDATE SET
                    execution_id        = EXCLUDED.execution_id,
                    masked_hash         = EXCLUDED.masked_hash,
                    masked_columns      = EXCLUDED.masked_columns,
                    registros_total     = EXCLUDED.registros_total,
                    registros_alterados = EXCLUDED.registros_alterados,
                    status              = EXCLUDED.status,
                    error_message       = EXCLUDED.error_message,
                    created_at          = NOW()
                """,
                (
                    row["execution_id"],
                    row["file"],
                    row["file_name"],
                    row["hash_before"],
                    row["hash_after"],
                    Json(row["masked_columns"]),
                    row["registros_total"],
                    row["registros_alterados"],
                    row["status"],
                    row["error_message"],
                ),
            )
            conn.commit()


# Auditoria (parquet)
def _gravar_auditoria(
    minio: ClienteMinio, execution_id: str, registros: List[dict]
) -> str:
    df = pd.DataFrame(registros)
    if "masked_columns" in df.columns:
        df["masked_columns"] = df["masked_columns"].apply(
            lambda v: json.dumps(v, ensure_ascii=False)
        )
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    buf.seek(0)
    key = f"{AUDIT_PREFIX}execution_id={execution_id}/part-0.parquet"
    minio.put_object(key, buf.getvalue())

    if _LOCAL_ARTIFACTS:
        local = SCRIPTS_DIR / f"auditoria_mascaramento_{execution_id}.parquet"
        df.to_parquet(local, engine="pyarrow", index=False)
        log.info("Auditoria: s3://%s/%s (cópia local: %s)", MINIO_BUCKET, key, local)
    else:
        log.info("Auditoria: s3://%s/%s", MINIO_BUCKET, key)
    return key
