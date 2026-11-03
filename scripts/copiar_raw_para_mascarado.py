"""Cópia controlada ``raw/`` -> ``masked/`` antes do mascaramento.

O raw é evidência de origem e não deve ser reescrito.  Esta etapa espelha cada
objeto elegível para ``masked/``; só a cópia é alterada por
``mascarar_minio.py``.  A tag ``source_etag`` torna a operação incremental: se
um arquivo do SFTP mudar no mesmo caminho, uma nova cópia é feita e volta a
ser mascarada antes de chegar à staging.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlencode

import psycopg2
from dotenv import load_dotenv

_plugins = Path(__file__).resolve().parents[1] / "plugins"
if str(_plugins) not in sys.path:
    sys.path.insert(0, str(_plugins))

from cliente_minio import ClienteMinio  # noqa: E402

load_dotenv()

RAW_PREFIX = "raw/"
MASKED_PREFIX = "masked/"
SCHEMA = os.environ.get("LAKE_SCHEMA", "lake")
CONTROL_TABLE = "_raw_to_masked_log"


def _tags(client: ClienteMinio, key: str) -> Dict[str, str]:
    return {tag["Key"]: tag["Value"] for tag in client.get_tagging(key)}


def _conn_str() -> str:
    return (
        f"host={os.environ['DB_DW_HOST_MCID']} port={os.environ.get('DB_DW_PORT_MCID', '5432')} "
        f"dbname={os.environ['DB_DW_DBNAME_MCID']} user={os.environ['DB_DW_USER_MCID']} "
        f"password={os.environ['DB_DW_PASSWORD_MCID']}"
    )


def _copias_atuais(conn_str: str) -> Dict[str, str]:
    with psycopg2.connect(conn_str) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.{CONTROL_TABLE} (
                source_key text PRIMARY KEY, source_etag text NOT NULL,
                masked_key text NOT NULL, copied_at timestamptz NOT NULL DEFAULT now()
            )"""
        )
        cur.execute(f"SELECT source_key, source_etag FROM {SCHEMA}.{CONTROL_TABLE}")
        return dict(cur.fetchall())


def _registrar_copia(conn_str: str, source_key: str, source_etag: str, masked_key: str) -> None:
    with psycopg2.connect(conn_str) as conn, conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {SCHEMA}.{CONTROL_TABLE}
                (source_key, source_etag, masked_key)
                VALUES (%s, %s, %s)
                ON CONFLICT (source_key) DO UPDATE SET
                    source_etag = EXCLUDED.source_etag,
                    masked_key = EXCLUDED.masked_key,
                    copied_at = now()""",
            (source_key, source_etag, masked_key),
        )


def run(
    limit: int = 0,
    pattern: str = "",
    only_ext: str = "",
    max_size_mb: int = 0,
    source_prefix: str = RAW_PREFIX,
    target_prefix: str = MASKED_PREFIX,
) -> Dict[str, int]:
    """Espelha objetos de raw para masked sem modificar a evidência original."""
    client = ClienteMinio()
    conn_str = _conn_str()
    copies = _copias_atuais(conn_str)
    ext_filter = {f".{item.strip().lstrip('.').lower()}" for item in only_ext.split(",") if item.strip()}
    counts: Dict[str, int] = {"copied": 0, "skipped_current": 0}
    handled = 0

    for key, size in client.listar_objetos(source_prefix):
        if key.endswith("/") or (pattern and pattern not in key):
            continue
        if ext_filter and Path(key).suffix.lower() not in ext_filter:
            continue
        if max_size_mb and size > max_size_mb * 1024 * 1024:
            continue
        if limit and handled >= limit:
            break
        handled += 1

        source_head = client.s3.head_object(Bucket=client.bucket, Key=key)
        source_etag = source_head["ETag"].strip('"')
        target = target_prefix + key[len(source_prefix) :]
        if copies.get(key) == source_etag:
            counts["skipped_current"] += 1
            continue

        # Há cortes legados do CadÚnico que já foram mascarados em raw/. Mantém a
        # marca ao copiá-los para que nunca sofram um segundo HMAC.
        source_masked = _tags(client, key).get("masked") == "true"
        client.s3.copy_object(
            Bucket=client.bucket,
            Key=target,
            CopySource={"Bucket": client.bucket, "Key": key},
            MetadataDirective="COPY",
            TaggingDirective="REPLACE",
            Tagging=urlencode(
                {
                    "masked": "true" if source_masked else "false",
                    "source_etag": source_etag,
                }
            ),
        )
        _registrar_copia(conn_str, key, source_etag, target)
        counts["copied"] += 1
    return counts
