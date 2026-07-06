import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import boto3
import psycopg2
from dotenv import load_dotenv

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

SCHEMA    = os.environ.get("SFTP_SCHEMA", "sftp")
LOG_TABLE = "_ingest_minio_log"

_LOG_FILE = (
    Path(__file__).parent
    / f"verificar_minio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
_formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

logging.root.setLevel(logging.INFO)
logging.root.addHandler(
    (lambda h: (h.setFormatter(_formatter), h)[1])(
        logging.FileHandler(_LOG_FILE, encoding="utf-8")
    )
)
logging.root.addHandler(
    (lambda h: (h.setFormatter(_formatter), h)[1])(
        logging.StreamHandler(sys.stdout)
    )
)

log = logging.getLogger(__name__)

def _conn_str() -> str:
    return (
        f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )


def _criar_cliente_minio():
    endpoint = MINIO_ENDPOINT
    if not endpoint.startswith("http"):
        endpoint = f"http://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _format_size(size_bytes: int) -> str:
    value: float = size_bytes
    for unit in ["B", "KB", "MB", "GB"]:
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def _obter_inseridos(conn_str: str) -> list:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT id, sftp_path, file_name, minio_key, file_size
                FROM {SCHEMA}.{LOG_TABLE}
                WHERE status = 'success'
                ORDER BY id
            """)
            return cur.fetchall()


def _marcar_como_erro(conn_str: str, row_id: int, motivo: str) -> None:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {SCHEMA}.{LOG_TABLE}
                SET status = 'error', error_message = %s, finished_at = NOW()
                WHERE id = %s
            """, (motivo[:500], row_id))
            conn.commit()

def main() -> None:
    log.info("Log: %s", _LOG_FILE)

    conn_str = _conn_str()
    s3 = _criar_cliente_minio()

    inseridos = _obter_inseridos(conn_str)
    total = len(inseridos)
    log.info("%d arquivo(s) com status 'success' para verificar.", total)

    ok = ausente = divergente = erro = 0

    for idx, (row_id, sftp_path, file_name, minio_key, file_size) in enumerate(inseridos, 1):
        if not minio_key:
            log.warning("[%d/%d] %s — minio_key ausente no banco, marcando como erro", idx, total, file_name)
            _marcar_como_erro(conn_str, row_id, "minio_key ausente")
            divergente += 1
            continue

        try:
            resp = s3.head_object(Bucket=MINIO_BUCKET, Key=minio_key)
            content_length = resp["ContentLength"]

            if file_size is not None and content_length != file_size:
                log.error(
                    "[%d/%d] DIVERGENTE %s — esperado %s, MinIO tem %s",
                    idx, total, file_name,
                    _format_size(file_size), _format_size(content_length),
                )
                _marcar_como_erro(
                    conn_str, row_id,
                    f"tamanho divergente: esperado {file_size}B, MinIO tem {content_length}B",
                )
                divergente += 1
            else:
                log.info("[%d/%d] ✓ %s (%s)", idx, total, file_name, _format_size(content_length))
                ok += 1

        except s3.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("404", "NoSuchKey"):
                log.error(
                    "[%d/%d] AUSENTE %s — não encontrado no MinIO (%s)",
                    idx, total, file_name, minio_key,
                )
                _marcar_como_erro(conn_str, row_id, "objeto não encontrado no MinIO")
                ausente += 1
            else:
                log.error("[%d/%d] ERRO ao verificar %s: %s", idx, total, file_name, e)
                erro += 1

        except Exception as e:
            log.error("[%d/%d] ERRO ao verificar %s: %s", idx, total, file_name, e)
            erro += 1

    log.info("=" * 60)
    log.info("Verificação concluída.")
    log.info("  ✓ OK:          %d", ok)
    log.info("  ✗ Ausentes:    %d", ausente)
    log.info("  ✗ Divergentes: %d", divergente)
    log.info("  ✗ Erros:       %d", erro)
    if ausente + divergente > 0:
        log.info(
            "%d arquivo(s) marcado(s) como 'error' — serão reinseridos na próxima execução.",
            ausente + divergente,
        )
    log.info("Log: %s", _LOG_FILE)


if __name__ == "__main__":
    main()
