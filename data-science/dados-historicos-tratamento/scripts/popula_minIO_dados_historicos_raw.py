"""
Script standalone para exportar tabelas do schema ``dados_historicos``
do PostgreSQL para o bucket ``data-lake-mcid`` no MinIO, no caminho
``raw/dados_historicos/``.

Pipeline
--------
1. Lista todas as tabelas do schema.
2. Para cada tabela:
   a. Leitura chunked (sempre, sem branch condicional) com ``ORDER BY ctid``.
   b. Grava CSV temporário com separador ``;`` e sem índice pandas.
   c. Upload para MinIO em ``raw/dados_historicos/<tabela>.csv``.
   d. Verifica integridade via ``stat_object``.
   e. Arquivo temporário removido automaticamente (``TemporaryDirectory``).
3. Exibe relatório final agregado.

Sobrescrita
-----------
Se o objeto já existir no MinIO, será **sobrescrito silenciosamente**.
Este é o comportamento esperado para re-execuções do script (atualizar
os dados no data lake).

Uso
---
.. code-block:: bash

    uv run python scripts/popula_minIO_dados_historicos_raw.py

Requer ``.env`` com as variáveis PostgreSQL e MinIO configuradas (veja
``.env.example``).
"""

from __future__ import annotations

import logging
import os
import tempfile
import time
import urllib.parse
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from sqlalchemy import create_engine, inspect, text
from tqdm import tqdm

# ── Logging ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── Configuração a partir do .env ──────────────────────────────────────────

load_dotenv()

db_name: str | None = os.getenv("DB_NAME")
user: str | None = os.getenv("DB_USER")
password: str | None = os.getenv("DB_PASSWORD")
host: str | None = os.getenv("DB_HOST")
port: str | None = os.getenv("DB_PORT")

minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
minio_access_key: str | None = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key: str | None = os.getenv("MINIO_SECRET_KEY")
minio_bucket: str = os.getenv("MINIO_BUCKET", "data-lake-mcid")
minio_secure: bool = os.getenv("MINIO_SECURE", "False").lower() == "true"

# Constantes
CHUNK_SIZE: int = 50000
SCHEMA_NAME: str = "dados_historicos"
MINIO_PREFIX: str = "raw/dados_historicos"

# ── Validação ──────────────────────────────────────────────────────────────

_required_vars = [
    db_name,
    user,
    password,
    host,
    port,
    minio_access_key,
    minio_secret_key,
]
if not all(_required_vars):
    raise EnvironmentError(
        "Variável de ambiente obrigatória não definida no .env. "
        "Consulte .env.example para a lista completa."
    )

# ── Conexões ───────────────────────────────────────────────────────────────

# PostgreSQL (senha com URL-encode para caracteres especiais)
_password_encoded = urllib.parse.quote_plus(password)  # type: ignore[arg-type]
_db_url = f"postgresql://{user}:{_password_encoded}@{host}:{port}/{db_name}"
engine = create_engine(_db_url)

# MinIO
try:
    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure,
    )
    if not minio_client.bucket_exists(minio_bucket):
        minio_client.make_bucket(minio_bucket)
        logger.info("Bucket '%s' criado com sucesso.", minio_bucket)
    else:
        logger.info("Bucket '%s' já existe.", minio_bucket)
except Exception as exc:
    logger.error("Erro ao conectar ao MinIO: %s", exc)
    raise


# ── Funções auxiliares ─────────────────────────────────────────────────────


def get_table_row_count(schema_name: str, table_name: str) -> int | None:
    """Retorna a contagem aproximada de linhas via ``pg_class.reltuples``.

    Usada apenas para a barra de progresso (``tqdm``); **não** influencia
    a decisão de chunked read (que é sempre aplicada).
    """
    try:
        with engine.connect() as conn:
            query = text(
                """
                SELECT reltuples::bigint AS row_count
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = :schema AND c.relname = :table
                """
            )
            result = conn.execute(
                query, {"schema": schema_name, "table": table_name}
            ).scalar()
            return result if result and result > 0 else None
    except Exception:
        logger.debug("Não foi possível estimar row_count para %s.", table_name)
        return None


def chunked_read_sql(
    query: str,
    engine: object,
    chunk_size: int = CHUNK_SIZE,
):
    """Lê query em chunks com ``ORDER BY ctid`` para estabilidade entre chunks.

    Yields
    ------
    pd.DataFrame
        Um chunk de até *chunk_size* linhas.
    """
    offset = 0
    while True:
        chunk_query = text(f"{query} ORDER BY ctid OFFSET {offset} LIMIT {chunk_size}")
        chunk: pd.DataFrame = pd.read_sql(chunk_query, engine)
        if chunk.empty:
            break
        yield chunk
        offset += chunk_size


def save_table_to_csv(
    schema_name: str,
    table_name: str,
    temp_dir: str,
    chunk_size: int = CHUNK_SIZE,
) -> tuple[str | None, int]:
    """Salva a tabela em CSV (``sep=';'``) no *temp_dir*.

    Sempre usa leitura chunked, independentemente do tamanho estimado.
    Tabelas vazias geram CSV apenas com header.

    Returns
    -------
    tuple[str | None, int]
        ``(caminho_csv, total_linhas)``.  ``caminho_csv`` é ``None`` em
        caso de erro.
    """
    try:
        csv_path = Path(temp_dir) / f"{table_name}.csv"
        row_estimate = get_table_row_count(schema_name, table_name)
        total_rows = 0
        first_chunk = True

        base_query = f'SELECT * FROM {schema_name}."{table_name}"'

        pbar = tqdm(
            total=row_estimate,
            desc=f"Lendo {table_name:<50s}",
            unit=" reg",
            bar_format="{desc} {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}]",
        )
        with pbar:
            for chunk in chunked_read_sql(base_query, engine, chunk_size):
                chunk.to_csv(
                    csv_path,
                    sep=";",
                    mode="a",
                    index=False,
                    header=first_chunk,
                )
                total_rows += len(chunk)
                first_chunk = False
                pbar.update(len(chunk))

        # Tabela vazia: gera CSV apenas com header
        if first_chunk:
            cols_query = text(f'SELECT * FROM {schema_name}."{table_name}" LIMIT 0')
            cols = pd.read_sql(cols_query, engine).columns
            pd.DataFrame(columns=cols).to_csv(csv_path, sep=";", index=False)
            logger.info("Tabela %s: vazia — CSV apenas com header.", table_name)

        logger.info("Tabela %s: %d registros → %s", table_name, total_rows, csv_path)
        return str(csv_path), total_rows

    except Exception as exc:
        logger.error("Erro ao processar tabela %s: %s", table_name, exc)
        return None, 0


def upload_to_minio(
    csv_path: str,
    table_name: str,
    total_rows: int,
) -> tuple[bool, int]:
    """Faz upload do CSV para o MinIO e verifica integridade.

    O objeto MinIO será sobrescrito silenciosamente se já existir.

    Returns
    -------
    tuple[bool, int]
        ``(sucesso, bytes_enviados)``.
    """
    # Objeto no MinIO: raw/dados_historicos/<tabela>.csv
    object_name = f"{MINIO_PREFIX}/{table_name}.csv"

    if not os.path.exists(csv_path):
        logger.error("Arquivo %s não encontrado para upload.", csv_path)
        return False, 0

    try:
        file_size = os.path.getsize(csv_path)
        logger.info(
            "Enviando %s → MinIO (%s, %d bytes)...",
            table_name,
            object_name,
            file_size,
        )

        with open(csv_path, "rb") as file_data:
            minio_client.put_object(
                bucket_name=minio_bucket,
                object_name=object_name,
                data=file_data,
                length=file_size,
                content_type="text/csv",
            )

        # Verificação pós-upload
        try:
            stat = minio_client.stat_object(minio_bucket, object_name)
            logger.info(
                "✓ Upload concluído: %s (%d bytes, %d registros)",
                object_name,
                stat.size,
                total_rows,
            )
            return True, file_size
        except S3Error:
            logger.error("Falha ao verificar upload de %s.", object_name)
            return False, 0

    except Exception as exc:
        logger.error("Erro durante upload de %s: %s", table_name, exc)
        return False, 0


def exibir_relatorio_final(
    total_tables: int,
    success_count: int,
    fail_count: int,
    total_rows: int,
    total_bytes: int,
    elapsed_seconds: float,
) -> None:
    """Exibe relatório agregado ao final da transferência."""
    elapsed_min = elapsed_seconds / 60.0
    logger.info("%s", "=" * 60)
    logger.info("RELATÓRIO FINAL — Transferência MinIO")
    logger.info("%s", "-" * 60)
    logger.info("Tabelas no schema       : %d", total_tables)
    logger.info("Transferidas com sucesso: %d", success_count)
    logger.info("Falhas                  : %d", fail_count)
    logger.info("Total de registros      : %d", total_rows)
    logger.info(
        "Total de bytes          : %s (%d bytes)",
        _format_bytes(total_bytes),
        total_bytes,
    )
    logger.info(
        "Tempo total             : %.1f min (%.1f s)", elapsed_min, elapsed_seconds
    )
    logger.info("%s", "=" * 60)


def _format_bytes(num_bytes: float) -> str:
    """Formata bytes em unidade legível."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


# ── Pipeline principal ─────────────────────────────────────────────────────


def transfer_schema_tables(
    schema_name: str = SCHEMA_NAME,
    chunk_size: int = CHUNK_SIZE,
) -> None:
    """Transfere todas as tabelas de um schema para o MinIO."""
    inspector = inspect(engine)
    tables: list[str] = inspector.get_table_names(schema=schema_name)

    if not tables:
        logger.warning("Nenhuma tabela encontrada no schema '%s'.", schema_name)
        return

    logger.info(
        "Iniciando transferência de %d tabelas do schema '%s'.",
        len(tables),
        schema_name,
    )

    # Contadores para o relatório final
    success_count = 0
    fail_count = 0
    total_rows_all = 0
    total_bytes_all = 0
    t0 = time.monotonic()

    # Único diretório temporário para toda a execução
    with tempfile.TemporaryDirectory() as temp_dir:
        for table_name in tables:
            logger.info("%s", "-" * 60)
            logger.info("Tabela: %s", table_name)

            try:
                # 1. Salvar CSV
                csv_path, rows = save_table_to_csv(
                    schema_name, table_name, temp_dir, chunk_size
                )

                if csv_path is None:
                    logger.warning("✗ Tabela %s: erro ao gerar CSV.", table_name)
                    fail_count += 1
                    continue

                if rows == 0:
                    logger.info("Tabela %s: vazia (0 registros).", table_name)

                # 2. Upload para MinIO (tabelas vazias também são enviadas)
                upload_ok, file_bytes = upload_to_minio(csv_path, table_name, rows)

                if upload_ok:
                    success_count += 1
                    total_rows_all += rows
                    total_bytes_all += file_bytes
                else:
                    logger.warning(
                        "✗ Tabela %s: processada, mas upload falhou.",
                        table_name,
                    )
                    fail_count += 1

            except Exception as exc:
                logger.error(
                    "Erro inesperado ao processar tabela %s: %s",
                    table_name,
                    exc,
                )
                fail_count += 1

    elapsed = time.monotonic() - t0
    exibir_relatorio_final(
        total_tables=len(tables),
        success_count=success_count,
        fail_count=fail_count,
        total_rows=total_rows_all,
        total_bytes=total_bytes_all,
        elapsed_seconds=elapsed,
    )


# ── Entry point ────────────────────────────────────────────────────────────


def main() -> None:
    """Testa conexões e inicia a transferência."""
    try:
        # Testar PostgreSQL
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            logger.info("Conexão PostgreSQL OK (resultado: %s).", result)

        # Testar MinIO
        try:
            minio_client.list_buckets()
            logger.info("Conexão MinIO OK.")
        except Exception as exc:
            logger.error("Falha na conexão MinIO: %s", exc)
            return

        transfer_schema_tables()

    except Exception as exc:
        logger.error("Erro na execução principal: %s", exc)
        raise


if __name__ == "__main__":
    main()
