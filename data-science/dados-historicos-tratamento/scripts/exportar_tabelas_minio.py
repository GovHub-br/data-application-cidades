"""
Script standalone para exportar tabelas do schema ``dados_historicos``
do PostgreSQL (ou de um diretório local) para o bucket ``data-lake-mcid``
no MinIO.

Pipeline
--------
1. Lista todas as tabelas do schema (ou usa lista fornecida via ``--tables``).
2. Para cada tabela:
   a. Leitura chunked com ``ORDER BY ctid``.
   b. Grava CSV (sep ``;``) ou Parquet em diretório temporário.
   c. Upload para MinIO.
   d. Verifica integridade via ``stat_object``.
3. Exibe relatório final agregado com log de exportação.

Sobrescrita
-----------
Se o objeto já existir no MinIO, será **sobrescrito silenciosamente**.

Uso
---
.. code-block:: bash

    # Modo PostgreSQL (CSV padrão)
    uv run python scripts/exportar_tabelas_minio.py

    # Modo PostgreSQL com Parquet
    uv run python scripts/exportar_tabelas_minio.py --parquet

    # Tabelas específicas (modo PostgreSQL)
    uv run python scripts/exportar_tabelas_minio.py --tables tabela_a tabela_b

    # Schema e prefixo personalizados
    uv run python scripts/exportar_tabelas_minio.py --schema-name meu_schema --minio-prefix custom/prefix

    # Log personalizado
    uv run python scripts/exportar_tabelas_minio.py --log-path custom_log.csv

    # Modo diretório local
    uv run python scripts/exportar_tabelas_minio.py --source-dir data/treated_tables

    # Modo diretório local com Parquet
    uv run python scripts/exportar_tabelas_minio.py --source-dir data/treated_tables --parquet

    # Modo diretório local com tabelas específicas
    uv run python scripts/exportar_tabelas_minio.py --source-dir data/treated_tables --tables tabela_a tabela_b

Requer ``.env`` com as variáveis MinIO configuradas. No modo PostgreSQL,
também requer as variáveis PostgreSQL (veja ``.env.example``).
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import tempfile
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from pyarrow import parquet as pq
from sqlalchemy import Engine, create_engine, inspect, text
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

# ── Conexão PostgreSQL (lazy) ──────────────────────────────────────────────


def _criar_conexao_pg() -> Engine:
    """Valida variáveis de ambiente e cria engine PostgreSQL.

    Retorna o engine SQLAlchemy.
    """
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

    _password_encoded = urllib.parse.quote_plus(password)  # type: ignore[arg-type]
    _db_url = f"postgresql://{user}:{_password_encoded}@{host}:{port}/{db_name}"
    engine = create_engine(_db_url)
    return engine


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


def get_table_row_count(
    engine: Engine, schema_name: str, table_name: str
) -> int | None:
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
    engine: Engine,
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


def write_export_log(
    log_path: str,
    source_table: str,
    destination_path: str,
    rows: int,
    bytes_: int,
    format_: str,
    status: str,
) -> None:
    """Registra uma linha no log de exportação CSV.

    Cria o arquivo com cabeçalho se não existir; sempre adiciona ao final.
    """
    log_file = Path(log_path)
    write_header = not log_file.exists()

    with log_file.open("a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(
                [
                    "source_table",
                    "destination_path",
                    "rows",
                    "bytes",
                    "format",
                    "status",
                    "written_at",
                ]
            )
        writer.writerow(
            [
                source_table,
                destination_path,
                rows,
                bytes_,
                format_,
                status,
                datetime.now(timezone.utc).isoformat(),
            ]
        )


def save_table_to_csv(
    schema_name: str,
    table_name: str,
    temp_dir: str,
    engine: Engine,
    chunk_size: int = CHUNK_SIZE,
) -> tuple[str | None, int]:
    """Salva a tabela em CSV (``sep=';'``) no *temp_dir*.

    Sempre usa leitura chunked, independentemente do tamanho estimado.
    Tabelas vazias retornam ``(None, 0)`` — nenhum arquivo gerado.

    Returns
    -------
    tuple[str | None, int]
        ``(caminho_csv, total_linhas)``.  ``caminho_csv`` é ``None`` em
        caso de erro ou tabela vazia.
    """
    try:
        csv_path = Path(temp_dir) / f"{table_name}.csv"
        row_estimate = get_table_row_count(engine, schema_name, table_name)
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

        # Tabela vazia: nenhum arquivo gerado
        if first_chunk:
            logger.info("Tabela %s: vazia — nenhum arquivo gerado.", table_name)
            return None, 0

        logger.info("Tabela %s: %d registros → %s", table_name, total_rows, csv_path)
        return str(csv_path), total_rows

    except Exception as exc:
        logger.error("Erro ao processar tabela %s: %s", table_name, exc)
        return None, 0


def save_table_to_parquet(
    schema_name: str,
    table_name: str,
    temp_dir: str,
    engine: Engine,
    chunk_size: int = CHUNK_SIZE,
) -> tuple[str | None, int]:
    """Salva a tabela em Parquet no *temp_dir* usando escrita chunked.

    Returns
    -------
    tuple[str | None, int]
        ``(caminho_parquet, total_linhas)``.  ``caminho_parquet`` é ``None``
        em caso de erro ou tabela vazia.
    """
    parquet_path = Path(temp_dir) / f"{table_name}.parquet"
    row_estimate = get_table_row_count(engine, schema_name, table_name)
    total_rows = 0
    writer: pq.ParquetWriter | None = None

    base_query = f'SELECT * FROM {schema_name}."{table_name}"'

    pbar = tqdm(
        total=row_estimate,
        desc=f"Lendo {table_name:<50s}",
        unit=" reg",
        bar_format="{desc} {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}]",
    )

    try:
        with pbar:
            for chunk in chunked_read_sql(base_query, engine, chunk_size):
                table = pa.Table.from_pandas(chunk.astype(str))
                if writer is None:
                    writer = pq.ParquetWriter(str(parquet_path), table.schema)
                assert writer is not None
                writer.write_table(table)
                total_rows += len(chunk)
                pbar.update(len(chunk))

        if total_rows == 0:
            logger.info("Tabela %s: vazia — nenhum arquivo gerado.", table_name)
            return None, 0

        logger.info(
            "Tabela %s: %d registros → %s", table_name, total_rows, parquet_path
        )
        return str(parquet_path), total_rows

    except Exception as exc:
        logger.error("Erro ao processar tabela %s como parquet: %s", table_name, exc)
        return None, 0
    finally:
        if writer is not None:
            writer.close()


def upload_to_minio(
    file_path: str,
    table_name: str,
    total_rows: int,
    as_parquet: bool = False,
    minio_prefix: str = "raw/dados_historicos",
) -> tuple[bool, int]:
    """Faz upload do arquivo para o MinIO e verifica integridade.

    O objeto MinIO será sobrescrito silenciosamente se já existir.

    Returns
    -------
    tuple[bool, int]
        ``(sucesso, bytes_enviados)``.
    """
    ext = "parquet" if as_parquet else "csv"
    object_name = f"{minio_prefix}/{table_name}.{ext}"
    content_type = "application/octet-stream" if as_parquet else "text/csv"

    if not os.path.exists(file_path):
        logger.error("Arquivo %s não encontrado para upload.", file_path)
        return False, 0

    try:
        file_size = os.path.getsize(file_path)
        logger.info(
            "Enviando %s → MinIO (%s, %d bytes)...",
            table_name,
            object_name,
            file_size,
        )

        with open(file_path, "rb") as file_data:
            minio_client.put_object(
                bucket_name=minio_bucket,
                object_name=object_name,
                data=file_data,
                length=file_size,
                content_type=content_type,
            )

        # Verificação pós-upload
        try:
            stat = minio_client.stat_object(minio_bucket, object_name)
            logger.info(
                "\u2713 Upload conclu\u00eddo: %s (%d bytes, %d registros)",
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
    skipped_empty_count: int = 0,
    total_rows: int = 0,
    total_bytes: int = 0,
    elapsed_seconds: float = 0.0,
) -> None:
    """Exibe relatório agregado ao final da transferência."""
    elapsed_min = elapsed_seconds / 60.0
    logger.info("%s", "=" * 60)
    logger.info("RELATÓRIO FINAL — Transferência MinIO")
    logger.info("%s", "-" * 60)
    logger.info("Tabelas no schema       : %d", total_tables)
    logger.info("Transferidas com sucesso: %d", success_count)
    logger.info("Tabelas vazias (skip)   : %d", skipped_empty_count)
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


# ── Funções para modo diretório local ──────────────────────────────────────


def _limpar_nome_arquivo(filename: str) -> str:
    """Remove extensão ``.csv`` e sufixo ``_tratado`` do nome do arquivo.

    Examples
    --------
    ``bb_2011_01_janeiro_rel_11jan2011_tratado.csv`` → ``bb_2011_01_janeiro_rel_11jan2011``
    ``_classificacao.csv`` → ``_classificacao``
    """
    name = filename
    if name.endswith(".csv"):
        name = name[:-4]
    if name.endswith("_tratado"):
        name = name[:-8]
    return name


def _construir_mapa_arquivos(
    source_dir: str, tables: list[str] | None
) -> dict[str, Path]:
    """Lista ``*.csv`` em *source_dir* e mapeia nome limpo → Path.

    Se *tables* for fornecido, filtra apenas os arquivos cujo nome limpo
    esteja na lista. Se algum nome em *tables* não for encontrado, emite
    erro e encerra.
    """
    file_map: dict[str, Path] = {}
    missing: list[str] = []

    for csv_path in sorted(Path(source_dir).glob("*.csv")):
        nome_limpo = _limpar_nome_arquivo(csv_path.name)
        if nome_limpo in file_map:
            logger.warning(
                "Colisão de nome limpo '%s' para arquivos: %s e %s",
                nome_limpo,
                file_map[nome_limpo],
                csv_path,
            )
        file_map[nome_limpo] = csv_path

    if tables is not None:
        tables_set = set(tables)
        # Filtra apenas os que estão na lista
        file_map = {k: v for k, v in file_map.items() if k in tables_set}
        missing = [t for t in tables if t not in file_map]
        if missing:
            logger.error(
                "Tabela(s) não encontrada(s) no diretório '%s': %s",
                source_dir,
                ", ".join(missing),
            )
            sys.exit(1)

    return file_map


def _read_csv_chunked(csv_path: Path, chunk_size: int):
    """Lê CSV chunked com fallback de encoding UTF-8 → latin1.

    Yields
    ------
    pd.DataFrame
    """
    for encoding in ("utf-8", "latin1"):
        try:
            reader = pd.read_csv(
                csv_path,
                chunksize=chunk_size,
                sep="\t",
                encoding=encoding,
            )
            for chunk in reader:
                yield chunk
            return
        except UnicodeDecodeError:
            logger.debug(
                "Falha encoding %s para %s, tentando próximo.",
                encoding,
                csv_path.name,
            )
            continue
    # Se ambos os encodings falharem
    logger.error(
        "Não foi possível ler o arquivo %s com UTF-8 nem latin1.",
        csv_path.name,
    )


def save_table_to_csv_from_file(
    csv_path: Path, temp_dir: str, chunk_size: int
) -> tuple[str | None, int]:
    """Lê CSV tab-separated e escreve como CSV (sep ``;``) em *temp_dir*.

    Returns
    -------
    tuple[str | None, int]
        ``(caminho_csv, total_linhas)``.  ``None, 0`` em caso de erro
        ou arquivo vazio.
    """
    try:
        out_path = Path(temp_dir) / f"{csv_path.stem}.csv"
        total_rows = 0
        first_chunk = True

        for chunk in _read_csv_chunked(csv_path, chunk_size):
            chunk.to_csv(
                out_path,
                sep=";",
                mode="a",
                index=False,
                header=first_chunk,
            )
            total_rows += len(chunk)
            first_chunk = False

        if first_chunk:
            logger.info("Arquivo %s: vazio — nenhum arquivo gerado.", csv_path.name)
            return None, 0

        logger.info(
            "Arquivo %s: %d registros → %s", csv_path.name, total_rows, out_path
        )
        return str(out_path), total_rows

    except Exception as exc:
        logger.error("Erro ao processar arquivo %s: %s", csv_path.name, exc)
        return None, 0


def save_table_to_parquet_from_file(
    csv_path: Path, temp_dir: str, chunk_size: int
) -> tuple[str | None, int]:
    """Lê CSV tab-separated e escreve como Parquet em *temp_dir*.

    Returns
    -------
    tuple[str | None, int]
        ``(caminho_parquet, total_linhas)``.  ``None, 0`` em caso de erro
        ou arquivo vazio.
    """
    parquet_path = Path(temp_dir) / f"{csv_path.stem}.parquet"
    total_rows = 0
    writer: pq.ParquetWriter | None = None

    try:
        for chunk in _read_csv_chunked(csv_path, chunk_size):
            table = pa.Table.from_pandas(chunk.astype(str))
            if writer is None:
                writer = pq.ParquetWriter(str(parquet_path), table.schema)
            assert writer is not None
            writer.write_table(table)
            total_rows += len(chunk)

        if total_rows == 0:
            logger.info("Arquivo %s: vazio — nenhum arquivo gerado.", csv_path.name)
            return None, 0

        logger.info(
            "Arquivo %s: %d registros → %s",
            csv_path.name,
            total_rows,
            parquet_path,
        )
        return str(parquet_path), total_rows

    except Exception as exc:
        logger.error(
            "Erro ao processar arquivo %s como parquet: %s",
            csv_path.name,
            exc,
        )
        return None, 0
    finally:
        if writer is not None:
            writer.close()


def transfer_local_csv_dir(
    source_dir: str,
    minio_prefix: str,
    tables: list[str] | None,
    as_parquet: bool,
    log_path: str,
    chunk_size: int = CHUNK_SIZE,
) -> None:
    """Transfere CSVs de um diretório local para o MinIO.

    Pipeline
    --------
    1. Mapeia arquivos ``*.csv`` do diretório via ``_construir_mapa_arquivos``.
    2. Para cada arquivo, salva como CSV (sep ``;``) ou Parquet em diretório
       temporário.
    3. Faz upload para o MinIO.
    4. Exibe relatório final.
    """
    file_map = _construir_mapa_arquivos(source_dir, tables)
    if not file_map:
        logger.warning("Nenhum arquivo CSV encontrado em '%s'.", source_dir)
        return

    logger.info(
        "Iniciando transferência de %d arquivos do diretório '%s'.",
        len(file_map),
        source_dir,
    )

    success_count = 0
    fail_count = 0
    skipped_empty_count = 0
    total_rows_all = 0
    total_bytes_all = 0
    t0 = time.monotonic()

    with tempfile.TemporaryDirectory() as temp_dir:
        for nome_limpo, csv_path in file_map.items():
            logger.info("%s", "-" * 60)
            logger.info("Arquivo: %s → %s", csv_path.name, nome_limpo)

            try:
                fmt = "parquet" if as_parquet else "csv"

                # 1. Salvar dados (CSV ou Parquet)
                if as_parquet:
                    file_path, rows = save_table_to_parquet_from_file(
                        csv_path, temp_dir, chunk_size
                    )
                else:
                    file_path, rows = save_table_to_csv_from_file(
                        csv_path, temp_dir, chunk_size
                    )

                # Arquivo vazio — sem arquivo gerado
                if file_path is None and rows == 0:
                    logger.info("Arquivo %s: vazio — skipping.", csv_path.name)
                    write_export_log(
                        log_path=log_path,
                        source_table=nome_limpo,
                        destination_path="",
                        rows=0,
                        bytes_=0,
                        format_=fmt,
                        status="skipped_empty",
                    )
                    skipped_empty_count += 1
                    continue

                # Erro ao gerar arquivo
                if file_path is None:
                    logger.warning(
                        "\u2717 Arquivo %s: erro ao gerar arquivo.",
                        csv_path.name,
                    )
                    write_export_log(
                        log_path=log_path,
                        source_table=nome_limpo,
                        destination_path="",
                        rows=0,
                        bytes_=0,
                        format_=fmt,
                        status="error",
                    )
                    fail_count += 1
                    continue

                # 2. Upload para MinIO
                upload_ok, file_bytes = upload_to_minio(
                    file_path,
                    nome_limpo,
                    rows,
                    as_parquet=as_parquet,
                    minio_prefix=minio_prefix,
                )

                if upload_ok:
                    success_count += 1
                    total_rows_all += rows
                    total_bytes_all += file_bytes
                    ext = "parquet" if as_parquet else "csv"
                    object_name = f"{minio_prefix}/{nome_limpo}.{ext}"
                    write_export_log(
                        log_path=log_path,
                        source_table=nome_limpo,
                        destination_path=object_name,
                        rows=rows,
                        bytes_=file_bytes,
                        format_=fmt,
                        status="success",
                    )
                else:
                    logger.warning(
                        "\u2717 Arquivo %s: processado, mas upload falhou.",
                        csv_path.name,
                    )
                    fail_count += 1
                    write_export_log(
                        log_path=log_path,
                        source_table=nome_limpo,
                        destination_path="",
                        rows=rows,
                        bytes_=0,
                        format_=fmt,
                        status="error",
                    )

            except Exception as exc:
                logger.error(
                    "Erro inesperado ao processar arquivo %s: %s",
                    csv_path.name,
                    exc,
                )
                fmt = "parquet" if as_parquet else "csv"
                fail_count += 1
                write_export_log(
                    log_path=log_path,
                    source_table=nome_limpo,
                    destination_path="",
                    rows=0,
                    bytes_=0,
                    format_=fmt,
                    status="error",
                )

    elapsed = time.monotonic() - t0
    exibir_relatorio_final(
        total_tables=len(file_map),
        success_count=success_count,
        fail_count=fail_count,
        skipped_empty_count=skipped_empty_count,
        total_rows=total_rows_all,
        total_bytes=total_bytes_all,
        elapsed_seconds=elapsed,
    )


# ── Pipeline principal ─────────────────────────────────────────────────────


def transfer_schema_tables(
    schema_name: str,
    minio_prefix: str,
    tables: list[str] | None,
    as_parquet: bool,
    log_path: str,
    engine: Engine,
    chunk_size: int = CHUNK_SIZE,
) -> None:
    """Transfere tabelas de um schema para o MinIO."""
    inspector = inspect(engine)

    if tables is not None:
        table_list: list[str] = tables
    else:
        table_list = inspector.get_table_names(schema=schema_name)

    if not table_list:
        logger.warning("Nenhuma tabela encontrada no schema '%s'.", schema_name)
        return

    logger.info(
        "Iniciando transferência de %d tabelas do schema '%s'.",
        len(table_list),
        schema_name,
    )

    # Contadores para o relatório final
    success_count = 0
    fail_count = 0
    skipped_empty_count = 0
    total_rows_all = 0
    total_bytes_all = 0
    t0 = time.monotonic()

    # Único diretório temporário para toda a execução
    with tempfile.TemporaryDirectory() as temp_dir:
        for table_name in table_list:
            logger.info("%s", "-" * 60)
            logger.info("Tabela: %s", table_name)

            try:
                fmt = "parquet" if as_parquet else "csv"

                # 1. Salvar dados (CSV ou Parquet)
                if as_parquet:
                    file_path, rows = save_table_to_parquet(
                        schema_name, table_name, temp_dir, engine, chunk_size
                    )
                else:
                    file_path, rows = save_table_to_csv(
                        schema_name, table_name, temp_dir, engine, chunk_size
                    )

                # Tabela vazia — sem arquivo gerado
                if file_path is None and rows == 0:
                    logger.info("Tabela %s: vazia — skipping.", table_name)
                    write_export_log(
                        log_path=log_path,
                        source_table=table_name,
                        destination_path="",
                        rows=0,
                        bytes_=0,
                        format_=fmt,
                        status="skipped_empty",
                    )
                    skipped_empty_count += 1
                    continue

                # Erro ao gerar arquivo
                if file_path is None:
                    logger.warning(
                        "\u2717 Tabela %s: erro ao gerar arquivo.", table_name
                    )
                    write_export_log(
                        log_path=log_path,
                        source_table=table_name,
                        destination_path="",
                        rows=0,
                        bytes_=0,
                        format_=fmt,
                        status="error",
                    )
                    fail_count += 1
                    continue

                # 2. Upload para MinIO
                upload_ok, file_bytes = upload_to_minio(
                    file_path,
                    table_name,
                    rows,
                    as_parquet=as_parquet,
                    minio_prefix=minio_prefix,
                )

                if upload_ok:
                    success_count += 1
                    total_rows_all += rows
                    total_bytes_all += file_bytes
                    ext = "parquet" if as_parquet else "csv"
                    object_name = f"{minio_prefix}/{table_name}.{ext}"
                    write_export_log(
                        log_path=log_path,
                        source_table=table_name,
                        destination_path=object_name,
                        rows=rows,
                        bytes_=file_bytes,
                        format_=fmt,
                        status="success",
                    )
                else:
                    logger.warning(
                        "\u2717 Tabela %s: processada, mas upload falhou.",
                        table_name,
                    )
                    fail_count += 1
                    write_export_log(
                        log_path=log_path,
                        source_table=table_name,
                        destination_path="",
                        rows=rows,
                        bytes_=0,
                        format_=fmt,
                        status="error",
                    )

            except Exception as exc:
                logger.error(
                    "Erro inesperado ao processar tabela %s: %s",
                    table_name,
                    exc,
                )
                fmt = "parquet" if as_parquet else "csv"
                fail_count += 1
                write_export_log(
                    log_path=log_path,
                    source_table=table_name,
                    destination_path="",
                    rows=0,
                    bytes_=0,
                    format_=fmt,
                    status="error",
                )

    elapsed = time.monotonic() - t0
    exibir_relatorio_final(
        total_tables=len(table_list),
        success_count=success_count,
        fail_count=fail_count,
        skipped_empty_count=skipped_empty_count,
        total_rows=total_rows_all,
        total_bytes=total_bytes_all,
        elapsed_seconds=elapsed,
    )


# ── Entry point ────────────────────────────────────────────────────────────


def main() -> None:
    """Testa conexões, valida argumentos e inicia a transferência."""
    parser = argparse.ArgumentParser(
        description=(
            "Exporta tabelas do PostgreSQL ou diretório local para o MinIO"
            " (CSV ou Parquet)."
        )
    )
    parser.add_argument(
        "--schema-name",
        default="dados_historicos",
        help="Nome do schema no PostgreSQL (default: dados_historicos)",
    )
    parser.add_argument(
        "--minio-prefix",
        default=None,
        help="Prefixo do caminho no MinIO (default: raw/dados_historicos para PG, staging/dados_historicos para --source-dir)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help="Lista de tabelas específicas para exportar",
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        default=False,
        help="Converte para Parquet em vez de CSV",
    )
    parser.add_argument(
        "--source-dir",
        type=str,
        default=None,
        help="Diretório local com CSVs tab-separated (alternativa ao PostgreSQL)",
    )
    parser.add_argument(
        "--log-path",
        default="data/minio_export_log.csv",
        help="Caminho do arquivo de log de exportação (default: data/minio_export_log.csv)",
    )
    args = parser.parse_args()

    # Resolver minio_prefix conforme o modo
    if args.source_dir:
        minio_prefix = args.minio_prefix or "staging/dados_historicos"
    else:
        minio_prefix = args.minio_prefix or "raw/dados_historicos"

    # Testar MinIO (comum a ambos os modos)
    try:
        minio_client.list_buckets()
        logger.info("Conexão MinIO OK.")
    except Exception as exc:
        logger.error("Falha na conexão MinIO: %s", exc)
        return

    if args.source_dir:
        # ── Modo diretório local ────────────────────────────────────────────
        if not os.path.isdir(args.source_dir):
            logger.error("Diretório não encontrado: %s", args.source_dir)
            sys.exit(1)

        transfer_local_csv_dir(
            source_dir=args.source_dir,
            minio_prefix=minio_prefix,
            tables=args.tables,
            as_parquet=args.parquet,
            log_path=args.log_path,
        )
    else:
        # ── Modo PostgreSQL ─────────────────────────────────────────────────
        try:
            engine = _criar_conexao_pg()

            # Testar PostgreSQL
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                logger.info("Conexão PostgreSQL OK (resultado: %s).", result)

            # Validar --tables se fornecido
            if args.tables:
                inspector = inspect(engine)
                existing = set(inspector.get_table_names(schema=args.schema_name))
                invalid = [t for t in args.tables if t not in existing]
                if invalid:
                    logger.error(
                        "Tabela(s) não encontrada(s) no schema '%s': %s",
                        args.schema_name,
                        ", ".join(invalid),
                    )
                    sys.exit(1)

            transfer_schema_tables(
                schema_name=args.schema_name,
                minio_prefix=minio_prefix,
                tables=args.tables,
                as_parquet=args.parquet,
                log_path=args.log_path,
                engine=engine,
            )

        except Exception as exc:
            logger.error("Erro na execução principal: %s", exc)
            raise


if __name__ == "__main__":
    main()
