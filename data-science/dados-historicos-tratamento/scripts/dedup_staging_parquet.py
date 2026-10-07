"""
Script standalone para deduplicar in-place os arquivos parquet de
``staging/dados_historicos/`` no bucket ``data-lake-mcid`` (MinIO).

Contexto
--------
A auditoria de 2026-09-05 (ver ``.informacoes/2026-09-05-estimativa-volume-staging-bronze-silver.md``
e a change OpenSpec ``deduplicar-staging-dados-historicos``) confirmou que
praticamente todos os arquivos de ``staging/dados_historicos/`` têm ~50% de
linhas em duplicata exata (byte-idênticas nas colunas de negócio), causada
provavelmente pela paginação ``ORDER BY ctid OFFSET/LIMIT`` usada pelos
scripts de exportação originais (``exportar_tabelas_minio.py`` /
``popula_minIO_dados_historicos_raw.py``).

Este script corrige o *dado já existente* em staging (não a exportação):
lê cada arquivo, remove duplicata exata por hash de conteúdo de negócio
(colunas técnicas ``content_hash``/``report_date``/``source_table``/
``sub_table_index`` excluídas do hash — mudam por definição a cada
extração), grava local, valida a contagem contra o número de conteúdos
distintos, e só então sobrescreve o objeto em MinIO. Reler o objeto após o
upload confirma que a contagem final bate com o esperado.

Sem backup
----------
Esta correção **não copia os arquivos originais para um prefixo de
backup**. Já existe uma cópia dos dados no schema Postgres ``prod``
``dados_historicos_formatados`` (indisponível enquanto o Postgres ``prod``
não voltar do incidente relatado em
``.informacoes/2026-09-05-incidente-postgres-prod-piloto-pg-duckdb.md``).
Se a correção introduzir um erro, a recuperação depende desse schema voltar
a ficar acessível — não há restauração local disponível até lá.

Uso
---
.. code-block:: bash

    # Medir sem gravar nada (dry-run)
    uv run python scripts/dedup_staging_parquet.py --files entrada_bb --dry-run

    # Aplicar a um ou mais arquivos específicos
    uv run python scripts/dedup_staging_parquet.py --files bb_2012_10_outubro_entrada_bb_20121023.parquet

    # Aplicar a todos os arquivos de uma família (regex sobre o nome)
    uv run python scripts/dedup_staging_parquet.py --family-regex "entrada_bb"

Requer ``.env`` com as variáveis MinIO configuradas.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import tempfile
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from minio import Minio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY: str | None = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY: str | None = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "data-lake-mcid")
MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "False").lower() == "true"

STAGING_PREFIX = "staging/dados_historicos/"
TECH_COLS = {"content_hash", "report_date", "source_table", "sub_table_index"}
NULL_MARKER = "␀NULL␀"


def minio_client() -> Minio:
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError("MINIO_ACCESS_KEY/MINIO_SECRET_KEY (ou MINIO_ROOT_USER/MINIO_ROOT_PASSWORD) não configurados no .env")
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def duckdb_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}'")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}'")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}'")
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_use_ssl={'true' if MINIO_SECURE else 'false'}")
    return con


def business_columns(con: duckdb.DuckDBPyConnection, s3_path: str) -> list[str]:
    cols = [r[0] for r in con.execute(f"describe select * from read_parquet('{s3_path}')").fetchall()]
    biz = [c for c in cols if c not in TECH_COLS]
    if not biz:
        raise ValueError(f"{s3_path}: nenhuma coluna de negócio encontrada (todas técnicas?)")
    return biz


def hash_expr(biz_cols: list[str]) -> str:
    parts = ", ".join(f"coalesce(cast(\"{c}\" as varchar), '{NULL_MARKER}')" for c in biz_cols)
    return f"md5(concat_ws('|', {parts}))"


def dedup_file(con: duckdb.DuckDBPyConnection, filename: str, local_out: Path) -> dict:
    """Lê o arquivo remoto, deduplica, grava local. Retorna métricas."""
    s3_path = f"s3://{MINIO_BUCKET}/{STAGING_PREFIX}{filename}"
    biz_cols = business_columns(con, s3_path)
    hexpr = hash_expr(biz_cols)

    total_rows = con.execute(f"select count(*) from read_parquet('{s3_path}')").fetchone()[0]
    distinct_rows = con.execute(
        f"select count(distinct {hexpr}) from read_parquet('{s3_path}')"
    ).fetchone()[0]

    query = f"""
    copy (
        with base as (
            select *, row_number() over () as __rid
            from read_parquet('{s3_path}')
        ), hashed as (
            select *, {hexpr} as __h
            from base
        ), ranked as (
            select * exclude (__h), row_number() over (partition by __h order by __rid) as __rn
            from hashed
        )
        select * exclude (__rid, __rn)
        from ranked
        where __rn = 1
    ) to '{local_out.as_posix()}' (format parquet)
    """
    con.execute(query)

    written_rows = con.execute(f"select count(*) from read_parquet('{local_out.as_posix()}')").fetchone()[0]

    return {
        "filename": filename,
        "biz_cols": biz_cols,
        "total_rows": total_rows,
        "distinct_rows": distinct_rows,
        "written_rows": written_rows,
        "excess_removed": total_rows - written_rows,
    }


def upload_and_verify(con: duckdb.DuckDBPyConnection, client: Minio, filename: str, local_out: Path, expected_rows: int) -> None:
    object_name = f"{STAGING_PREFIX}{filename}"
    client.fput_object(MINIO_BUCKET, object_name, str(local_out))
    s3_path = f"s3://{MINIO_BUCKET}/{object_name}"
    reread_rows = con.execute(f"select count(*) from read_parquet('{s3_path}')").fetchone()[0]
    if reread_rows != expected_rows:
        raise RuntimeError(
            f"{filename}: verificação pós-upload falhou — esperado {expected_rows} linhas, "
            f"lido {reread_rows} do objeto em MinIO"
        )
    logger.info(f"  {filename}: upload OK, {reread_rows} linhas confirmadas em MinIO")


def list_staging_files(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        f"select file from glob('s3://{MINIO_BUCKET}/{STAGING_PREFIX}*.parquet')"
    ).fetchall()
    return [r[0].rsplit("/", 1)[-1] for r in rows]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--files", nargs="*", help="Nomes de arquivo específicos (sem prefixo) em staging/dados_historicos/")
    parser.add_argument("--files-from", help="Arquivo texto com um nome de arquivo por linha (evita problemas de word-splitting do shell com nomes contendo espaços/acentos)")
    parser.add_argument("--family-regex", help="Regex (case-insensitive) sobre o nome do arquivo para selecionar a família")
    parser.add_argument("--dry-run", action="store_true", help="Só mede e valida localmente, não sobrescreve nada em MinIO")
    args = parser.parse_args()

    if not args.files and not args.files_from and not args.family_regex:
        parser.error("forneça --files, --files-from ou --family-regex")

    con = duckdb_conn()

    if args.family_regex:
        rx = re.compile(args.family_regex, re.I)
        all_files = list_staging_files(con)
        targets = [f for f in all_files if rx.search(f)]
    elif args.files_from:
        targets = [line.strip() for line in Path(args.files_from).read_text(encoding="utf-8").splitlines() if line.strip()]
    else:
        targets = args.files

    if not targets:
        logger.error("Nenhum arquivo encontrado para o filtro fornecido.")
        return 1

    logger.info(f"{len(targets)} arquivo(s) selecionado(s) para deduplicação{' (dry-run)' if args.dry_run else ''}.")

    client = None if args.dry_run else minio_client()

    ok, failed = 0, 0
    with tempfile.TemporaryDirectory(prefix="dedup_staging_") as tmpdir:
        for filename in targets:
            local_out = Path(tmpdir) / filename
            try:
                metrics = dedup_file(con, filename, local_out)
            except Exception as exc:
                logger.error(f"  {filename}: ERRO ao deduplicar — {exc}")
                failed += 1
                continue

            if metrics["written_rows"] != metrics["distinct_rows"]:
                logger.error(
                    f"  {filename}: INCONSISTENTE — distinct_rows medido "
                    f"({metrics['distinct_rows']}) != linhas escritas ({metrics['written_rows']}); "
                    "pulando upload."
                )
                failed += 1
                continue

            pct = round(100.0 * metrics["excess_removed"] / metrics["total_rows"], 2) if metrics["total_rows"] else 0.0
            logger.info(
                f"  {filename}: total={metrics['total_rows']} -> {metrics['written_rows']} "
                f"(removido {metrics['excess_removed']}, {pct}%)"
            )

            if args.dry_run:
                ok += 1
                continue

            try:
                upload_and_verify(con, client, filename, local_out, metrics["written_rows"])
                ok += 1
            except Exception as exc:
                logger.error(f"  {filename}: ERRO no upload/verificação — {exc}")
                failed += 1

    logger.info(f"Concluído: {ok} OK, {failed} falha(s) de {len(targets)} arquivo(s).")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
