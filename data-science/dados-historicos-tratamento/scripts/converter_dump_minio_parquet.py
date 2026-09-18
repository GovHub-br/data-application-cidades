#!/usr/bin/env python
"""
Converte o dump tratado de ``staging/dados_historicos/`` (CSV ``;`` UTF-8)
para Parquet (Snappy) no MinIO, reconstruindo os nomes canônicos dos objetos
a partir do conteúdo (coluna ``source_table``).

Contexto
--------
Os objetos CSV em ``staging/dados_historicos/`` foram estagiados com **nomes de
objeto corrompidos** (mojibake ``previs_o`` → ``previsão`` e truncamento de
prefixo ``aixa_`` → ``caixa_``). Cada CSV tratado embute o nome canônico na
coluna ``source_table`` (primeira linha não vazia); quando a coluna não existe
no arquivo, o nome do próprio objeto (sem ``.csv``) é usado como canônico.

Abordagem (design D2 do change ``reloginho-dados-historicos``)
---------------------------------------------------------------
pandas (``dtype=str`` preservando o conteúdo tratado) + pyarrow, um objeto por
vez, com temp dir local:

    download CSV → pandas read (sep=';', dtype=str) → extract ``source_table``
    → write Parquet Snappy → read-back row count (conferência) → upload MinIO
    ``staging/dados_historicos/<source_table>.parquet`` → stat verification
    → remove CSV de origem → log local.

Idempotência / resumibilidade
-----------------------------
- Um log CSV local (``data/minio_conversao_parquet_log.csv``) registra cada
  tentativa (``source_key`` → ``source_table`` → ``parquet_name`` → linhas →
  status). Objetos já registrados como ``success`` são pulados na re-execução.
- O CSV de origem só é removido **após** a gravação íntegra e o upload verificado.
  Se o processo cair no meio, o objeto continua no MinIO e é reprocessado.

Robustez
--------
- Cliente MinIO com ``urllib3.PoolManager`` (timeouts de connect/read + retries
  automáticos) para tolerar a rede lenta/flaky do MinIO.
- Retry explícito por operação (download/upload/remove) com backoff.

Requires ``.env`` na raiz do repositório com ``MINIO_ENDPOINT``,
``MINIO_ACCESS_KEY``, ``MINIO_SECRET_KEY``, ``MINIO_BUCKET``, ``MINIO_SECURE``.

Uso
---
.. code-block:: bash

    uv run python scripts/converter_dump_minio_parquet.py
    uv run python scripts/converter_dump_minio_parquet.py --log-path data/meu_log.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import tempfile
import threading
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from pyarrow import parquet as pq

# ── Constantes ───────────────────────────────────────────────────────────────

META_FILES: frozenset[str] = frozenset(
    {"_classificacao.csv", "_dedup_map.csv", "_qualidade.csv"}
)
MINIO_PREFIX: str = "staging/dados_historicos"
DEFAULT_LOG: str = "data/minio_conversao_parquet_log.csv"
MAX_RETRIES: int = 5
RETRY_BASE_SLEEP: float = 2.0

# ── Configuração a partir do .env (raiz do repo) ─────────────────────────────

REPO_ROOT = Path(__file__).resolve().parents[3]
ROOT_ENV = REPO_ROOT / ".env"
SUB_ENV = Path(__file__).resolve().parents[1] / ".env"

if ROOT_ENV.exists():
    load_dotenv(ROOT_ENV)
elif SUB_ENV.exists():
    load_dotenv(SUB_ENV)
else:
    load_dotenv()  # tenta o cwd

BUCKET: str = os.getenv("MINIO_BUCKET", "data-lake-mcid")
ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "10.0.0.56:9000")
ACCESS_KEY: str | None = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY: str | None = os.getenv("MINIO_SECRET_KEY")
SECURE: bool = os.getenv("MINIO_SECURE", "False").lower() == "true"

# ── Cliente MinIO com timeout/retry ─────────────────────────────────────────

_HTTP_POOL = urllib3.PoolManager(
    timeout=urllib3.util.Timeout(connect=15, read=180),
    retries=urllib3.util.Retry(
        total=3,
        backoff_factor=0.7,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "PUT", "DELETE", "POST"}),
    ),
)


def make_client() -> Minio:
    """Cria cliente MinIO validando credenciais mínimas."""
    if not ACCESS_KEY or not SECRET_KEY:
        raise RuntimeError(
            "MINIO_ACCESS_KEY/MINIO_SECRET_KEY ausentes. Configure o .env da "
            "raiz do repositório."
        )
    return Minio(
        ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=SECURE,
        http_client=_HTTP_POOL,
    )


def with_retry(fn, *, attempts: int = MAX_RETRIES, label: str = "op"):
    """Executa *fn* com retries e backoff exponencial."""
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < attempts - 1:
                sleep = RETRY_BASE_SLEEP * (2**attempt)
                print(
                    f"    ! {label}: tentativa {attempt + 1}/{attempts} falhou "
                    f"({type(exc).__name__}: {exc}); retry em {sleep:.0f}s",
                    flush=True,
                )
                time.sleep(sleep)
    assert last_exc is not None
    raise last_exc


# ── Log local de conversão ───────────────────────────────────────────────────


def append_log(log_path: Path, row: dict[str, str]) -> None:
    """Adiciona uma linha ao log CSV (cria header se necessário)."""
    write_header = not log_path.exists()
    with log_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "source_key",
                "source_table",
                "parquet_name",
                "rows",
                "status",
                "error",
                "written_at",
            ],
        )
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def load_successful_keys(log_path: Path) -> set[str]:
    """Lê o log e devolve os ``source_key`` já convertidos com sucesso."""
    if not log_path.exists():
        return set()
    done: set[str] = set()
    with log_path.open(newline="", encoding="utf-8") as f:
        for rec in csv.DictReader(f):
            if rec.get("status") == "success" and rec.get("source_key"):
                done.add(rec["source_key"])
    return done


# ── Extração do nome canônico ────────────────────────────────────────────────

DUCKDB_THRESHOLD = 20 * 1024 * 1024  # arquivos acima deste tamanho → duckdb


def _extract_source_table_duckdb(con, csv_path: Path) -> str | None:
    """Extrai ``source_table`` via duckdb (primeiro valor não vazio)."""
    try:
        cols = [
            d[0]
            for d in con.execute(
                f"SELECT * FROM read_csv('{csv_path}', delim=';', header=true, "
                f"all_varchar=true, sample_size=-1) LIMIT 1"
            ).description
        ]
    except Exception:  # noqa: BLE001
        return None
    if "source_table" not in cols:
        return None
    row = con.execute(
        f"SELECT source_table FROM read_csv('{csv_path}', delim=';', "
        f"header=true, all_varchar=true, sample_size=-1) "
        f"WHERE source_table IS NOT NULL AND TRIM(source_table) != '' LIMIT 1"
    ).fetchone()
    return row[0].strip() if row else None


def download_chunked(
    client: Minio, bucket: str, key: str, dest: Path, chunk_size: int = 8 << 20
) -> int:
    """Baixa um objeto com GETs em faixas (range), com retry por chunk.

    A rede para o MinIO é flaky/slow em downloads grandes (conexão cai no
    meio ou o fluxo engasga). Estratégia:
    - baixar em faixas de *chunk_size* bytes, cada faixa uma requisição
      independente (resiliente a quedas de conexão);
    - dentro da faixa, gravar via ``stream()`` de forma incremental (o
      progresso não fica preso em memória e a escrita avança mesmo em
      fluxo lento);
    - em caso de falha da faixa, retentar apenas a faixa (``f.seek`` para
      a posição inicial antes de regravar).
    """
    stat = client.stat_object(bucket, key)
    total = stat.size
    with dest.open("wb") as f:
        offset = 0
        while offset < total:
            length = min(chunk_size, total - offset)

            def _fetch(_off: int = offset, _len: int = length) -> int:
                resp = client.get_object(bucket, key, offset=_off, length=_len)
                n = 0
                try:
                    f.seek(_off)
                    for data in resp.stream(amt=1 << 20):
                        f.write(data)
                        n += len(data)
                finally:
                    resp.close()
                    resp.release_conn()
                if n != _len:
                    raise IOError(f"chunk curto: {n} bytes lidos, esperado {_len}")
                return n

            with_retry(_fetch, label=f"chunk@{offset}")
            offset += length
    return total


def extract_source_table(df: pd.DataFrame, key_name: str) -> str | None:
    """Extrai o nome canônico da coluna ``source_table`` (primeiro valor).

    Fallback: se a coluna não existir, usa o nome do objeto (sem ``.csv``).
    """
    if "source_table" in df.columns:
        series = df["source_table"].fillna("").astype(str).str.strip()
        for value in series:
            if value:
                return value
    return None


def convert_to_parquet(
    csv_local: Path, parquet_local: Path, key_name: str, size: int
) -> tuple[str, int]:
    """Converte CSV → Parquet (Snappy) e devolve ``(source_table, n_rows)``.

    Arquivos grandes (> DUCKDB_THRESHOLD) usam DuckDB (streaming, memória
    eficiente); os demais usam pandas com ``dtype=str`` preservando o conteúdo
    tratado. Quando a coluna ``source_table`` não existe, usa o nome do objeto
    (sem ``.csv``) como nome canônico.
    """
    fallback_name = key_name[:-4] if key_name.endswith(".csv") else key_name

    if size >= DUCKDB_THRESHOLD:
        import duckdb

        con = duckdb.connect()
        try:
            source_table = _extract_source_table_duckdb(con, csv_local)
            if not source_table:
                source_table = fallback_name
                print(
                    f"    ! sem coluna source_table; usando nome do objeto: "
                    f"'{source_table}'",
                    flush=True,
                )
            con.execute(
                f"COPY (SELECT * FROM read_csv('{csv_local}', delim=';', "
                f"header=true, all_varchar=true, sample_size=-1)) "
                f"TO '{parquet_local}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
            )
            rows_csv = int(
                con.execute(
                    f"SELECT count(*) FROM read_parquet('{parquet_local}')"
                ).fetchone()[0]
            )
        finally:
            con.close()
    else:
        df = pd.read_csv(csv_local, sep=";", dtype=str)
        rows_csv = len(df)
        source_table = extract_source_table(df, key_name)
        if not source_table:
            # Fallback: nome do objeto (algumas tabelas tratadas não embutem
            # as colunas de metadados).
            source_table = fallback_name
            print(
                f"    ! sem coluna source_table; usando nome do objeto: '{source_table}'",
                flush=True,
            )
        df.to_parquet(parquet_local, engine="pyarrow", compression="snappy", index=False)

    source_table = source_table.strip()
    if "/" in source_table or "\x00" in source_table:
        raise ValueError(f"source_table inválido para objeto: {key_name!r}")
    return source_table, rows_csv


# ── Conversão de um objeto ───────────────────────────────────────────────────


def process_object(
    client: Minio,
    bucket: str,
    key: str,
    work_dir: Path,
    log_path: Path,
    log_lock: threading.Lock,
    index: int,
) -> str:
    """Converte um objeto CSV para Parquet e remove o CSV de origem.

    Returns
    -------
    str
        ``success``, ``skipped`` (não é CSV de dados) ou ``error``.
    """
    key_name = key.split("/")[-1]
    if key_name in META_FILES:
        return "skipped"
    if not key_name.endswith(".csv"):
        print(f"[{index}] {key_name}: não é CSV de dados — pulando", flush=True)
        return "skipped"

    csv_local = work_dir / f"{index}.csv"
    parquet_local = work_dir / f"{index}.parquet"
    print(f"[{index}] {key_name}", flush=True)

    # 1. Download do CSV (chunked com retry por chunk — rede flaky)
    def _download() -> int:
        return download_chunked(client, bucket, key, csv_local)

    try:
        size = with_retry(_download, label="download")
        print(f"    download OK ({size:,} bytes)", flush=True)

        # 2. Conversão CSV → Parquet (Snappy) + nome canônico
        source_table, rows_csv = convert_to_parquet(
            csv_local, parquet_local, key_name, size
        )
        parquet_name = f"{source_table}.parquet"

        # 3. Conferência de row count (parquet × csv)
        rows_parquet = pq.read_metadata(parquet_local).num_rows
        print(
            f"    parquet {parquet_name}: {rows_csv:,} linhas "
            f"(read-back {rows_parquet:,})",
            flush=True,
        )
        if rows_csv != rows_parquet:
            raise RuntimeError(
                f"row count diverge: csv={rows_csv} parquet={rows_parquet}"
            )

        # 5. Upload do Parquet
        object_name = f"{MINIO_PREFIX}/{parquet_name}"

        def _upload() -> int:
            file_size = parquet_local.stat().st_size
            with parquet_local.open("rb") as f:
                client.put_object(
                    bucket_name=bucket,
                    object_name=object_name,
                    data=f,
                    length=file_size,
                    content_type="application/octet-stream",
                )
            return file_size

        up_size = with_retry(_upload, attempts=8, label="upload")
        stat = client.stat_object(bucket, object_name)
        if stat.size != up_size:
            raise RuntimeError(
                f"tamanho divergente após upload: esperado {up_size}, stat {stat.size}"
            )

        # 6. Remove o CSV de origem (somente após gravação íntegra + upload OK)
        with_retry(
            lambda: client.remove_object(bucket, key),
            attempts=3,
            label="remove_csv",
        )

        with log_lock:
            append_log(
                log_path,
                {
                    "source_key": key_name,
                    "source_table": source_table,
                    "parquet_name": parquet_name,
                    "rows": str(rows_csv),
                    "status": "success",
                    "error": "",
                    "written_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        print(f"    OK → {object_name} ({rows_csv:,} linhas)", flush=True)
        return "success"

    except Exception as exc:  # noqa: BLE001
        print(f"    ERRO: {type(exc).__name__}: {exc}", flush=True)
        with log_lock:
            append_log(
                log_path,
                {
                    "source_key": key_name,
                    "source_table": "",
                    "parquet_name": "",
                    "rows": "0",
                    "status": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "written_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        return "error"
    finally:
        for p in (csv_local, parquet_local):
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass


# ── Pipeline principal ───────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Converte CSVs de staging/dados_historicos (MinIO) para "
        "Parquet Snappy com nomes canônicos a partir de source_table."
    )
    parser.add_argument(
        "--log-path",
        default=DEFAULT_LOG,
        help=f"Caminho do log CSV local (default: {DEFAULT_LOG})",
    )
    parser.add_argument(
        "--prefix",
        default=MINIO_PREFIX,
        help="Prefixo dos objetos no bucket (default: staging/dados_historicos)",
    )
    parser.add_argument(
        "--only",
        nargs="*",
        default=None,
        help="Processar apenas estes nomes de objeto (sem extensão).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Processar no máximo N objetos de dados (ordem de listagem).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Número de workers paralelos (default: 4)",
    )
    args = parser.parse_args()

    log_path = Path(args.log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    client = make_client()
    if not client.bucket_exists(BUCKET):
        print(f"Bucket {BUCKET!r} não existe no MinIO.", file=sys.stderr)
        sys.exit(1)

    successful = load_successful_keys(log_path)
    print(
        f"Log {log_path}: {len(successful)} objetos já convertidos (success).",
        flush=True,
    )

    objs = [
        o
        for o in client.list_objects(BUCKET, prefix=f"{args.prefix}/", recursive=True)
        if o.object_name.split("/")[-1] not in META_FILES
        and o.object_name.split("/")[-1].endswith(".csv")
    ]
    if args.only:
        only_set = {n if n.endswith(".csv") else f"{n}.csv" for n in args.only}
        objs = [o for o in objs if o.object_name.split("/")[-1] in only_set]
    if args.limit is not None:
        objs = objs[: args.limit]
    print(f"Objetos de dados no prefixo {args.prefix}/: {len(objs)}", flush=True)

    counts = {"success": 0, "error": 0, "skipped": 0, "already_done": 0}
    t0 = time.monotonic()
    log_lock = threading.Lock()

    def run_one(obj, index: int) -> str:
        key = obj.object_name
        key_name = key.split("/")[-1]
        if key_name in successful:
            print(f"[{index}] {key_name}: já convertido — pulando", flush=True)
            return "already_done"
        worker_client = make_client()
        return process_object(
            worker_client, BUCKET, key, work_dir, log_path, log_lock, index
        )

    with tempfile.TemporaryDirectory(prefix="conv_minio_") as tmp:
        work_dir = Path(tmp)
        pending = [
            (obj, i)
            for i, obj in enumerate(objs, 1)
            if obj.object_name.split("/")[-1] not in successful
        ]
        print(
            f"A processar {len(pending)} objetos com {args.workers} workers...",
            flush=True,
        )
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = [pool.submit(run_one, obj, i) for obj, i in pending]
            for fut in futures:
                status = fut.result()
                counts[status] += 1
        counts["already_done"] += len(objs) - len(pending)

    elapsed = time.monotonic() - t0
    print("=" * 70, flush=True)
    print("RESUMO DA CONVERSÃO", flush=True)
    print(f"  total objetos de dados : {len(objs)}", flush=True)
    print(f"  convertidos (success)  : {counts['success']}", flush=True)
    print(f"  já convertidos (skip)  : {counts['already_done']}", flush=True)
    print(f"  erros                  : {counts['error']}", flush=True)
    print(f"  tempo total            : {elapsed / 60:.1f} min", flush=True)
    print(f"  log                    : {log_path}", flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
