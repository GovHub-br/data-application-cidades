# scripts/raw_para_staging.py

"""
Conversão Raw -> Staging (Parquet) do data lake (MinIO).

Percorre os objetos de raw/ (CSV/TXT/XLSX), detecta separador e encoding de cada arquivo,
e grava a versão colunar em Parquet em staging/, para a etapa seguinte (Staging->DB via
dbt/DuckDB) consumir dados já em formato eficiente e uniforme.

Decisões do projeto:
  - Todas as colunas como STRING — a tipagem (datas/números) fica para o dbt/DuckDB.
  - pandas (chunked) + pyarrow ParquetWriter: streaming memory-bounded, suporta qualquer
    encoding Python (cp1250/cp1252/utf-8).
  - Nomes de coluna normalizados para snake_case ASCII (dedup); header original guardado
    nos metadados do parquet e na tabela de controle.
  - Colunas de linhagem `_source_file`, `_ingested_at`, `_source_hash` em cada parquet.

IMPORTANTE (ordem no pipeline): rode DEPOIS do mascaramento (`mascarar_minio.py --apply`),
senão o parquet conterá PII (staging lê raw/ como está).

Idempotência: tabela de controle sftp._staging_log com UNIQUE(raw_key, source_hash); objetos
já convertidos (mesmo hash) são pulados. Use --force para reprocessar.
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
from psycopg2.extras import Json

from lake_utils import (
    baixar,
    criar_cliente_minio,
    detectar_dialeto,
    detectar_encoding,
    encoding_fallback,
    md5_arquivo,
    normalizar_colunas,
    norm_header,
    sample_bytes,
)

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
CONTROL_TABLE = "_staging_log"

RAW_PREFIX     = os.environ.get("MASKING_PREFIX", "raw/")
STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "staging/")
AUDIT_PREFIX   = "audit/staging/"

TMPDIR = os.environ.get("LAKE_TMPDIR") or os.environ.get("MASKING_TMPDIR") or None
if TMPDIR:
    os.makedirs(TMPDIR, exist_ok=True)

SUPPORTED_TABULAR = {".csv", ".txt"}
SUPPORTED_EXCEL   = {".xlsx"}
UNSUPPORTED       = {".xls", ".zip"}

LINEAGE_COLS = ["_source_file", "_ingested_at", "_source_hash"]

_LOG_FILE = (
    Path(__file__).parent
    / f"raw_para_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logging.root.setLevel(logging.INFO)
for _h in (logging.FileHandler(_LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stderr)):
    _h.setFormatter(_formatter)
    logging.root.addHandler(_h)
log = logging.getLogger(__name__)

_TRANSFER_CONFIG = TransferConfig(multipart_chunksize=8 * 1024 * 1024, max_concurrency=2)


# Infra: conexões / controle
def _conn_str() -> str:
    return (
        f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )


def _cliente_minio():
    return criar_cliente_minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)


def _criar_control_table(conn_str: str) -> None:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
            cur.execute(f"""
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
                    UNIQUE (raw_key, source_hash)
                );
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_staging_log_status
                ON {SCHEMA}.{CONTROL_TABLE} (status);
            """)
            conn.commit()
    log.info("Tabela de controle %s.%s garantida.", SCHEMA, CONTROL_TABLE)


def _carregar_convertidos(conn_str: str) -> set:
    """(raw_key, source_hash) já convertidos e gravados em staging/ (status 'converted').

    Só o `--apply` (status 'converted') conta para idempotência; execuções dry-run
    (status 'dry_run') não bloqueiam reprocessamento.
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT raw_key, source_hash FROM {SCHEMA}.{CONTROL_TABLE}
                WHERE status = 'converted' AND source_hash IS NOT NULL
            """)
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
                ON CONFLICT (raw_key, source_hash) DO UPDATE SET
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
                    rec["execution_id"], rec["raw_key"], rec["staging_key"], rec["source_hash"],
                    rec["n_linhas"], rec["n_colunas"], rec["n_bad_lines"], rec["encoding"],
                    rec["delimiter"], Json(rec["column_map"]), rec["status"], rec["error_message"],
                ),
            )
            conn.commit()


# S3 helpers
def _listar_objetos(s3, prefix: str):
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"]


def _subir(s3, path: str, key: str) -> None:
    with open(path, "rb") as f:
        s3.upload_fileobj(f, MINIO_BUCKET, key, Config=_TRANSFER_CONFIG)


# Conversão CSV/TXT -> Parquet (streaming, chunked)
def _staging_key(raw_key: str, sufixo_aba: str = "") -> str:
    """raw/<...>/<nome>.<ext> -> staging/<...>/<nome>[__aba].parquet"""
    rel = raw_key[len(RAW_PREFIX):] if raw_key.startswith(RAW_PREFIX) else raw_key
    stem = rel.rsplit(".", 1)[0] if "." in rel.rsplit("/", 1)[-1] else rel
    return f"{STAGING_PREFIX}{stem}{sufixo_aba}.parquet"


class _BadLineCounter:
    """Conta linhas ruins descartadas pelo pandas (on_bad_lines=callable)."""

    def __init__(self):
        self.n = 0

    def __call__(self, bad_line):  # noqa: ANN001
        self.n += 1
        return None  # descarta a linha


def _converter_tabular(
    src_path: str, dst_path: str, delim: str, real_encoding: str,
    source_file: str, source_hash: str, ingested_at: str,
    chunksize: int, bad_mode: str,
) -> Tuple[int, int, int, dict, str]:
    """Retorna (n_linhas, n_colunas, n_bad_lines, column_map, encoding_usado).

    encoding_usado pode diferir de real_encoding: a detecção olha só o sample de 64 KB, e um
    byte que o encoding não aceita pode aparecer depois. Nesse caso cai para latin-1 (que
    mapeia os 256 bytes) e reinicia a conversão — ver `encoding_fallback` em lake_utils.

    bad_mode:
      - 'skip'  (default): engine C (rápido) descarta linhas mal-formadas; n_bad não é contado (-1).
      - 'count': engine Python (mais lento) descarta E conta as linhas mal-formadas.
      - 'error': falha o arquivo na 1ª linha mal-formada.
    """
    encoding = real_encoding
    while True:
        try:
            resultado = _converter_tabular_1x(
                src_path, dst_path, delim, encoding, source_file, source_hash,
                ingested_at, chunksize, bad_mode,
            )
            return (*resultado, encoding)
        except UnicodeDecodeError as e:
            proximo = encoding_fallback(encoding)
            if proximo is None:
                raise
            log.warning(
                "%s: %s não decodifica o arquivo inteiro (%s) — refazendo com %s.",
                source_file, encoding, e, proximo,
            )
            encoding = proximo


def _converter_tabular_1x(
    src_path: str, dst_path: str, delim: str, real_encoding: str,
    source_file: str, source_hash: str, ingested_at: str,
    chunksize: int, bad_mode: str,
) -> Tuple[int, int, int, dict]:
    """Uma passada de conversão com um encoding fixo. Retorna (n_linhas, n_colunas, n_bad, map)."""
    bad_counter = _BadLineCounter()
    if bad_mode == "count":
        engine, on_bad = "python", bad_counter
    elif bad_mode == "error":
        engine, on_bad = "c", "error"
    else:  # skip
        engine, on_bad = "c", "skip"

    reader = pd.read_csv(
        src_path,
        sep=delim,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        encoding=real_encoding,
        quotechar='"',
        chunksize=chunksize,
        on_bad_lines=on_bad,
        engine=engine,
    )

    writer: Optional[pq.ParquetWriter] = None
    schema: Optional[pa.Schema] = None
    nomes_finais: List[str] = []
    column_map: dict = {}
    n_linhas = 0
    n_colunas = 0

    try:
        for chunk in reader:
            if writer is None:
                header_original = list(chunk.columns)
                nomes_finais, column_map = normalizar_colunas([str(c) for c in header_original])
                n_colunas = len(nomes_finais)
                # todas as colunas string + colunas de linhagem
                campos = [pa.field(n, pa.string()) for n in nomes_finais]
                campos += [pa.field(c, pa.string()) for c in LINEAGE_COLS]
                meta = {
                    b"source_file": source_file.encode("utf-8"),
                    b"source_hash": source_hash.encode("utf-8"),
                    b"encoding": real_encoding.encode("utf-8"),
                    b"delimiter": delim.encode("utf-8"),
                    b"column_map": json.dumps(column_map, ensure_ascii=False).encode("utf-8"),
                }
                schema = pa.schema(campos, metadata=meta)
                writer = pq.ParquetWriter(dst_path, schema, compression="snappy")

            chunk.columns = nomes_finais
            chunk["_source_file"] = source_file
            chunk["_ingested_at"] = ingested_at
            chunk["_source_hash"] = source_hash
            table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
            writer.write_table(table)
            n_linhas += len(chunk)
    finally:
        if writer is not None:
            writer.close()

    # em modo 'skip' as linhas ruins não são contadas → -1 sinaliza "não contado"
    n_bad = bad_counter.n if bad_mode == "count" else -1
    return n_linhas, n_colunas, n_bad, column_map


# Conversão XLSX -> Parquet (uma aba por parquet)
def _abas_xlsx(src_path: str) -> List[str]:
    import openpyxl

    wb = openpyxl.load_workbook(src_path, read_only=True)
    try:
        return list(wb.sheetnames)
    finally:
        wb.close()


def _converter_xlsx_aba(
    src_path: str, aba: str, dst_path: str,
    source_file: str, source_hash: str, ingested_at: str,
) -> Tuple[int, int, dict]:
    """Converte uma aba para parquet. Retorna (n_linhas, n_colunas, column_map)."""
    df = pd.read_excel(src_path, sheet_name=aba, dtype=str, header=0, engine="openpyxl")
    df = df.fillna("")
    header_original = [str(c) for c in df.columns]
    nomes_finais, column_map = normalizar_colunas(header_original)
    df.columns = nomes_finais
    df["_source_file"] = source_file
    df["_ingested_at"] = ingested_at
    df["_source_hash"] = source_hash

    campos = [pa.field(n, pa.string()) for n in nomes_finais]
    campos += [pa.field(c, pa.string()) for c in LINEAGE_COLS]
    meta = {
        b"source_file": source_file.encode("utf-8"),
        b"source_hash": source_hash.encode("utf-8"),
        b"sheet": aba.encode("utf-8"),
        b"column_map": json.dumps(column_map, ensure_ascii=False).encode("utf-8"),
    }
    schema = pa.schema(campos, metadata=meta)
    table = pa.Table.from_pandas(df.astype(str), schema=schema, preserve_index=False)
    pq.write_table(table, dst_path, compression="snappy")
    return len(df), len(nomes_finais), column_map


# Verificação
def _verificar_parquet(dst_path: str, n_esperado: int) -> None:
    pf = pq.ParquetFile(dst_path)
    n = pf.metadata.num_rows
    if n != n_esperado:
        raise ValueError(f"parquet: nº de linhas divergem ({n} != {n_esperado})")


# Processamento de um objeto
def _novo_registro(execution_id: str, raw_key: str) -> dict:
    return {
        "execution_id": execution_id,
        "raw_key": raw_key,
        "staging_key": None,
        "source_hash": None,
        "n_linhas": 0,
        "n_colunas": 0,
        "n_bad_lines": 0,
        "encoding": None,
        "delimiter": None,
        "column_map": {},
        "status": None,
        "error_message": None,
    }


def processar_objeto(
    s3, key: str, execution_id: str, apply: bool, convertidos: set,
    chunksize: int, bad_mode: str,
) -> List[dict]:
    """Retorna lista de registros (XLSX pode gerar 1 por aba)."""
    t0 = time.time()
    ext = os.path.splitext(key)[1].lower()

    if ext in UNSUPPORTED:
        rec = _novo_registro(execution_id, key)
        rec["status"] = "skipped_unsupported"
        return [rec]

    src = None
    try:
        sample = sample_bytes(s3, MINIO_BUCKET, key)
        real_encoding = detectar_encoding(sample)

        # CSV / TXT
        if ext in SUPPORTED_TABULAR:
            rec = _novo_registro(execution_id, key)
            rec["encoding"] = real_encoding
            dialeto = detectar_dialeto(sample, real_encoding)
            if dialeto is None:
                rec["status"] = "skipped_no_header"
                return [rec]
            delim, _lineterm, _fq = dialeto
            rec["delimiter"] = delim

            src = baixar(s3, MINIO_BUCKET, key, ext, TMPDIR)
            source_hash = md5_arquivo(src)
            rec["source_hash"] = source_hash
            if (key, source_hash) in convertidos:
                rec["status"] = "skipped_already"
                return [rec]

            staging_key = _staging_key(key)
            rec["staging_key"] = staging_key
            ingested_at = datetime.now(timezone.utc).isoformat()

            dst = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet", dir=TMPDIR).name
            try:
                n_linhas, n_colunas, n_bad, column_map, enc_usado = _converter_tabular(
                    src, dst, delim, real_encoding, os.path.basename(key),
                    source_hash, ingested_at, chunksize, bad_mode,
                )
                # o encoding registrado é o que de fato decodificou o arquivo (pode ter caído
                # para latin-1 no fallback), não o palpite feito sobre o sample
                rec.update(n_linhas=n_linhas, n_colunas=n_colunas, n_bad_lines=n_bad,
                           column_map=column_map, encoding=enc_usado)
                _verificar_parquet(dst, n_linhas)
                if apply:
                    _subir(s3, dst, staging_key)
                    rec["status"] = "converted"
                else:
                    _subir(s3, dst, _dryrun_key(staging_key))
                    rec["status"] = "dry_run"
                if n_bad > 0:
                    log.warning("%s — %d linha(s) mal-formada(s) descartada(s)", key, n_bad)
            finally:
                if os.path.exists(dst):
                    os.unlink(dst)
            return [rec]

        # XLSX (uma aba por parquet)
        if ext in SUPPORTED_EXCEL:
            src = baixar(s3, MINIO_BUCKET, key, ext, TMPDIR)
            source_hash = md5_arquivo(src)
            if (key, source_hash) in convertidos:
                rec = _novo_registro(execution_id, key)
                rec["source_hash"] = source_hash
                rec["status"] = "skipped_already"
                return [rec]

            ingested_at = datetime.now(timezone.utc).isoformat()
            abas = _abas_xlsx(src)
            multi = len(abas) > 1
            recs: List[dict] = []
            for aba in abas:
                rec = _novo_registro(execution_id, key)
                rec["source_hash"] = source_hash
                sufixo = f"__{norm_header(aba) or 'aba'}" if multi else ""
                staging_key = _staging_key(key, sufixo)
                rec["staging_key"] = staging_key
                dst = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet", dir=TMPDIR).name
                try:
                    n_linhas, n_colunas, column_map = _converter_xlsx_aba(
                        src, aba, dst, os.path.basename(key), source_hash, ingested_at,
                    )
                    if n_colunas == 0:
                        rec["status"] = "skipped_no_header"
                        recs.append(rec)
                        continue
                    rec.update(n_linhas=n_linhas, n_colunas=n_colunas, column_map=column_map)
                    _verificar_parquet(dst, n_linhas)
                    if apply:
                        _subir(s3, dst, staging_key)
                        rec["status"] = "converted"
                    else:
                        _subir(s3, dst, _dryrun_key(staging_key))
                        rec["status"] = "dry_run"
                finally:
                    if os.path.exists(dst):
                        os.unlink(dst)
                recs.append(rec)
            return recs

        rec = _novo_registro(execution_id, key)
        rec["status"] = "skipped_unsupported"
        return [rec]

    except Exception as e:  # noqa: BLE001
        rec = _novo_registro(execution_id, key)
        rec["status"] = "error"
        rec["error_message"] = str(e)[:500]
        log.error("✗ %s: %s", key, e)
        return [rec]
    finally:
        _ = round(time.time() - t0, 2)
        if src and os.path.exists(src):
            os.unlink(src)


def _dryrun_key(staging_key: str) -> str:
    return "staging_dryrun/" + staging_key[len(STAGING_PREFIX):]


# Auditoria (parquet)
def _gravar_auditoria(s3, execution_id: str, registros: List[dict]) -> None:
    df = pd.DataFrame(registros)
    if "column_map" in df.columns:
        df["column_map"] = df["column_map"].apply(lambda v: json.dumps(v, ensure_ascii=False))
    import io as _io

    buf = _io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    key = f"{AUDIT_PREFIX}execution_id={execution_id}/part-0.parquet"
    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buf.getvalue())
    local = Path(__file__).parent / f"auditoria_staging_{execution_id}.parquet"
    df.to_parquet(local, engine="pyarrow", index=False)
    log.info("Auditoria: s3://%s/%s (cópia local: %s)", MINIO_BUCKET, key, local)


# Main
def main() -> None:
    parser = argparse.ArgumentParser(description="Conversão Raw -> Staging (Parquet) no MinIO.")
    parser.add_argument("--apply", action="store_true",
                        help="Grava em staging/. Sem esta flag roda em dry-run (staging_dryrun/).")
    parser.add_argument("--force", action="store_true",
                        help="Reprocessa objetos já convertidos.")
    parser.add_argument("--limit", type=int, default=0, help="Processa no máximo N objetos.")
    parser.add_argument("--pattern", default="", help="Filtra por substring na key.")
    parser.add_argument("--only-ext", default="", help="Extensões a processar, ex.: csv,txt")
    parser.add_argument("--max-size-mb", type=int, default=0,
                        help="Pula objetos maiores que N MB (0 = sem limite).")
    parser.add_argument("--chunksize", type=int, default=200_000,
                        help="Linhas por chunk na leitura CSV/TXT (default 200k).")
    parser.add_argument("--bad-lines", choices=["skip", "count", "error"], default="skip",
                        help="Linhas mal-formadas: 'skip' descarta (rápido, engine C, sem contagem); "
                             "'count' descarta e conta (engine Python, mais lento); 'error' falha o arquivo.")
    parser.add_argument("--prefix", default=RAW_PREFIX, help="Prefixo a varrer (default raw/).")
    args = parser.parse_args()

    execution_id = uuid.uuid4().hex
    apply = args.apply
    only_ext = {("." + e.strip().lstrip(".")).lower() for e in args.only_ext.split(",") if e.strip()}

    log.info("=" * 70)
    log.info("Execução %s | modo=%s | prefixo=%s", execution_id,
             "APPLY (staging/)" if apply else "DRY-RUN (staging_dryrun/)", args.prefix)
    log.info("=" * 70)

    conn_str = _conn_str()
    _criar_control_table(conn_str)
    convertidos = set() if args.force else _carregar_convertidos(conn_str)

    s3 = _cliente_minio()

    registros: List[dict] = []
    contagem: Dict[str, int] = {}
    processados = 0

    for key, size in _listar_objetos(s3, args.prefix):
        if args.pattern and args.pattern not in key:
            continue
        ext = os.path.splitext(key)[1].lower()
        if only_ext and ext not in only_ext:
            continue
        if os.path.basename(key).startswith("~$"):
            continue
        if args.max_size_mb and size > args.max_size_mb * 1024 * 1024:
            continue
        if args.limit and processados >= args.limit:
            break
        processados += 1

        recs = processar_objeto(s3, key, execution_id, apply, convertidos,
                                args.chunksize, args.bad_lines)
        registros.extend(recs)
        for rec in recs:
            contagem[rec["status"]] = contagem.get(rec["status"], 0) + 1
            if rec["source_hash"] is not None and rec["status"] != "skipped_already":
                _registrar_control(conn_str, rec)

        r0 = recs[0]
        icone = {"converted": "✓", "dry_run": "◐", "error": "✗"}.get(r0["status"], "·")
        log.info(
            "%s [%d] %s — %s | abas/parts=%d | linhas=%d | cols=%d",
            icone, processados, key, r0["status"], len(recs),
            sum(r["n_linhas"] for r in recs), r0["n_colunas"],
        )

    if registros:
        _gravar_auditoria(s3, execution_id, registros)

    log.info("=" * 70)
    log.info("Concluído. Objetos: %d | parts geradas: %d", processados, len(registros))
    for status, n in sorted(contagem.items()):
        log.info("  %-20s %d", status, n)
    if not apply:
        log.info("DRY-RUN — nada em staging/. Prévia em staging_dryrun/")
    log.info("Log: %s", _LOG_FILE)


if __name__ == "__main__":
    main()
