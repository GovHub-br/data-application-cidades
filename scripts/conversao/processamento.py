"""Conversão de um objeto do raw/: escolhe o formato, converte, verifica e envia."""

import json
import logging
import os
import tempfile
import time
from datetime import datetime, timezone
from typing import List

import pyarrow.parquet as pq

from datalake.chaves import load_id_of
from lake_utils import (
    MDB_DELIM,
    MDB_ENCODING,
    detectar_dialeto,
    detectar_encoding,
    md5_arquivo,
    mdb_disponivel,
    mdb_export_para_csv,
    mdb_tabelas,
    norm_header,
)

from cliente_minio import ClienteMinio

from .config import (
    RAW_PREFIX,
    STAGING_PREFIX,
    SUPPORTED_EXCEL,
    SUPPORTED_JSON,
    SUPPORTED_MDB,
    SUPPORTED_TABULAR,
    TMPDIR,
    UNSUPPORTED,
)
from .pendente import _descartar_pendentes_antigos, _guardar_pendente, _pendente
from .registros_json import _converter_json
from .tabular import _converter_tabular
from .xlsx import _abas_xlsx, _converter_xlsx_aba

log = logging.getLogger(__name__)


def _staging_key(raw_key: str, sufixo_parte: str = "") -> str:
    """raw/<pastas>/<nome>.<ext> -> staging/<pastas>/<nome>[__parte].parquet

    A extensão de origem não entra no nome: `foo.csv` e `foo.txt` são o mesmo dado e viram
    um único `foo.parquet`. Quem garante que só um deles chega aqui é o
    `_descartar_gemeos`.

    sufixo_parte: aba de XLSX ou tabela de .mdb (`__<nome>`), quando o arquivo gera N
    parquets.
    """
    rel = raw_key[len(RAW_PREFIX) :] if raw_key.startswith(RAW_PREFIX) else raw_key
    return f"{STAGING_PREFIX}{os.path.splitext(rel)[0]}{sufixo_parte}.parquet"


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


def processar_objeto(  # noqa: C901
    minio: ClienteMinio,
    key: str,
    execution_id: str,
    apply: bool,
    convertidos: set,
    chunksize: int,
    bad_mode: str,
) -> List[dict]:
    """Retorna lista de registros (XLSX pode gerar 1 por aba)."""
    t0 = time.time()
    ext = os.path.splitext(key)[1].lower()
    load_id = load_id_of(key)  # só as cargas da ingestão nova (ADR-0005)

    if ext in UNSUPPORTED:
        rec = _novo_registro(execution_id, key)
        rec["status"] = "skipped_unsupported"
        return [rec]

    src = None
    try:
        sample = minio.sample_bytes(key)
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

            src = minio.baixar_para_tempfile(key, ext, TMPDIR)
            source_hash = md5_arquivo(src)
            rec["source_hash"] = source_hash
            if (key, source_hash) in convertidos:
                rec["status"] = "skipped_already"
                return [rec]

            staging_key = _staging_key(key)
            rec["staging_key"] = staging_key
            ingested_at = datetime.now(timezone.utc).isoformat()

            destino = staging_key if apply else _dryrun_key(staging_key)
            pendente = _pendente(destino, source_hash)
            if os.path.exists(pendente + ".json"):
                with open(pendente + ".json", encoding="utf-8") as f:
                    rec.update(json.load(f))
                _verificar_parquet(pendente, rec["n_linhas"])
                log.info("%s: só reenvia o parquet guardado em %s", key, pendente)
                minio.upload_arquivo(pendente, destino)
                rec["status"] = "converted" if apply else "dry_run"
                os.unlink(pendente)
                os.unlink(pendente + ".json")
                return [rec]
            _descartar_pendentes_antigos(pendente)

            dst = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet", dir=TMPDIR).name
            try:
                n_linhas, n_colunas, n_bad, column_map, enc_usado = _converter_tabular(
                    src,
                    dst,
                    delim,
                    real_encoding,
                    os.path.basename(key),
                    source_hash,
                    ingested_at,
                    chunksize,
                    bad_mode,
                    load_id=load_id,
                )
                # o encoding registrado é o que de fato decodificou o arquivo (pode ter
                # caído para latin-1 no fallback), não o palpite feito sobre o sample
                rec.update(
                    n_linhas=n_linhas,
                    n_colunas=n_colunas,
                    n_bad_lines=n_bad,
                    column_map=column_map,
                    encoding=enc_usado,
                )
                _verificar_parquet(dst, n_linhas)
                try:
                    minio.upload_arquivo(dst, destino)
                except Exception:
                    _guardar_pendente(dst, pendente, rec)
                    raise
                rec["status"] = "converted" if apply else "dry_run"
                if n_bad > 0:
                    log.warning("%s — %d linha(s) mal-formada(s) descartada(s)", key, n_bad)
            finally:
                if os.path.exists(dst):
                    os.unlink(dst)
            return [rec]

        # MDB/ACCDB (uma tabela por parquet)
        if ext in SUPPORTED_MDB:
            if not mdb_disponivel():
                raise RuntimeError(
                    "mdbtools não encontrado no PATH — necessário para ler .mdb "
                    "(instale o pacote 'mdbtools')."
                )
            src = minio.baixar_para_tempfile(key, ext, TMPDIR)
            source_hash = md5_arquivo(src)
            if (key, source_hash) in convertidos:
                rec = _novo_registro(execution_id, key)
                rec["source_hash"] = source_hash
                rec["status"] = "skipped_already"
                return [rec]

            ingested_at = datetime.now(timezone.utc).isoformat()
            recs: List[dict] = []
            for tabela in mdb_tabelas(src):
                rec = _novo_registro(execution_id, key)
                rec["source_hash"] = source_hash
                # mdb-export sempre entrega CSV vírgula em UTF-8 — sem detecção necessária
                rec["encoding"] = MDB_ENCODING
                rec["delimiter"] = MDB_DELIM
                staging_key = _staging_key(key, f"__{norm_header(tabela) or 'tabela'}")
                rec["staging_key"] = staging_key

                csv_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", dir=TMPDIR).name
                dst = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet", dir=TMPDIR).name
                try:
                    mdb_export_para_csv(src, tabela, csv_tmp)
                    if os.path.getsize(csv_tmp) == 0:
                        rec["status"] = "skipped_no_header"
                        recs.append(rec)
                        continue
                    # O .mdb é o único formato que chega à staging com PII: o
                    # mascarar_minio não reescreve Access no raw/ (mdbtools é
                    # read-only), só acusa. Mascara aqui o CSV exportado, com as
                    # mesmas regras e o mesmo HMAC, antes de virar parquet.
                    from mascaramento.tabular import mascarar_csv

                    csv_mascarado = csv_tmp + ".mascarado"
                    alvos, _, alterados = mascarar_csv(
                        csv_tmp, csv_mascarado, MDB_DELIM, MDB_ENCODING
                    )
                    os.replace(csv_mascarado, csv_tmp)
                    if alvos:
                        log.info(
                            "%s / %s: PII mascarada em %d linhas (%s)",
                            key,
                            tabela,
                            alterados,
                            ", ".join(f"{a['column']}:{a['action']}" for a in alvos),
                        )
                    n_linhas, n_colunas, n_bad, column_map, enc_usado = _converter_tabular(
                        csv_tmp,
                        dst,
                        MDB_DELIM,
                        MDB_ENCODING,
                        os.path.basename(key),
                        source_hash,
                        ingested_at,
                        chunksize,
                        bad_mode,
                        extra_meta={"table": tabela},
                        load_id=load_id,
                    )
                    if n_colunas == 0:
                        rec["status"] = "skipped_no_header"
                        recs.append(rec)
                        continue
                    rec.update(
                        n_linhas=n_linhas,
                        n_colunas=n_colunas,
                        n_bad_lines=n_bad,
                        column_map=column_map,
                        encoding=enc_usado,
                    )
                    _verificar_parquet(dst, n_linhas)
                    if apply:
                        minio.upload_arquivo(dst, staging_key)
                        rec["status"] = "converted"
                    else:
                        minio.upload_arquivo(dst, _dryrun_key(staging_key))
                        rec["status"] = "dry_run"
                except Exception as e:  # noqa: BLE001 — falha numa tabela não derruba as outras
                    rec["status"] = "error"
                    rec["error_message"] = f"tabela '{tabela}': {e}"[:500]
                    log.error("✗ %s [%s]: %s", key, tabela, e)
                finally:
                    for p in (csv_tmp, csv_tmp + ".mascarado", dst):
                        if os.path.exists(p):
                            os.unlink(p)
                recs.append(rec)
            return recs

        # JSON (registros já achatados pelo cliente da fonte -> um parquet full-text)
        if ext in SUPPORTED_JSON:
            rec = _novo_registro(execution_id, key)
            src = minio.baixar_para_tempfile(key, ext, TMPDIR)
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
                n_linhas, n_colunas, column_map = _converter_json(
                    src, dst, os.path.basename(key), source_hash, ingested_at, load_id=load_id
                )
                if n_colunas == 0:
                    rec["status"] = "skipped_no_header"
                    return [rec]
                rec.update(n_linhas=n_linhas, n_colunas=n_colunas, column_map=column_map)
                _verificar_parquet(dst, n_linhas)
                if apply:
                    minio.upload_arquivo(dst, staging_key)
                    rec["status"] = "converted"
                else:
                    minio.upload_arquivo(dst, _dryrun_key(staging_key))
                    rec["status"] = "dry_run"
            finally:
                if os.path.exists(dst):
                    os.unlink(dst)
            return [rec]

        # XLSX (uma aba por parquet)
        if ext in SUPPORTED_EXCEL:
            src = minio.baixar_para_tempfile(key, ext, TMPDIR)
            source_hash = md5_arquivo(src)
            if (key, source_hash) in convertidos:
                rec = _novo_registro(execution_id, key)
                rec["source_hash"] = source_hash
                rec["status"] = "skipped_already"
                return [rec]

            ingested_at = datetime.now(timezone.utc).isoformat()
            abas = _abas_xlsx(src)
            multi = len(abas) > 1
            recs = []
            for aba in abas:
                rec = _novo_registro(execution_id, key)
                rec["source_hash"] = source_hash
                sufixo = f"__{norm_header(aba) or 'aba'}" if multi else ""
                staging_key = _staging_key(key, sufixo)
                rec["staging_key"] = staging_key
                dst = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet", dir=TMPDIR).name
                try:
                    n_linhas, n_colunas, column_map = _converter_xlsx_aba(
                        src,
                        aba,
                        dst,
                        os.path.basename(key),
                        source_hash,
                        ingested_at,
                        load_id=load_id,
                    )
                    if n_colunas == 0:
                        rec["status"] = "skipped_no_header"
                        recs.append(rec)
                        continue
                    rec.update(n_linhas=n_linhas, n_colunas=n_colunas, column_map=column_map)
                    _verificar_parquet(dst, n_linhas)
                    if apply:
                        minio.upload_arquivo(dst, staging_key)
                        rec["status"] = "converted"
                    else:
                        minio.upload_arquivo(dst, _dryrun_key(staging_key))
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
    return "staging_dryrun/" + staging_key[len(STAGING_PREFIX) :]
