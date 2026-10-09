# scripts/mascarar_minio.py

"""
Mascaramento de PII (dados de pessoa física) na camada raw/ do data lake (MinIO).

Percorre os objetos de raw/, detecta colunas sensíveis pelo header e mascara os valores,
sobrescrevendo o objeto no lugar — o raw deixa de conter PII.

Técnica:
  - Identificadores (CPF, NIS) -> token HMAC-SHA256 determinístico: irreversível, mas
    preserva join e contagem de distintos entre bases.
  - Quasi-identificadores (nome de PF, endereço, CEP, nascimento) -> redação.
  - CEP/endereço só são mascarados quando o arquivo tem indicador de PF; em base
    PJ/empreendimento o CEP é preservado.

O arquivo é lido e reescrito em transporte latin-1, byte a byte, então coluna não
mascarada sai byte-idêntica seja qual for o encoding real. O encoding só é detectado para
interpretar os NOMES das colunas.

Idempotência: objeto já mascarado recebe a tag `masked=true` e é pulado nas execuções
seguintes, evitando duplo-HMAC. --force reprocessa.

Auditoria: um parquet por execução em audit/masking/execution_id=<uuid>/ no MinIO, e uma
linha por arquivo em lake._masking_log no Postgres.

Roda em DRY-RUN por padrão, gravando a prévia em masked_dryrun/; --apply sobrescreve.
"""

import argparse
import csv
import io
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
from dotenv import load_dotenv
from psycopg2.extras import Json

from lake_utils import (
    separar_pipeline_novo,
    MDB_EXT,
    detectar_dialeto,
    detectar_encoding,
    md5_arquivo,
    mdb_contar,
    mdb_disponivel,
    mdb_header,
    mdb_tabelas,
)

# plugins/ (ClienteMinio) está na PYTHONPATH dentro do container Airflow; rodando
# standalone, adiciona plugins/ ao sys.path para o import resolver.
_plugins = Path(__file__).resolve().parents[1] / "plugins"
if _plugins.is_dir() and str(_plugins) not in sys.path:
    sys.path.insert(0, str(_plugins))

from cliente_minio import ClienteMinio  # noqa: E402

load_dotenv()

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

PG_HOST = os.environ["DB_DW_HOST_MCID"]
PG_PORT = int(os.environ.get("DB_DW_PORT_MCID", 5432))
PG_USER = os.environ["DB_DW_USER_MCID"]
PG_PASSWORD = os.environ["DB_DW_PASSWORD_MCID"]
PG_DBNAME = os.environ["DB_DW_DBNAME_MCID"]

SCHEMA = os.environ.get("LAKE_SCHEMA", "lake")
CONTROL_TABLE = "_masking_log"

RAW_PREFIX = os.environ.get("MASKING_PREFIX", "raw/")
DRYRUN_PREFIX = "masked_dryrun/"
AUDIT_PREFIX = "audit/masking/"

HMAC_SECRET = os.environ.get("MASKING_HMAC_SECRET", "").encode("utf-8")
TOKEN_LEN = int(os.environ.get("MASKING_TOKEN_LEN", 16))
REDACTION = os.environ.get("MASKING_REDACTION", "***")

# /tmp costuma ser tmpfs pequeno; os Base_PF_FGTS têm ~2 GB e o processamento mantém
# original + mascarado em disco ao mesmo tempo (~4,5 GB). Aponte para um disco com espaço.
TMPDIR = os.environ.get("MASKING_TMPDIR") or None
if TMPDIR:
    os.makedirs(TMPDIR, exist_ok=True)

SUPPORTED_TABULAR = {".csv", ".txt"}
SUPPORTED_EXCEL = {".xlsx"}
SUPPORTED_MDB = MDB_EXT  # .mdb/.accdb — só LEITURA (ver _analisar_mdb)
UNSUPPORTED = {".xls", ".zip"}


# csv pode ter campos grandes (linhas longas de bases bancárias)
csv.field_size_limit(2**31 - 1)

# Regras de classificação, tokens e reescrita de CSV/TXT/XLSX: em
# plugins/ingestion/masking (o mesmo código do preparo MaskPii da ingestão nova).
from ingestion.masking import (  # noqa: E402
    MaskingKeys,
    classificar,
    mascarar_tabular,
    mascarar_xlsx,
    verificar_roundtrip_tabular,
    xlsx_tem_alvo,
)
from ingestion.masking import targets_por_posicao as _targets_por_mapa  # noqa: E402
from ingestion.masking.rules import _PF_INDICATOR_FORTE  # noqa: E402,F401

KEYS = MaskingKeys(secret=HMAC_SECRET, token_len=TOKEN_LEN, redaction=REDACTION)

# Arquivos SEM cabeçalho, onde o matching por nome não teria o que casar: a posição das
# colunas é declarada à mão, por key exata, depois de conferir o conteúdo. Estar aqui
# também significa que a linha 0 é dado, não cabeçalho (ver `_mascarar_tabular`).
COLUNAS_POR_POSICAO: Dict[str, Dict[int, str]] = {
    "raw/sftp/fabrica/GEFUS/ANTERIORES/CAIXA_AF_GEHIS_ALIENACAO_IMOVEL_M202112.TXT": {
        2: "cpf",
        3: "nis",
    },
}


def _avisar_mascaramento_sem_prova(key: str, rec: dict) -> None:
    """Avisa quando um arquivo é mascarado sem prova estrutural de pessoa física.

    Como a reescrita é no lugar, o valor não volta sem reingerir da origem — todo
    mascaramento sem CPF/NIS/nascimento/sensível merece conferência.
    """
    if rec.get("status") not in ("masked", "dry_run"):
        return
    cats = {m["category"] for m in (rec.get("masked_columns") or [])}
    if cats and not (cats & _PF_INDICATOR_FORTE):
        log.warning(
            "%s — mascarado SEM indicador forte de PF (só %s). Confira se não é dado "
            "institucional/de empreendimento antes de aplicar.",
            key,
            ", ".join(sorted(cats)),
        )


def targets_por_posicao(key: str) -> Optional[List[dict]]:
    """Alvos declarados para uma key sem cabeçalho. None se a key não está no mapa."""
    mapa = COLUNAS_POR_POSICAO.get(key)
    return _targets_por_mapa(mapa) if mapa else None


def _mascarar_tabular(
    src_path: str,
    dst_path: str,
    delim: str,
    lineterm: str,
    fully_quoted: bool,
    real_encoding: str,
    targets_fixos: Optional[List[dict]] = None,
) -> Tuple[List[dict], bool, int, int]:
    return mascarar_tabular(
        src_path,
        dst_path,
        delim,
        lineterm,
        fully_quoted,
        real_encoding,
        targets_fixos,
        KEYS,
    )


_verificar_roundtrip_tabular = verificar_roundtrip_tabular
_xlsx_tem_alvo = xlsx_tem_alvo


def _mascarar_xlsx(
    src_path: str, dst_path: str
) -> Tuple[List[dict], bool, int, int, bool]:
    return mascarar_xlsx(src_path, dst_path, KEYS)


# Artefatos locais (arquivo de log, cópia local da auditoria) — úteis rodando standalone,
# mas o diretório do script pode não ser gravável (ex.: bind-mount no Airflow). Controlado
# por LAKE_LOCAL_ARTIFACTS (default "1"): o container do Airflow define "0" para
# desligá-los. O log em stderr fica sempre ativo (o Airflow o captura na UI).
_LOCAL_ARTIFACTS = os.environ.get("LAKE_LOCAL_ARTIFACTS", "1").lower() not in (
    "0",
    "false",
    "no",
)
_LOG_FILE = (
    Path(__file__).parent
    / f"mascarar_minio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
_formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)
if _LOCAL_ARTIFACTS:
    # Standalone: o script gerencia os próprios handlers (stderr + arquivo de log).
    logging.root.setLevel(logging.INFO)
    for _h in (
        logging.StreamHandler(sys.stderr),
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
    ):
        _h.setFormatter(_formatter)
        logging.root.addHandler(_h)
# Sob o Airflow o logger só propaga: um StreamHandler(sys.stderr) aqui multiplicaria cada
# linha, porque o Airflow redireciona stderr de volta ao logging.


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
            cur.execute(f"""
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
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_masking_log_status
                ON {SCHEMA}.{CONTROL_TABLE} (status);
            """)
            conn.commit()
    log.info("Tabela de controle %s.%s garantida.", SCHEMA, CONTROL_TABLE)


def _carregar_masked_hashes(conn_str: str) -> set:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT masked_hash FROM {SCHEMA}.{CONTROL_TABLE}
                WHERE status = 'masked' AND masked_hash IS NOT NULL
            """)
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


# Análise de .mdb (Access) — LEITURA APENAS
def _analisar_mdb(src_path: str) -> Tuple[List[dict], bool, int]:
    """Varre as tabelas do .mdb procurando colunas sensíveis. Retorna (targets, has_pf,
    linhas).

    NÃO mascara: o mdbtools é read-only e reescrever um .mdb exigiria Java. A função só
    responde "tem PII?"; se tiver, o chamador falha, porque gravar PII em silêncio no lake
    seria pior que um erro visível.
    """
    targets: List[dict] = []
    has_pf_any = False
    total = 0
    for tabela in mdb_tabelas(src_path):
        header = mdb_header(src_path, tabela)
        if not header:
            continue
        n = mdb_contar(src_path, tabela)
        if n > 0:
            total += n
        # mdb_header já devolve str decodificado de MDB_ENCODING: nada a reinterpretar
        t, has_pf = classificar(header, None)
        has_pf_any = has_pf_any or has_pf
        targets.extend({**x, "table": tabela} for x in t)
    return targets, has_pf_any, total


# Processamento de um objeto
def _novo_registro(execution_id: str, key: str) -> dict:
    return {
        "execution_id": execution_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "bucket": MINIO_BUCKET,
        "file": key,
        "file_name": key.rsplit("/", 1)[-1],
        "file_format": (
            key.rsplit(".", 1)[-1].lower() if "." in key.rsplit("/", 1)[-1] else ""
        ),
        "encoding": None,
        "delimiter": None,
        "has_pf_indicator": False,
        "has_formulas": False,
        "masked_columns": [],
        "registros_total": 0,
        "registros_alterados": 0,
        "hash_before": None,
        "hash_after": None,
        "status": None,
        "error_message": None,
        "duration_s": 0.0,
    }


def processar_objeto(  # noqa: C901
    minio: ClienteMinio,
    conn_str: str,
    key: str,
    execution_id: str,
    apply: bool,
    masked_hashes: set,
) -> dict:
    t0 = time.time()
    rec = _novo_registro(execution_id, key)
    ext = os.path.splitext(key)[1].lower()

    if ext in UNSUPPORTED:
        rec["status"] = "skipped_unsupported"
        rec["duration_s"] = round(time.time() - t0, 2)
        return rec

    src = dst = None
    try:
        sample = minio.sample_bytes(key)
        real_encoding = detectar_encoding(sample)
        rec["encoding"] = real_encoding

        if ext in SUPPORTED_TABULAR:
            dialeto = detectar_dialeto(sample, real_encoding)
            if dialeto is None:
                rec["status"] = "skipped_no_header"
                rec["duration_s"] = round(time.time() - t0, 2)
                return rec
            delim, lineterm, fully_quoted = dialeto
            rec["delimiter"] = delim

            src = minio.baixar_para_tempfile(key, ext, TMPDIR)
            rec["hash_before"] = md5_arquivo(src)
            if rec["hash_before"] in masked_hashes:
                rec["status"] = "skipped_already"
                return rec

            dst = tempfile.NamedTemporaryFile(delete=False, suffix=ext, dir=TMPDIR).name
            targets_fixos = targets_por_posicao(key)
            targets, has_pf, total, alterados = _mascarar_tabular(
                src, dst, delim, lineterm, fully_quoted, real_encoding, targets_fixos
            )
            rec.update(
                has_pf_indicator=has_pf,
                masked_columns=[
                    {k: t[k] for k in ("column", "category", "action")} for t in targets
                ],
                registros_total=total,
                registros_alterados=alterados,
            )
            if not targets:
                rec["status"] = "skipped_no_pii"
                return rec

            _verificar_roundtrip_tabular(
                src, dst, delim, total, sem_header=targets_fixos is not None
            )

        elif ext in SUPPORTED_MDB:
            # .mdb é somente-leitura (mdbtools não escreve): aqui só verificamos se há
            # PII.
            if not mdb_disponivel():
                raise RuntimeError(
                    "mdbtools não encontrado no PATH — necessário para ler .mdb "
                    "(instale o pacote 'mdbtools')."
                )
            src = minio.baixar_para_tempfile(key, ext, TMPDIR)
            rec["hash_before"] = md5_arquivo(src)
            if rec["hash_before"] in masked_hashes:
                rec["status"] = "skipped_already"
                return rec

            targets, has_pf, total = _analisar_mdb(src)
            rec.update(
                has_pf_indicator=has_pf,
                masked_columns=[
                    {k: t[k] for k in ("column", "category", "action")} for t in targets
                ],
                registros_total=total,
            )
            if not targets:
                rec["status"] = "skipped_no_pii"
                return rec

            # Tem PII e não há como reescrever .mdb — falhar alto em vez de fingir que
            # mascarou.
            cols = ", ".join(f"{t['table']}.{t['column']}" for t in targets[:5])
            rec["status"] = "error"
            rec["error_message"] = (
                f"PII encontrada em .mdb ({len(targets)} coluna(s): {cols}) — mdbtools é "
                "read-only e não há como reescrever o arquivo. Tratar à parte "
                "(converter e "
                "descartar o .mdb, ou usar Jackcess/UCanAccess via Java)."
            )[:500]
            log.error("✗ %s: %s", key, rec["error_message"])
            return rec

        elif ext in SUPPORTED_EXCEL:
            src = minio.baixar_para_tempfile(key, ext, TMPDIR)
            rec["hash_before"] = md5_arquivo(src)
            if rec["hash_before"] in masked_hashes:
                rec["status"] = "skipped_already"
                return rec
            tem_alvo, has_pf_scan = _xlsx_tem_alvo(src)
            if not tem_alvo:
                rec["has_pf_indicator"] = has_pf_scan
                rec["status"] = "skipped_no_pii"
                return rec
            dst = tempfile.NamedTemporaryFile(delete=False, suffix=ext, dir=TMPDIR).name
            targets, has_pf, total, alterados, has_formulas = _mascarar_xlsx(src, dst)
            rec.update(
                has_pf_indicator=has_pf,
                masked_columns=[
                    {k: t[k] for k in ("column", "category", "action")} for t in targets
                ],
                registros_total=total,
                registros_alterados=alterados,
                has_formulas=has_formulas,
            )
            if not targets:
                rec["status"] = "skipped_no_pii"
                return rec
            if has_formulas:
                log.warning(
                    "%s contém fórmulas em colunas não mascaradas — valores em cache "
                    "serão perdidos até reabrir/resalvar num Excel real (leitura "
                    "programática pode "
                    "ver None nessas células).",
                    key,
                )
        else:
            rec["status"] = "skipped_unsupported"
            return rec

        rec["hash_after"] = md5_arquivo(dst)

        if apply:
            minio.upload_arquivo(dst, key)
            minio.marcar_mascarado(key, execution_id, rec["hash_after"])
            rec["status"] = "masked"
        else:
            preview_key = (
                DRYRUN_PREFIX + key[len(RAW_PREFIX) :]
                if key.startswith(RAW_PREFIX)
                else DRYRUN_PREFIX + key
            )
            minio.upload_arquivo(dst, preview_key)
            rec["status"] = "dry_run"

        return rec

    except Exception as e:  # noqa: BLE001
        rec["status"] = "error"
        rec["error_message"] = str(e)[:500]
        log.error("✗ %s: %s", key, e)
        return rec
    finally:
        rec["duration_s"] = round(time.time() - t0, 2)
        for p in (src, dst):
            if p and os.path.exists(p):
                os.unlink(p)


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
        local = Path(__file__).parent / f"auditoria_mascaramento_{execution_id}.parquet"
        df.to_parquet(local, engine="pyarrow", index=False)
        log.info("Auditoria: s3://%s/%s (cópia local: %s)", MINIO_BUCKET, key, local)
    else:
        log.info("Auditoria: s3://%s/%s", MINIO_BUCKET, key)
    return key


# Execução
def run(  # noqa: C901
    apply: bool = False,
    force: bool = False,
    limit: int = 0,
    pattern: str = "",
    only_ext: str = "",
    max_size_mb: int = 0,
    prefix: Optional[str] = None,
) -> Dict[str, int]:
    """Mascara PII nos objetos de raw/. Retorna a contagem por status.

    Ponto de entrada reutilizável (CLI via main(); DAG do Airflow chama run(apply=True)).
    """
    if not HMAC_SECRET:
        raise SystemExit(
            "MASKING_HMAC_SECRET não definida no .env — necessária para "
            "tokenizar CPF/NIS."
        )

    prefix = prefix if prefix is not None else RAW_PREFIX
    execution_id = uuid.uuid4().hex
    only_ext_set = {
        ("." + e.strip().lstrip(".")).lower() for e in only_ext.split(",") if e.strip()
    }

    log.info("=" * 70)
    log.info(
        "Execução %s | modo=%s | prefixo=%s",
        execution_id,
        "APPLY (sobrescreve raw/)" if apply else "DRY-RUN (masked_dryrun/)",
        prefix,
    )
    log.info("=" * 70)

    conn_str = _conn_str()
    _criar_control_table(conn_str)
    masked_hashes = set() if force else _carregar_masked_hashes(conn_str)

    minio = ClienteMinio()

    registros: List[dict] = []
    contagem: Dict[str, int] = {}
    processados = 0

    # A raw do pipeline novo (plugins/ingestion) é imutável: o _SUCCESS de cada
    # partição guarda o sha256 dos arquivos. A listagem é materializada porque o
    # marcador pode vir depois dos arquivos da pasta.
    objetos, do_pipeline_novo = separar_pipeline_novo(list(minio.listar_objetos(prefix)))
    if do_pipeline_novo:
        contagem["skipped_pipeline_novo"] = len(do_pipeline_novo)
        log.info(
            "%d objeto(s) de partições do pipeline novo ignorado(s) (têm _SUCCESS).",
            len(do_pipeline_novo),
        )

    for key, size in objetos:
        # marcador de pasta (0 byte, key terminando em "/"): não é arquivo
        if key.endswith("/"):
            continue
        if pattern and pattern not in key:
            continue
        ext = os.path.splitext(key)[1].lower()
        if only_ext_set and ext not in only_ext_set:
            continue
        # arquivos de lock/temporários do Excel (~$...) não são planilhas reais
        if os.path.basename(key).startswith("~$"):
            continue
        if max_size_mb and size > max_size_mb * 1024 * 1024:
            continue
        if limit and processados >= limit:
            break
        processados += 1

        if not force and minio.esta_mascarado(key):
            rec = _novo_registro(execution_id, key)
            rec["status"] = "skipped_already"
            registros.append(rec)
            contagem["skipped_already"] = contagem.get("skipped_already", 0) + 1
            log.info("→ [%d] %s — já mascarado (tag), pulando", processados, key)
            continue

        rec = processar_objeto(minio, conn_str, key, execution_id, apply, masked_hashes)
        registros.append(rec)
        contagem[rec["status"]] = contagem.get(rec["status"], 0) + 1
        _avisar_mascaramento_sem_prova(key, rec)

        if rec["hash_before"] is not None:
            _registrar_control(conn_str, rec)

        icone = {"masked": "✓", "dry_run": "◐", "error": "✗"}.get(rec["status"], "·")
        log.info(
            "%s [%d] %s — %s | cols=%d | linhas=%d/%d",
            icone,
            processados,
            key,
            rec["status"],
            len(rec["masked_columns"]),
            rec["registros_alterados"],
            rec["registros_total"],
        )

    if registros:
        _gravar_auditoria(minio, execution_id, registros)

    log.info("=" * 70)
    log.info("Concluído. Objetos: %d", processados)
    for status, n in sorted(contagem.items()):
        log.info("  %-20s %d", status, n)
    if not apply:
        log.info("DRY-RUN — nada foi sobrescrito em raw/. Prévia em %s", DRYRUN_PREFIX)
    log.info("Log: %s", _LOG_FILE)
    return contagem


# Main
def main() -> None:
    parser = argparse.ArgumentParser(description="Mascaramento de PII no raw/ do MinIO.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Efetiva a sobrescrita em raw/. Sem esta flag roda em dry-run.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocessa objetos já mascarados (tag masked=true).",
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="Processa no máximo N objetos."
    )
    parser.add_argument("--pattern", default="", help="Filtra por substring na key.")
    parser.add_argument(
        "--only-ext", default="", help="Extensões a processar, ex.: csv,txt"
    )
    parser.add_argument(
        "--max-size-mb",
        type=int,
        default=0,
        help="Pula objetos maiores que N MB (0 = sem limite). "
        "Útil p/ fatiar dry-runs: pequenos primeiro, grandes depois.",
    )
    parser.add_argument(
        "--prefix", default=RAW_PREFIX, help="Prefixo a varrer (default raw/)."
    )
    args = parser.parse_args()

    run(
        apply=args.apply,
        force=args.force,
        limit=args.limit,
        pattern=args.pattern,
        only_ext=args.only_ext,
        max_size_mb=args.max_size_mb,
        prefix=args.prefix,
    )


if __name__ == "__main__":
    main()
