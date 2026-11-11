# scripts/mascarar_minio.py

"""
Mascaramento de PII (dados de pessoa física) na camada raw/ do data lake (MinIO).

Percorre os objetos de raw/, detecta colunas sensíveis pelo header e mascara os valores,
sobrescrevendo o objeto no lugar — o raw deixa de conter PII.

Técnica:
  - Identificadores (CPF, NIS) -> token HMAC-SHA256 determinístico: irreversível, mas
    preserva join e contagem de distintos entre bases.
  - Nome de PF, endereço e CEP -> redação.
  - CEP/endereço só são mascarados quando o arquivo tem indicador de PF; em base
    PJ/empreendimento o CEP é preservado.
  - Data de nascimento, sexo, raça/cor e deficiência são preservados: não identificam
    ninguém sozinhos e respondem perguntas de perfil. Servem só de indicador de PF.

O arquivo é lido e reescrito em transporte latin-1, byte a byte, então coluna não
mascarada sai byte-idêntica seja qual for o encoding real. O encoding só é detectado para
interpretar os NOMES das colunas.

Idempotência: objeto já mascarado recebe a tag `masked=true` e é pulado nas execuções
seguintes, evitando duplo-HMAC. --force reprocessa.

Auditoria: um parquet por execução em audit/masking/execution_id=<uuid>/ no MinIO, e uma
linha por arquivo em lake._masking_log no Postgres.

Roda em DRY-RUN por padrão, gravando a prévia em masked_dryrun/; --apply sobrescreve.

Organização: este arquivo percorre o raw/ e decide o que fazer com cada objeto; o
trabalho fica em mascaramento/:
  - config: variáveis de ambiente, limites e log;
  - regras: quais colunas são sensíveis e como cada valor é transformado;
  - tabular: CSV/TXT, em disco ou direto do MinIO;
  - paralelo: CSV/TXT grande e sem aspas, em pedaços, um por processo;
  - xlsx e mdb: os outros formatos;
  - auditoria: lake._masking_log e o parquet de cada execução.
"""

import argparse
import logging
import os
import tempfile
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from lake_utils import (
    detectar_dialeto,
    detectar_encoding,
    md5_arquivo,
    mdb_disponivel,
)

from mascaramento.config import (
    DRYRUN_PREFIX,
    HMAC_SECRET,
    MINIO_BUCKET,
    RAW_PREFIX,
    SUPPORTED_EXCEL,
    SUPPORTED_MDB,
    SUPPORTED_TABULAR,
    TMPDIR,
    UNSUPPORTED,
    _LOG_FILE,
)
from mascaramento.regras import (
    _avisar_mascaramento_sem_prova,
    targets_por_posicao,
)
from mascaramento.auditoria import (
    _carregar_masked_hashes,
    _conn_str,
    _criar_control_table,
    _gravar_auditoria,
    _registrar_control,
)
from mascaramento.tabular import _mascarar_tabular_stream
from mascaramento.paralelo import (
    _mascarar_tabular_paralelo,
    _pode_paralelizar,
    _targets_do_sample,
)
from mascaramento.xlsx import (
    _mascarar_xlsx,
    _xlsx_tem_alvo,
)
from mascaramento.mdb import _analisar_mdb

# depois do pacote: mascaramento/__init__.py põe plugins/ no sys.path
from cliente_minio import TRIAGEM_LIBERADA, ClienteMinio

log = logging.getLogger(__name__)


# Processamento de um objeto
def _preview_key(key: str) -> str:
    if key.startswith(RAW_PREFIX):
        return DRYRUN_PREFIX + key[len(RAW_PREFIX) :]
    return DRYRUN_PREFIX + key


def _novo_registro(execution_id: str, key: str) -> dict:
    return {
        "execution_id": execution_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "bucket": MINIO_BUCKET,
        "file": key,
        "file_name": key.rsplit("/", 1)[-1],
        "file_format": (key.rsplit(".", 1)[-1].lower() if "." in key.rsplit("/", 1)[-1] else ""),
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


def _marcar_resultado(
    minio: ClienteMinio, key: str, execution_id: str, rec: dict
) -> None:
    """Grava no objeto a tag que libera a staging: triagem ou mascaramento já feito."""
    if rec["status"] in TRIAGEM_LIBERADA:
        minio.marcar_triagem(key, execution_id, rec["status"])
    elif rec["status"] == "skipped_already" and rec["hash_before"]:
        # conteúdo igual a um mascaramento anterior, mas sem a tag (ex.: objeto copiado
        # na reorganização do lake): regrava para a próxima execução pular na hora
        minio.marcar_mascarado(key, execution_id, rec["hash_before"])


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

            # CSV/TXT vai do MinIO ao MinIO sem disco: o CadÚnico pessoa tem 37 GB
            dest_key = key if apply else _preview_key(key)
            targets_fixos = targets_por_posicao(key)
            # cópia de arquivo já mascarado (ex.: ANTERIORES/) só se reconhece pelo hash;
            # sem esta leitura simples, seria mascarada e enviada inteira para nada
            if (
                masked_hashes
                and _targets_do_sample(sample, delim, real_encoding, targets_fixos)[0]
            ):
                hash_before = minio.md5_objeto(key)
                if hash_before in masked_hashes:
                    rec["hash_before"] = hash_before
                    rec["status"] = "skipped_already"
                    rec["duration_s"] = round(time.time() - t0, 2)
                    return rec
            resultado = None
            if _pode_paralelizar(sample, fully_quoted):
                resultado = _mascarar_tabular_paralelo(
                    minio,
                    key,
                    dest_key,
                    delim,
                    lineterm,
                    real_encoding,
                    sample,
                    targets_fixos,
                    masked_hashes,
                )
            if resultado is None:
                resultado = _mascarar_tabular_stream(
                    minio,
                    key,
                    dest_key,
                    delim,
                    lineterm,
                    fully_quoted,
                    real_encoding,
                    targets_fixos,
                    masked_hashes,
                )
            targets, has_pf, total, alterados, hash_before, hash_after = resultado
            rec.update(
                hash_before=hash_before,
                hash_after=hash_after,
                has_pf_indicator=has_pf,
                masked_columns=[
                    {k: t[k] for k in ("column", "category", "action")} for t in targets
                ],
                registros_total=total,
                registros_alterados=alterados,
            )
            if not targets:
                rec["status"] = "skipped_no_pii"
            elif hash_before in masked_hashes:
                rec["status"] = "skipped_already"
            elif apply:
                minio.marcar_mascarado(key, execution_id, hash_after)
                rec["status"] = "masked"
            else:
                rec["status"] = "dry_run"
            return rec

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

            # mdbtools só lê: o .mdb fica como está no raw e a staging mascara cada
            # tabela exportada, com as mesmas regras, antes de gravar o parquet
            cols = ", ".join(f"{t['table']}.{t['column']}" for t in targets[:5])
            rec["status"] = "mdb_mascara_na_staging"
            log.info(
                "%s: PII em .mdb (%s), mascarada na conversão para a staging", key, cols
            )
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
            minio.upload_arquivo(dst, _preview_key(key))
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
            "MASKING_HMAC_SECRET não definida no .env — necessária para tokenizar CPF/NIS."
        )

    prefix = prefix if prefix is not None else RAW_PREFIX
    execution_id = uuid.uuid4().hex
    only_ext_set = {("." + e.strip().lstrip(".")).lower() for e in only_ext.split(",") if e.strip()}

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

    for key, size in minio.listar_objetos(prefix):
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
        if apply:
            _marcar_resultado(minio, key, execution_id, rec)
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
    parser.add_argument("--limit", type=int, default=0, help="Processa no máximo N objetos.")
    parser.add_argument("--pattern", default="", help="Filtra por substring na key.")
    parser.add_argument("--only-ext", default="", help="Extensões a processar, ex.: csv,txt")
    parser.add_argument(
        "--max-size-mb",
        type=int,
        default=0,
        help="Pula objetos maiores que N MB (0 = sem limite). "
        "Útil p/ fatiar dry-runs: pequenos primeiro, grandes depois.",
    )
    parser.add_argument("--prefix", default=RAW_PREFIX, help="Prefixo a varrer (default raw/).")
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
