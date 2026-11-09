# scripts/raw_para_staging.py

"""
Conversão Raw -> Staging (Parquet) do data lake (MinIO).

Percorre raw/ (CSV/TXT/XLSX/MDB/JSON), detecta separador e encoding de cada arquivo e
grava a versão colunar em staging/, espelhando a estrutura de pastas sem a extensão de
origem: raw/sftp/fabrica/GEFUS/foo.csv -> staging/sftp/fabrica/GEFUS/foo.parquet

Decisões do projeto:
  - Todas as colunas como STRING — a tipagem fica para o dbt/DuckDB.
  - pandas (chunked) + pyarrow ParquetWriter: streaming memory-bounded.
  - Colunas em snake_case ASCII (dedup); header original nos metadados do parquet.
  - Linhagem `_source_file`, `_ingested_at`, `_source_hash` em cada parquet.

Rode DEPOIS do `mascarar_minio.py --apply`: a staging lê o raw/ como está, e fora de ordem
o parquet sai com PII.

Idempotência: lake._staging_log com UNIQUE(raw_key, staging_key, source_hash). O
staging_key entra na chave porque XLSX/MDB geram N parts com o mesmo (raw_key,
source_hash). Use --force para reprocessar.

Organização: este arquivo lista o raw/, escolhe o que converter e registra o resultado;
o trabalho fica em conversao/:
  - config: variáveis de ambiente, prefixos, formatos e log;
  - gemeos: um parquet por dado quando ele chega em vários formatos ou pastas;
  - processamento: a conversão de um objeto, do download ao upload;
  - tabular, xlsx e registros_json: um módulo por formato;
  - pendente: o parquet guardado quando o upload falha;
  - controle: lake._staging_log e o parquet de auditoria.
"""

import argparse
import logging
import os
import uuid
from typing import Dict, List, Optional, Tuple

from conversao.config import PASTAS_IGNORADAS, RAW_PREFIX, _LOG_FILE
from conversao.controle import (
    _carregar_convertidos,
    _conn_str,
    _criar_control_table,
    _gravar_auditoria,
    _registrar_control,
)
from conversao.gemeos import _descartar_gemeos
from conversao.processamento import _novo_registro, processar_objeto

# depois do pacote: conversao/__init__.py põe plugins/ no sys.path
from cliente_minio import ClienteMinio  # noqa: E402

log = logging.getLogger(__name__)

# Execução
def run(  # noqa: C901
    apply: bool = False,
    force: bool = False,
    limit: int = 0,
    pattern: str = "",
    only_ext: str = "",
    max_size_mb: int = 0,
    chunksize: int = 200_000,
    bad_lines: str = "skip",
    prefix: Optional[str] = None,
) -> Dict[str, int]:
    """Converte os objetos de raw/ para Parquet em staging/. Retorna a contagem por
    status.

    Ponto de entrada reutilizável (CLI via main(); DAG do Airflow chama run(apply=True)).
    """
    prefix = prefix if prefix is not None else RAW_PREFIX
    execution_id = uuid.uuid4().hex
    only_ext_set = {
        ("." + e.strip().lstrip(".")).lower() for e in only_ext.split(",") if e.strip()
    }

    log.info("=" * 70)
    log.info(
        "Execução %s | modo=%s | prefixo=%s",
        execution_id,
        "APPLY (staging/)" if apply else "DRY-RUN (staging_dryrun/)",
        prefix,
    )
    log.info("=" * 70)

    conn_str = _conn_str()
    _criar_control_table(conn_str)
    convertidos = set() if force else _carregar_convertidos(conn_str)

    minio = ClienteMinio()

    registros: List[dict] = []
    contagem: Dict[str, int] = {}
    processados = 0

    # A listagem é materializada e o descarte de gêmeos roda ANTES dos filtros do usuário:
    # cada recorte elegeria um vencedor diferente, e `--only-ext csv` seguido de
    # `--only-ext txt` faria os dois formatos escreverem no mesmo parquet. Assim a staging
    # final independe de como a execução foi fatiada.
    todos: List[Tuple[str, int]] = []
    for key, size in minio.listar_objetos(prefix):
        # marcador de pasta (0 byte, key terminando em "/"): não é arquivo
        if key.endswith("/"):
            continue
        if os.path.basename(key).startswith("~$"):
            continue
        # antes do descarte de gêmeos: um objeto de pasta ignorada poderia vencer pelo
        # mesmo stem e mandar o dado bom para skipped_duplicado
        rel = key[len(prefix) :] if key.startswith(prefix) else key
        if rel.split("/", 1)[0] in PASTAS_IGNORADAS:
            continue
        todos.append((key, size))

    vencedores, gemeos = _descartar_gemeos(todos)

    # `pattern` aceita vários substrings separados por vírgula (OR entre eles). Um
    # arquivo específico por vírgula evita reprocessar o lake inteiro para consertar
    # meia dúzia de origens — e a listagem do raw/ roda uma vez só, não uma por padrão.
    padroes = [x.strip() for x in pattern.split(",") if x.strip()]

    def _selecionado(key: str, size: int) -> bool:
        if padroes and not any(p in key for p in padroes):
            return False
        if only_ext_set and os.path.splitext(key)[1].lower() not in only_ext_set:
            return False
        if max_size_mb and size > max_size_mb * 1024 * 1024:
            return False
        return True

    candidatos = [(k, s) for k, s in vencedores if _selecionado(k, s)]

    # só reporta os gêmeos que o recorte atual teria processado
    tamanho = dict(todos)
    gemeos_no_recorte = [k for k in gemeos if _selecionado(k, tamanho[k])]
    for key in gemeos_no_recorte:
        rec = _novo_registro(execution_id, key)
        rec["status"] = "skipped_duplicado"
        registros.append(rec)
        contagem["skipped_duplicado"] = contagem.get("skipped_duplicado", 0) + 1
    if gemeos_no_recorte:
        log.info(
            "%d objeto(s) descartado(s) por já existirem em outro formato "
            "(ver skipped_duplicado na auditoria).",
            len(gemeos_no_recorte),
        )

    for key, size in candidatos:
        if limit and processados >= limit:
            break
        processados += 1

        recs = processar_objeto(
            minio, key, execution_id, apply, convertidos, chunksize, bad_lines
        )
        registros.extend(recs)
        for rec in recs:
            contagem[rec["status"]] = contagem.get(rec["status"], 0) + 1
            if rec["source_hash"] is not None and rec["status"] != "skipped_already":
                _registrar_control(conn_str, rec)

        r0 = recs[0]
        icone = {"converted": "✓", "dry_run": "◐", "error": "✗"}.get(r0["status"], "·")
        log.info(
            "%s [%d] %s — %s | abas/parts=%d | linhas=%d | cols=%d",
            icone,
            processados,
            key,
            r0["status"],
            len(recs),
            sum(r["n_linhas"] for r in recs),
            r0["n_colunas"],
        )

    if registros:
        _gravar_auditoria(minio, execution_id, registros)

    log.info("=" * 70)
    log.info("Concluído. Objetos: %d | parts geradas: %d", processados, len(registros))
    for status, n in sorted(contagem.items()):
        log.info("  %-20s %d", status, n)
    if not apply:
        log.info("DRY-RUN — nada em staging/. Prévia em staging_dryrun/")
    log.info("Log: %s", _LOG_FILE)
    return contagem


# Main
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Conversão Raw -> Staging (Parquet) no MinIO."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Grava em staging/. Sem esta flag roda em dry-run (staging_dryrun/).",
    )
    parser.add_argument(
        "--force", action="store_true", help="Reprocessa objetos já convertidos."
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="Processa no máximo N objetos."
    )
    parser.add_argument(
        "--pattern",
        default="",
        help=(
            "Só objetos cuja key contenha este substring. Aceita vários separados por "
            "vírgula (OR): --pattern 'INT065_...20241129,base_trabalho_social_pnhr_bb'."
        ),
    )
    parser.add_argument(
        "--only-ext",
        default="",
        help="Extensões a processar, separadas por vírgula (ex.: csv,txt).",
    )
    parser.add_argument(
        "--max-size-mb",
        type=int,
        default=0,
        help="Pula objetos maiores que N MB (0 = sem limite).",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=200_000,
        help="Linhas por chunk na leitura CSV/TXT (default 200k).",
    )
    parser.add_argument(
        "--bad-lines",
        choices=["skip", "count", "error"],
        default="skip",
        help="Linhas mal-formadas: 'skip' descarta (rápido, engine C, sem contagem); "
        "'count' descarta e conta (engine Python, mais lento); 'error' falha o arquivo.",
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
        chunksize=args.chunksize,
        bad_lines=args.bad_lines,
        prefix=args.prefix,
    )


if __name__ == "__main__":
    main()
