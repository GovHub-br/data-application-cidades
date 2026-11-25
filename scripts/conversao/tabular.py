"""Conversão CSV/TXT -> Parquet (streaming, chunked)."""

import json
import logging
from typing import Any, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from lake_utils import encoding_fallback, normalizar_colunas

from .config import colunas_de_linhagem

log = logging.getLogger(__name__)

# Colunas de padding: export de Excel pode arrastar a largura inteira da planilha (16.384
# colunas). A regra é conservadora — só descarta coluna sem nome E 100% vazia. Como saber
# se está vazia exige varrer o arquivo, a passada extra só roda quando o header passa
# deste limite de colunas anônimas.
LIMITE_COLS_SEM_NOME = 10


def _e_sem_nome(coluna: str) -> bool:
    """pandas nomeia header vazio como 'Unnamed: N'."""
    return str(coluna).startswith("Unnamed:")


def _n_colunas_arquivo(
    src_path: str, delim: str, encoding: str, encoding_errors: str = "strict"
) -> int:
    """Nº de colunas do header (lê só o header, nrows=0)."""
    return len(
        pd.read_csv(
            src_path,
            sep=delim,
            dtype=str,
            nrows=0,
            encoding=encoding,
            encoding_errors=encoding_errors,
            quotechar='"',
        ).columns
    )


def _colunas_a_manter(
    src_path: str,
    delim: str,
    encoding: str,
    chunksize: int,
    encoding_errors: str = "strict",
) -> Optional[List[str]]:
    """Nomes das colunas que devem ir p/ o parquet, ou None se não há padding a limpar.

    None = nada a fazer (caso da esmagadora maioria dos arquivos) → sem passada extra.
    """
    todas = list(
        pd.read_csv(
            src_path,
            sep=delim,
            dtype=str,
            nrows=0,
            encoding=encoding,
            encoding_errors=encoding_errors,
            quotechar='"',
        ).columns
    )
    anonimas = [c for c in todas if _e_sem_nome(c)]
    if len(anonimas) <= LIMITE_COLS_SEM_NOME:
        return None  # arquivo normal: nem varre o conteúdo

    # varre o arquivo p/ saber quais anônimas têm algum valor (só essas escapam do
    # descarte)
    com_dado = set()
    reader = pd.read_csv(
        src_path,
        sep=delim,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        encoding=encoding,
        encoding_errors=encoding_errors,
        quotechar='"',
        chunksize=chunksize,
        on_bad_lines="skip",
        engine="c",
        usecols=anonimas,
    )
    for chunk in reader:
        for c in chunk.columns:
            if c not in com_dado and (chunk[c].str.strip() != "").any():
                com_dado.add(c)
        if len(com_dado) == len(anonimas):
            break  # todas têm dado: nada a descartar

    manter = [c for c in todas if not _e_sem_nome(c) or c in com_dado]
    return None if len(manter) == len(todas) else manter


class _BadLineCounter:
    """Conta linhas ruins descartadas pelo pandas (on_bad_lines=callable)."""

    def __init__(self) -> None:
        self.n = 0

    def __call__(self, bad_line: List[str]) -> None:
        self.n += 1
        return None  # descarta a linha


def _converter_tabular(
    src_path: str,
    dst_path: str,
    delim: str,
    real_encoding: str,
    source_file: str,
    source_hash: str,
    ingested_at: str,
    chunksize: int,
    bad_mode: str,
    extra_meta: Optional[dict] = None,
    load_id: Optional[str] = None,
) -> Tuple[int, int, int, dict, str]:
    """Retorna (n_linhas, n_colunas, n_bad_lines, column_map, encoding_usado).

    extra_meta: pares extras p/ os metadados do parquet (ex.: {"table": "..."} no .mdb).

    encoding_usado pode diferir de real_encoding: a detecção só olha o sample de 64 KB e
    um byte recusado depois força uma segunda passada. Nesse caso o retorno vem sufixado
    ("utf-8+replace"), e é isso que fica gravado no lake._staging_log e nos metadados do
    parquet — dá para listar os arquivos afetados sem reprocessar nada.

    bad_mode:
      - 'skip'  (default): engine C (rápido) descarta linhas mal-formadas; n_bad não é
        contado (-1).
      - 'count': engine Python (mais lento) descarta E conta as linhas mal-formadas.
      - 'error': falha o arquivo na 1ª linha mal-formada.
    """
    # Tenta, nesta ordem: o encoding detectado em modo estrito; o MESMO encoding com
    # errors="replace"; só então latin-1.
    #
    # O passo 2 mantém o dano onde ele está: os poucos bytes inválidos viram U+FFFD e o
    # resto do arquivo decodifica certo. Cair direto para latin-1 nunca falha — aceita os
    # 256 bytes — e por isso transformaria TODO acento do arquivo em mojibake, cabeçalho
    # incluso.
    encoding = real_encoding
    encoding_errors = "strict"
    while True:
        try:
            resultado = _converter_tabular_1x(
                src_path,
                dst_path,
                delim,
                encoding,
                source_file,
                source_hash,
                ingested_at,
                chunksize,
                bad_mode,
                extra_meta,
                encoding_errors,
                load_id=load_id,
            )
            usado = encoding if encoding_errors == "strict" else f"{encoding}+replace"
            return (*resultado, usado)
        except UnicodeDecodeError as e:
            if encoding_errors == "strict":
                log.warning(
                    "%s: %s não decodifica o arquivo inteiro (%s) — refazendo com "
                    "errors=replace (bytes inválidos viram U+FFFD; o resto do arquivo "
                    "fica intacto).",
                    source_file,
                    encoding,
                    e,
                )
                encoding_errors = "replace"
                continue
            proximo = encoding_fallback(encoding)
            if proximo is None:
                raise
            log.warning(
                "%s: %s+replace ainda falhou (%s) — último recurso, refazendo com %s. "
                "ATENÇÃO: latin-1 aceita qualquer byte e vai gerar mojibake se o arquivo "
                "for utf-8.",
                source_file,
                encoding,
                e,
                proximo,
            )
            encoding = proximo
            encoding_errors = "strict"


def _converter_tabular_1x(
    src_path: str,
    dst_path: str,
    delim: str,
    real_encoding: str,
    source_file: str,
    source_hash: str,
    ingested_at: str,
    chunksize: int,
    bad_mode: str,
    extra_meta: Optional[dict] = None,
    encoding_errors: str = "strict",
    load_id: Optional[str] = None,
) -> Tuple[int, int, int, dict]:
    """Uma passada de conversão com um encoding fixo. Retorna (n_linhas, n_colunas, n_bad,
    map)."""
    bad_counter = _BadLineCounter()
    engine: Any
    on_bad: Any
    if bad_mode == "count":
        engine, on_bad = "python", bad_counter
    elif bad_mode == "error":
        engine, on_bad = "c", "error"
    else:  # skip
        engine, on_bad = "c", "skip"

    # descarta colunas de padding do Excel; usecols evita até parsear (um chunk de 200k
    # linhas x 16.382 colunas estouraria a memória à toa)
    manter = _colunas_a_manter(src_path, delim, real_encoding, chunksize, encoding_errors)
    if manter is not None:
        log.info(
            "%s: %d colunas de padding descartadas (mantidas %d).",
            source_file,
            _n_colunas_arquivo(src_path, delim, real_encoding, encoding_errors) - len(manter),
            len(manter),
        )

    reader = pd.read_csv(
        src_path,
        sep=delim,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        encoding=real_encoding,
        encoding_errors=encoding_errors,
        quotechar='"',
        chunksize=chunksize,
        on_bad_lines=on_bad,
        engine=engine,
        usecols=manter,
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
                campos += [pa.field(c, pa.string()) for c in colunas_de_linhagem(load_id)]
                meta = {
                    b"source_file": source_file.encode("utf-8"),
                    b"source_hash": source_hash.encode("utf-8"),
                    b"encoding": (
                        real_encoding
                        if encoding_errors == "strict"
                        else f"{real_encoding}+{encoding_errors}"
                    ).encode("utf-8"),
                    b"delimiter": delim.encode("utf-8"),
                    b"column_map": json.dumps(column_map, ensure_ascii=False).encode("utf-8"),
                }
                for k, v in (extra_meta or {}).items():
                    meta[k.encode("utf-8")] = str(v).encode("utf-8")
                schema = pa.schema(campos, metadata=meta)
                writer = pq.ParquetWriter(dst_path, schema, compression="snappy")

            chunk.columns = nomes_finais
            chunk["_source_file"] = source_file
            chunk["_ingested_at"] = ingested_at
            chunk["_source_hash"] = source_hash
            if load_id:
                chunk["_load_id"] = load_id
            table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
            writer.write_table(table)
            n_linhas += len(chunk)
    finally:
        if writer is not None:
            writer.close()

    # em modo 'skip' as linhas ruins não são contadas → -1 sinaliza "não contado"
    n_bad = bad_counter.n if bad_mode == "count" else -1
    return n_linhas, n_colunas, n_bad, column_map
