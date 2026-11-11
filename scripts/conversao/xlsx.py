"""Conversão XLSX -> Parquet (uma aba por parquet)."""

import json
from typing import List, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from lake_utils import normalizar_colunas

from .config import LINEAGE_COLS


def _abas_xlsx(src_path: str) -> List[str]:
    import openpyxl

    wb = openpyxl.load_workbook(src_path, read_only=True)
    try:
        return list(wb.sheetnames)
    finally:
        wb.close()


def _converter_xlsx_aba(
    src_path: str,
    aba: str,
    dst_path: str,
    source_file: str,
    source_hash: str,
    ingested_at: str,
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
