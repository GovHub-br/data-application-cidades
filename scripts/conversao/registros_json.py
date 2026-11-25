"""Conversão JSON -> Parquet (list[dict] já achatado pelo cliente da fonte)."""

import json
from typing import Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from lake_utils import normalizar_colunas

from .config import colunas_de_linhagem


def _converter_json(
    src_path: str,
    dst_path: str,
    source_file: str,
    source_hash: str,
    ingested_at: str,
    load_id: "str | None" = None,
) -> Tuple[int, int, dict]:
    """Converte raw/<fonte>/<dado>.json (list[dict]) para parquet full-text.

    Retorna (n_linhas, n_colunas, column_map). Formas fora de list[dict] não são tratadas
    aqui — só chega em raw/ o formato achatado pelo cliente da fonte; se vier diferente, a
    exceção do pandas sobe e processar_objeto marca status="error" no objeto, sem derrubar
    a execução.
    """
    with open(src_path, "r", encoding="utf-8") as f:
        registros = json.load(f)

    df = pd.json_normalize(registros)
    df = df.fillna("")
    for col in df.columns:
        if df[col].map(lambda v: isinstance(v, (list, dict))).any():
            df[col] = df[col].apply(
                lambda v: (
                    json.dumps(v, ensure_ascii=False, default=str)
                    if isinstance(v, (list, dict))
                    else v
                )
            )
    df = df.astype(str)

    header_original = [str(c) for c in df.columns]
    nomes_finais, column_map = normalizar_colunas(header_original)
    df.columns = nomes_finais
    df["_source_file"] = source_file
    df["_ingested_at"] = ingested_at
    df["_source_hash"] = source_hash
    if load_id:
        df["_load_id"] = load_id

    campos = [pa.field(n, pa.string()) for n in nomes_finais]
    campos += [pa.field(c, pa.string()) for c in colunas_de_linhagem(load_id)]
    meta = {
        b"source_file": source_file.encode("utf-8"),
        b"source_hash": source_hash.encode("utf-8"),
        b"column_map": json.dumps(column_map, ensure_ascii=False).encode("utf-8"),
    }
    schema = pa.schema(campos, metadata=meta)
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, dst_path, compression="snappy")
    return len(df), len(nomes_finais), column_map
