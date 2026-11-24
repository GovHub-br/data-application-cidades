"""Pure helpers of the API ingestion DAGs (no Airflow here: the tasks live in each DAG).

The DAG lands the original file in `snh/raw/<source>/<dataset>/load_id=<id>/`, converts it
with a parser of `plugins.datalake.readers` and writes the staging through
`write_text_parquet`, so every staging Parquet has the same shape (ADR-0002 R2).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pandas as pd

from plugins.datalake.keys import RAW_PREFIX, STAGING_PREFIX
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.lake_utils import normalize_columns

LINEAGE = ["_source_file", "_ingested_at", "_source_hash", "_load_id"]


def as_text(df: pd.DataFrame) -> pd.DataFrame:
    """Every value as text (ADR-0002): null stays null, dates in ISO, nested as JSON."""

    def text(value):
        if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
            return None
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False, default=str)
        return value.isoformat() if hasattr(value, "isoformat") else str(value)

    return df.apply(lambda col: col.map(text).astype("string"))


def write_text_parquet(
    df: pd.DataFrame, dst: str, source_file: str, source_hash: str, load: str
) -> dict:
    """The staging Parquet of one parser result: text columns + lineage columns."""
    names, column_map = normalize_columns([str(c) for c in df.columns])
    df = as_text(df)
    df.columns = names
    df["_source_file"] = source_file
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source_hash"] = source_hash
    df["_load_id"] = load
    schema = pa.schema([pa.field(c, pa.string()) for c in names + LINEAGE])
    pq.write_table(pa.Table.from_pandas(df, schema=schema, preserve_index=False), dst)
    return {"n_linhas": len(df), "n_colunas": len(names), "column_map": column_map}


def staging_key(raw_key: str) -> str:
    """snh/raw/<folder>/load_id=<id>/<name>.<ext> -> snh/staging/<folder>/load_id=<id>/<name>.parquet"""
    return STAGING_PREFIX + os.path.splitext(raw_key.removeprefix(RAW_PREFIX))[0] + ".parquet"


def staging_record(execution_id: str, raw_key: str, **fields) -> dict:
    """A row of lake._staging_log (the fields of plugins.datalake.tasks.STAGING_FIELDS)."""
    base = {"execution_id": execution_id, "raw_key": raw_key, "staging_key": None,
            "source_hash": None, "n_linhas": 0, "n_colunas": 0, "n_bad_lines": 0,
            "encoding": None, "delimiter": None, "column_map": {}, "status": None,
            "error_message": None}  # fmt: skip
    return {**base, **fields}
