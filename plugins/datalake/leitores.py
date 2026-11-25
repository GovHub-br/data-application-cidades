"""The single staging writer and its readers (ADR-0004, ADR-0002 R2).

`READERS` maps a reader name (the `reader` of the contract) to a function that turns one
raw object into staging Parquet records. `auto` is the generic conversion that already
exists in scripts/conversao/ (CSV, TXT, XLSX and the CSV of each Access table): it writes
every column as text, with the lineage columns and `_load_id`. Sources that do not deliver
a ready table (the FGV XLSX, the IBGE JSON, the Power BI answer of the Novo CAGED) register
their reader here; a reader only gives the data a table shape.
"""

from __future__ import annotations

from typing import Callable, Optional

READERS: dict[str, Callable[..., list]] = {}


def reader(name: str) -> Callable:
    def register(func: Callable) -> Callable:
        READERS[name] = func
        return func

    return register


@reader("auto")
def auto(minio, key: str, execution_id: str, apply: bool, converted: set, **options) -> list:
    """Generic conversion by file extension (existing code, unchanged)."""
    from conversao.processamento import processar_objeto

    return processar_objeto(
        minio,
        key,
        execution_id,
        apply,
        converted,
        options.get("chunksize", 200_000),
        options.get("bad_mode", "skip"),
    )


def convert(
    minio,
    key: str,
    execution_id: str,
    apply: bool,
    converted: set,
    reader_name: Optional[str] = None,
    **options,
) -> list:
    """Staging records of one approved raw object, through the reader of its contract."""
    if (reader_name or "auto") not in READERS:
        raise KeyError(f"no reader {reader_name!r} for {key}; register it in datalake.leitores")
    return READERS[reader_name or "auto"](minio, key, execution_id, apply, converted, **options)
