"""The single staging writer and its readers (ADR-0004, ADR-0002 R2).

`READERS` maps a reader name (the `reader` of the contract) to a function that turns one
raw object into staging Parquet records. `auto` is the generic conversion that already
exists in scripts/conversao/ (CSV, TXT, XLSX and the CSV of each Access table): it writes
every column as text, with the lineage columns and `_load_id`. Sources that do not deliver
a ready table (the FGV XLSX, the IBGE JSON, the Power BI answer of the Novo CAGED) register
their reader here; a reader only gives the data a table shape.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    import pandas as pd

READERS: dict[str, Callable[..., list]] = {}


def reader(name: str) -> Callable:
    def register(func: Callable) -> Callable:
        READERS[name] = func
        return func

    return register


@reader("auto")
def auto(minio, key: str, execution_id: str, apply: bool, converted: set, **options) -> list:
    """Generic conversion by file extension (existing code, unchanged)."""
    # shortcut: nome curto, como no tasks.transform (scripts/conversao roda também por CLI)
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


# --- API sources: the original file -> a table, nothing else -----------------------------
# The `transform` task of each API DAG writes the result with plugins.datalake.api. A
# parser only gives the delivered file its table shape: no typing, no `dt_ingest` (the
# moment of the load is `_load_id`/`_ingested_at`).

PARSERS: dict[str, Callable[[str], "pd.DataFrame"]] = {}


def parser(name: str) -> Callable:
    def register(func: Callable) -> Callable:
        PARSERS[name] = func
        return func

    return register


def parse(name: Optional[str], path: str) -> "pd.DataFrame":
    if name not in PARSERS:
        raise KeyError(f"no parser {name!r}; register it in plugins.datalake.readers")
    return PARSERS[name](path)


@parser("fgv_icst_csv")
def fgv_icst_csv(path: str) -> "pd.DataFrame":
    """ICST exported by FGVDados: `;`, latin-1, one header row."""
    import pandas as pd

    return pd.read_csv(
        path, sep=";", encoding="latin1", header=0, dtype=str,
        names=["mes", "icst_com_ajuste_sazonal", "icst_sem_ajuste_sazonal"],
    )  # fmt: skip


@parser("sinduscon_incc_xlsx")
def sinduscon_incc_xlsx(path: str) -> "pd.DataFrame":
    """INCC-M of the Sinduscon-PR: 3 title rows, columns A:E, a "Fonte FGV" footer."""
    import pandas as pd

    df = pd.read_excel(
        path, skiprows=3, usecols="A:E",
        names=["mes", "indice", "var_mes", "var_ano", "var_12_meses"],
    )  # fmt: skip
    df = df.dropna(subset=["mes"])
    return df[~df["mes"].astype(str).str.contains("Fonte", case=False, na=False)]


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
        raise KeyError(
            f"no reader {reader_name!r} for {key}; register it in plugins.datalake.readers"
        )
    return READERS[reader_name or "auto"](minio, key, execution_id, apply, converted, **options)
