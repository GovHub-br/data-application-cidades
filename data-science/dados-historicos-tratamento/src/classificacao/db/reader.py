"""Read tables from the ``dados_historicos`` schema as ``pd.DataFrame``.

This module provides functions to inspect the database schema, read full
tables into memory, and compute MD5 hashes of table contents for
deduplication or integrity checks.
"""

from __future__ import annotations

import hashlib
import logging
import warnings
from typing import Any

import pandas as pd
from sqlalchemy import Engine, text

from .connection import get_schema_source, get_schema_target

logger = logging.getLogger(__name__)

__all__ = [
    "listar_tabelas",
    "ler_tabela",
    "ler_schema_colunas",
    "calcular_hash_db",
    "ler_classificacao_db",
    "ClassificationNotFoundError",
]


class ClassificationNotFoundError(Exception):
    """Raised when the classification table is not found or is empty."""

    pass


# ── Public API ────────────────────────────────────────────────────────────


def listar_tabelas(engine: Engine, schema: str | None = None) -> list[str]:
    """List all base tables in the given schema.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database engine.
    schema : str, optional
        Schema name.  Defaults to the value returned by
        :func:`get_schema_source`.

    Returns
    -------
    list[str]
        Table names sorted alphabetically.

    Warnings
    --------
    UserWarning
        If the schema contains no tables.
    """
    if schema is None:
        schema = get_schema_source()

    stmt = text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = :schema AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    )

    with engine.connect() as conn:
        result = conn.execute(stmt, {"schema": schema})
        tables = [row[0] for row in result]

    if not tables:
        warnings.warn(
            f"Schema {schema} não contém tabelas.",
            stacklevel=2,
        )

    return tables


def ler_tabela(
    engine: Engine,
    table_name: str,
    schema: str | None = None,
) -> pd.DataFrame:
    """Read an entire table as a ``DataFrame`` of strings.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database engine.
    table_name : str
        Name of the table to read.
    schema : str, optional
        Schema name.  Defaults to the value returned by
        :func:`get_schema_source`.

    Returns
    -------
    pd.DataFrame
        Table contents with ``dtype=object`` (all columns converted to
        ``str``).  If a column named ``unnamed_0`` exists and its values
        are a sequential integer index (``"0", "1", "2", …``) it is
        dropped, mirroring the ``index_col=0`` behaviour used when
        loading CSV samples.

    Raises
    ------
    ValueError
        If the table does not exist in the given schema.
    """
    if schema is None:
        schema = get_schema_source()

    # Validate existence up front (raises clear error before the I/O call).
    if not _tabela_existe(engine, table_name, schema):
        raise ValueError(f"Tabela '{table_name}' não encontrada no schema '{schema}'.")

    df: pd.DataFrame = pd.read_sql_table(
        table_name,
        engine,
        schema=schema,
    )

    # The pipeline expects every column to be a plain string.
    df = df.astype(str)

    # Drop an ``unnamed_0`` column if it looks like a sequential row index
    # (mirrors the ``index_col=0`` used for CSV-based samples).
    if "unnamed_0" in df.columns:
        _dropar_unnamed_0_se_sequencial(df)

    # Guarantee object dtype everywhere after possible column drops.
    df = df.astype(str)

    return df


def ler_schema_colunas(
    engine: Engine,
    schema: str | None = None,
) -> pd.DataFrame:
    """Query ``information_schema.columns`` for the given schema.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database engine.
    schema : str, optional
        Schema name.  Defaults to the value returned by
        :func:`get_schema_source`.

    Returns
    -------
    pd.DataFrame
        Columns: ``table_name``, ``column_name``, ``data_type`` (all as
        strings), sorted by ``table_name`` then ``ordinal_position``.
    """
    if schema is None:
        schema = get_schema_source()

    stmt = text(
        "SELECT table_name, column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_schema = :schema "
        "ORDER BY table_name, ordinal_position"
    )

    with engine.connect() as conn:
        result = conn.execute(stmt, {"schema": schema})
        rows: list[dict[str, Any]] = [
            {"table_name": r[0], "column_name": r[1], "data_type": r[2]} for r in result
        ]

    return pd.DataFrame(rows, dtype=str)


def _ler_e_hash_tabela(
    engine: Engine,
    table_name: str,
    schema: str | None = None,
) -> tuple[str, int]:
    """Read a table in chunks, computing MD5 hash and row count in one pass.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database engine.
    table_name : str
        Name of the table to read and hash.
    schema : str, optional
        Schema name.  Defaults to the value returned by
        :func:`get_schema_source`.

    Returns
    -------
    tuple[str, int]
        ``(hash_hex, row_count)`` — 32-character MD5 digest and total
        number of rows in the table.
    """
    if schema is None:
        schema = get_schema_source()

    hasher = hashlib.md5()
    row_count = 0

    for chunk_df in pd.read_sql_table(
        table_name,
        engine,
        schema=schema,
        chunksize=1000,
    ):
        row_count += len(chunk_df)
        chunk_str = chunk_df.astype(str)
        chunk_str = chunk_str[sorted(chunk_str.columns)]
        hasher.update(str(chunk_str).encode("utf-8"))

    return hasher.hexdigest(), row_count


def calcular_hash_db(
    engine: Engine,
    table_name: str,
    schema: str | None = None,
) -> str:
    """Compute an MD5 hash of the full table contents.

    Reads the table in chunks of 1000 rows, converts each chunk to
    strings, sorts columns for deterministic ordering, and updates the
    hasher with the string representation of the chunk.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database engine.
    table_name : str
        Name of the table to hash.
    schema : str, optional
        Schema name.  Defaults to the value returned by
        :func:`get_schema_source`.

    Returns
    -------
    str
        MD5 hex digest (32-character string).
    """
    hash_hex, _ = _ler_e_hash_tabela(engine, table_name, schema)
    return hash_hex


def ler_classificacao_db(
    engine: Engine,
    schema: str | None = None,
) -> pd.DataFrame:
    """Read the ``_classificacao`` table from the target schema.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database engine.
    schema : str, optional
        Target schema name.  Defaults to the value returned by
        :func:`get_schema_target`.

    Returns
    -------
    pd.DataFrame
        Classification results with columns ``table_name``,
        ``formacao``, ``confidence``, ``notes``.

    Raises
    ------
    ClassificationNotFoundError
        If the ``_classificacao`` table does not exist in the schema
        or is empty.
    """
    if schema is None:
        schema = get_schema_target()

    # Check if table exists
    if not _tabela_existe(engine, "_classificacao", schema):
        raise ClassificationNotFoundError(
            f"Tabela '_classificacao' não encontrada no schema '{schema}'."
        )

    df = pd.read_sql_table("_classificacao", engine, schema=schema)

    if df.empty:
        raise ClassificationNotFoundError(
            f"Tabela '_classificacao' no schema '{schema}' está vazia."
        )

    # Select only the expected columns (ignore extras)
    colunas_esperadas = ["table_name", "formacao", "confidence", "notes"]
    colunas_presentes = [c for c in colunas_esperadas if c in df.columns]
    if len(colunas_presentes) < len(colunas_esperadas):
        faltando = set(colunas_esperadas) - set(colunas_presentes)
        raise ClassificationNotFoundError(
            f"Tabela '_classificacao' não contém as colunas esperadas. "
            f"Faltando: {faltando}"
        )

    return df[colunas_esperadas].astype(str)


# ── Internal helpers ──────────────────────────────────────────────────────


def _tabela_existe(engine: Engine, table_name: str, schema: str) -> bool:
    """Return ``True`` if *table_name* exists in *schema*."""
    stmt = text(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = :schema AND table_name = :table_name "
        "AND table_type = 'BASE TABLE'"
    )
    with engine.connect() as conn:
        count = conn.scalar(stmt, {"schema": schema, "table_name": table_name})
    return count is not None and count > 0


def _dropar_unnamed_0_se_sequencial(df: pd.DataFrame) -> None:
    """Drop ``unnamed_0`` column in-place if it holds sequential ints.

    Checks whether the column values match ``"0", "1", "2", …`` (the
    pattern produced by pandas when saving a DataFrame with ``index=True``
    and later re-reading it).
    """
    # Must have at least 3 rows to check the pattern reliably.
    if len(df) < 3:
        return

    first_values = df["unnamed_0"].iloc[:3].tolist()
    if first_values == ["0", "1", "2"]:
        del df["unnamed_0"]
        logger.debug("Coluna 'unnamed_0' removida (índice sequencial detectado).")
