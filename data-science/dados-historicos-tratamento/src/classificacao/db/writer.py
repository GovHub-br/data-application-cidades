"""Write treated DataFrames to the ``dados_historicos_formatados`` schema.

This module handles schema lifecycle (create / drop) and persists
treated tables, classification metadata, quality reports, and
deduplication mappings to the target PostgreSQL schema.
"""

from __future__ import annotations

import logging
import re

import pandas as pd
from sqlalchemy import Engine, text

from .connection import get_schema_target

logger = logging.getLogger(__name__)

__all__ = [
    "criar_schema_target",
    "sanitize_table_name",
    "sanitize_column_names",
    "escrever_tabela",
    "escrever_classificacao",
    "escrever_qualidade",
    "escrever_dedup_map",
]


# ── Public API ────────────────────────────────────────────────────────────


def criar_schema_target(
    engine: Engine,
    schema: str | None = None,
    *,
    confirm: bool = False,
) -> None:
    """Drop and recreate the target schema (CASCADE).

    This destroys all tables in the schema before re-creating it.
    Requires ``confirm=True`` when the schema already exists, to
    prevent accidental data loss.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database engine with sufficient privileges.
    schema : str, optional
        Target schema name.  Defaults to the value returned by
        :func:`get_schema_target`.
    confirm : bool, keyword-only, default False
        Must be ``True`` to drop an existing schema and all its
        contents.  If ``False`` and the schema already exists, a
        ``RuntimeError`` is raised.

    Raises
    ------
    RuntimeError
        If ``confirm=False`` and the target schema already exists.
    """
    if schema is None:
        schema = get_schema_target()

    with engine.connect() as conn:
        # Check if schema exists
        result = conn.execute(
            text(
                "SELECT EXISTS ("
                "SELECT 1 FROM information_schema.schemata "
                "WHERE schema_name = :schema"
                ") AS exists"
            ),
            {"schema": schema},
        )
        exists = bool(result.scalar())

        if exists and not confirm:
            raise RuntimeError(
                f"Schema '{schema}' já existe. "
                f"Passe confirm=True para recriá-lo (DROP ... CASCADE)."
            )

        if exists:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))

        conn.execute(text(f"CREATE SCHEMA {schema}"))
        conn.commit()

    logger.info("Schema '%s' recriado com sucesso.", schema)


def sanitize_table_name(name: str) -> str:
    """Sanitize a table name for use as a PostgreSQL identifier.

    Performs the following transformations in order:

    1. Replace any character that is not alphanumeric or an underscore
       with ``_``.
    2. Truncate to 63 characters (PostgreSQL identifier limit).  If the
       first character after step 1 is a digit, truncate to 62 to reserve
       one character for the leading ``_`` that will be prepended in
       step 4.
    3. Lowercase.
    4. Ensure the first character is a letter or underscore; if not,
       prepend ``_``.

    Parameters
    ----------
    name : str
        Original table name (may include dots, spaces, hyphens, etc.).

    Returns
    -------
    str
        Safe PostgreSQL identifier (lowercase, max 63 chars, starts with
        ``[a-z_]``).
    """
    # Step 1: replace unsafe characters
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    # Step 2: truncate — reserve 1 char when first char is a digit
    #          (so that prepending "_" in step 4 stays ≤ 63 chars)
    if safe and safe[0].isdigit():
        safe = safe[:62]
    else:
        safe = safe[:63]
    # Step 3: lowercase
    safe = safe.lower()
    # Step 4: ensure starts with letter or underscore
    if safe and not re.match(r"[a-z_]", safe[0]):
        safe = "_" + safe
    # Edge case: empty string after sanitisation
    if not safe:
        safe = "_table"
    return safe


def sanitize_column_names(df: pd.DataFrame, max_len: int = 63) -> pd.DataFrame:
    """Sanitize DataFrame column names to fit PostgreSQL identifier limits.

    Truncates long column names preserving the suffix (end portion), and
    resolves collisions by appending ``_2``, ``_3``, etc.  Operates
    in-place on the passed DataFrame and returns it for convenience.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame whose columns will be renamed (modified in-place).
    max_len : int, default 63
        Maximum length for column names (PostgreSQL identifier limit).

    Returns
    -------
    pd.DataFrame
        The same DataFrame instance with sanitized column names.
    """
    new_names: list[str] = []
    used: set[str] = set()

    for col in df.columns:
        name = str(col)

        # Truncate if too long, preserving the suffix (end portion)
        if len(name) > max_len:
            name = "..." + name[-(max_len - 3) :]

        # Resolve collisions
        if name in used:
            counter = 2
            while True:
                suffix = f"_{counter}"
                available = max_len - len(suffix)
                if available < 1:
                    candidate = suffix
                else:
                    candidate = name[:available] + suffix
                if candidate not in used:
                    name = candidate
                    break
                counter += 1

        used.add(name)
        new_names.append(name)

    df.columns = new_names
    return df


def escrever_tabela(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    schema: str | None = None,
) -> None:
    """Write a DataFrame to the target schema as a SQL table.

    Parameters
    ----------
    df : pd.DataFrame
        Data to persist.
    table_name : str
        Desired table name (will be sanitised via
        :func:`sanitize_table_name`).
    engine : sqlalchemy.engine.Engine
        Database engine.
    schema : str, optional
        Target schema.  Defaults to the value returned by
        :func:`get_schema_target`.
    """
    if schema is None:
        schema = get_schema_target()

    sanitized = sanitize_table_name(table_name)
    df = sanitize_column_names(df, max_len=63)

    df.to_sql(
        name=sanitized,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
    )
    logger.info(
        "Tabela '%s.%s' escrita (%d linhas).",
        schema,
        sanitized,
        len(df),
    )


def escrever_classificacao(
    df_class: pd.DataFrame,
    engine: Engine,
    schema: str | None = None,
) -> None:
    """Write the classification DataFrame as the ``_classificacao`` table.

    Parameters
    ----------
    df_class : pd.DataFrame
        Classification results with columns such as ``table_name``,
        ``formacao``, ``confidence``, ``notes``.
    engine : sqlalchemy.engine.Engine
        Database engine.
    schema : str, optional
        Target schema.  Defaults to ``get_schema_target()``.
    """
    escrever_tabela(df_class, "_classificacao", engine, schema=schema)


def escrever_qualidade(
    df_qualidade: pd.DataFrame,
    engine: Engine,
    schema: str | None = None,
) -> None:
    """Write the quality report DataFrame as the ``_qualidade`` table.

    Parameters
    ----------
    df_qualidade : pd.DataFrame
        Quality metrics per treated table.
    engine : sqlalchemy.engine.Engine
        Database engine.
    schema : str, optional
        Target schema.  Defaults to ``get_schema_target()``.
    """
    escrever_tabela(df_qualidade, "_qualidade", engine, schema=schema)


def escrever_dedup_map(
    df_dedup: pd.DataFrame,
    engine: Engine,
    schema: str | None = None,
) -> None:
    """Write the deduplication mapping as the ``_dedup_map`` table.

    Parameters
    ----------
    df_dedup : pd.DataFrame
        Mapping with columns ``source_table``, ``content_hash``,
        ``canonical_table``, ``is_duplicate``.
    engine : sqlalchemy.engine.Engine
        Database engine.
    schema : str, optional
        Target schema.  Defaults to ``get_schema_target()``.
    """
    escrever_tabela(df_dedup, "_dedup_map", engine, schema=schema)
