"""SQLAlchemy engine and session management for PostgreSQL.

Configuration is read from environment variables loaded via ``python-dotenv``
from the project's ``.env`` file at startup.

Environment variables
---------------------
DB_HOST
    PostgreSQL host address.
DB_PORT
    PostgreSQL port number.
DB_NAME
    Database name.
DB_USER
    Database user.
DB_PASSWORD
    Database password (URL-encoded automatically).
DB_SCHEMA_SOURCE
    Source schema (default: ``dados_historicos``).
DB_SCHEMA_TARGET
    Target schema (default: ``dados_historicos_formatados``).
"""

from __future__ import annotations

import logging
import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

# ── Load environment variables from .env at project root ──────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DOTENV_PATH = _PROJECT_ROOT / ".env"
load_dotenv(_DOTENV_PATH)

logger = logging.getLogger(__name__)

__all__ = [
    "get_engine",
    "get_session",
    "get_schema_source",
    "get_schema_target",
]

_DEFAULT_SCHEMA_SOURCE = "dados_historicos"
_DEFAULT_SCHEMA_TARGET = "dados_historicos_formatados"


# ── Public helpers ────────────────────────────────────────────────────────


def get_schema_source() -> str:
    """Return the source schema name from environment or the default.

    Reads ``DB_SCHEMA_SOURCE``. When the variable is not set, emits a
    warning and returns ``"dados_historicos"``.

    Returns
    -------
    str
        Schema name to read original tables from.
    """
    schema = os.environ.get("DB_SCHEMA_SOURCE")
    if schema is None:
        logger.warning(
            "DB_SCHEMA_SOURCE não definida. Usando padrão: '%s'.",
            _DEFAULT_SCHEMA_SOURCE,
        )
        return _DEFAULT_SCHEMA_SOURCE
    return schema


def get_schema_target() -> str:
    """Return the target schema name from environment or the default.

    Reads ``DB_SCHEMA_TARGET``. When the variable is not set, emits a
    warning and returns ``"dados_historicos_formatados"``.

    Returns
    -------
    str
        Schema name to write treated tables to.
    """
    schema = os.environ.get("DB_SCHEMA_TARGET")
    if schema is None:
        logger.warning(
            "DB_SCHEMA_TARGET não definida. Usando padrão: '%s'.",
            _DEFAULT_SCHEMA_TARGET,
        )
        return _DEFAULT_SCHEMA_TARGET
    return schema


def get_engine():
    """Create and return a SQLAlchemy ``Engine`` for the PostgreSQL database.

    Reads ``DB_HOST``, ``DB_PORT``, ``DB_NAME``, ``DB_USER``, and
    ``DB_PASSWORD`` from the environment.  The password is URL-encoded to
    support special characters.

    Returns
    -------
    sqlalchemy.engine.Engine
        Configured engine with ``pool_size=5``, ``max_overflow=10``, and
        a connection timeout of 30 seconds.

    Raises
    ------
    ValueError
        If any of the five required environment variables is missing.
    """
    host = os.environ.get("DB_HOST")
    port = os.environ.get("DB_PORT")
    name = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")

    _validar_varivel("DB_HOST", host)
    _validar_varivel("DB_PORT", port)
    _validar_varivel("DB_NAME", name)
    _validar_varivel("DB_USER", user)
    _validar_varivel("DB_PASSWORD", password)

    # URL-encode the password so special characters (e.g. @, :, /) are safe.
    password_encoded = urllib.parse.quote_plus(password)  # type: ignore[arg-type]

    connection_string = (
        f"postgresql+psycopg2://{user}:{password_encoded}@{host}:{port}/{name}"
    )

    engine = create_engine(
        connection_string,
        pool_size=5,
        max_overflow=10,
        connect_args={"connect_timeout": 30},
    )
    logger.info("Engine criado para %s@%s:%s/%s", user, host, port, name)
    return engine


def get_session(engine=None):
    """Create and return a SQLAlchemy ``Session`` bound to *engine*.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine, optional
        An existing engine.  When ``None`` (the default), a new engine is
        created via ``get_engine()``.

    Returns
    -------
    sqlalchemy.orm.Session
        A new session bound to the given engine.
    """
    if engine is None:
        engine = get_engine()
    return Session(engine)


# ── Internal helpers ──────────────────────────────────────────────────────


def _validar_varivel(nome: str, valor: str | None) -> None:
    """Raise ``ValueError`` if *valor* is ``None`` or empty."""
    if not valor:
        raise ValueError(
            f"Variável de ambiente {nome} não definida. Configure o arquivo .env."
        )
