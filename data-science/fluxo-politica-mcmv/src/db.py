"""Conexão com o Postgres medalhão do Lab (banco cidades).

A senha vem do ~/.pgpass (libpq lê sozinho) ou de um .env na pasta
data_science. A conexão oscila, por isso ler_sql tenta de novo.
"""
import os
import time
from pathlib import Path
from urllib.parse import quote

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

HOST = os.getenv("PGHOST", "")
PORT = os.getenv("PGPORT", "5432")
DB = os.getenv("PGDATABASE", "cidades")
USER = os.getenv("PGUSER", "cidades")
PASSWORD = os.getenv("PGPASSWORD", "")


def url() -> str:
    senha = f":{quote(PASSWORD, safe='')}" if PASSWORD else ""
    return f"postgresql+psycopg2://{USER}{senha}@{HOST}:{PORT}/{DB}?sslmode=disable"


def engine(**kw):
    return create_engine(url(), pool_pre_ping=True, connect_args={"connect_timeout": 10}, **kw)


def ler_sql(sql: str, params=None, tentativas: int = 3, **kw) -> pd.DataFrame:
    """Roda uma consulta e devolve DataFrame, com retry simples."""
    erro = None
    for i in range(tentativas):
        try:
            with engine().connect() as con:
                return pd.read_sql(text(sql), con, params=params, **kw)
        except Exception as e:  # noqa: BLE001
            erro = e
            time.sleep(2 * (i + 1))
    raise erro


def schemas() -> pd.DataFrame:
    return ler_sql(
        """
        select n.nspname as schema, count(c.relname) as objetos
        from pg_namespace n
        left join pg_class c on c.relnamespace = n.oid and c.relkind in ('r','v','m')
        where n.nspname not in ('pg_catalog','information_schema','pg_toast') and n.nspname not like 'pg\\_%%'
        group by 1 order by 1
        """
    )


def tabelas(schema: str) -> pd.DataFrame:
    return ler_sql(
        """
        select table_name, table_type
        from information_schema.tables
        where table_schema = :s order by 1
        """,
        params={"s": schema},
    )


def colunas(schema: str, tabela: str) -> pd.DataFrame:
    return ler_sql(
        """
        select column_name, data_type
        from information_schema.columns
        where table_schema = :s and table_name = :t
        order by ordinal_position
        """,
        params={"s": schema, "t": tabela},
    )
