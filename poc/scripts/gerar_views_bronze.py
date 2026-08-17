#!/usr/bin/env python3
"""Expõe a bronze (parquet tipado no MinIO) ao Postgres como VIEW, via pg_duckdb.

É a peça que responde à pergunta "e o dbt-postgres, como enxerga a bronze?". A view não
materializa nada: o dado continua só no parquet. É a partir dela que os models de
silver/gold seguem escritos em dialeto Postgres, com os UDFs Postgres intactos.

Duas coisas medidas na POC e embutidas aqui:

  1. `SELECT * FROM read_parquet(...)` NÃO serve: o pg_duckdb devolve uma única coluna
     opaca do tipo duckdb."row". É preciso enumerar `r['coluna']` — e com CAST explícito,
     senão toda coluna chega ao catálogo do Postgres como USER-DEFINED.

  2. A enumeração é GERADA a partir do footer do parquet, não escrita à mão. É o mesmo que
     `scripts/staging_para_db.py` já faz hoje (`_ler_metadados` + `_select_texto`), com uma
     diferença: em vez de CAST(... AS VARCHAR) fixo, o tipo vem do próprio parquet.
"""

import argparse
import os
import re
import sys
from pathlib import Path

import duckdb
import psycopg2
import yaml
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ.parent / "airflow_lappis" / "plugins"))
load_dotenv(RAIZ / ".env")

DIR_TIPOS = RAIZ / "dbt_poc" / "tipos"
BUCKET = os.environ["POC_MINIO_BUCKET"]
SCHEMA_VIEW = os.environ.get("POC_SCHEMA_BRONZE", "bronze")

# DuckDB -> Postgres. Mecânico: é só o mapeamento entre dois sistemas de tipos.
TIPOS = {
    "BOOLEAN": "boolean", "TINYINT": "smallint", "SMALLINT": "smallint",
    "INTEGER": "integer", "BIGINT": "bigint", "HUGEINT": "numeric",
    "FLOAT": "real", "DOUBLE": "double precision",
    "VARCHAR": "text", "DATE": "date", "TIME": "time",
    "TIMESTAMP": "timestamp", "TIMESTAMP WITH TIME ZONE": "timestamptz",
    "BLOB": "bytea", "UUID": "uuid",
}


def tipo_pg(tipo_duckdb: str) -> str:
    t = tipo_duckdb.upper()
    if t.startswith("DECIMAL"):
        m = re.match(r"DECIMAL\((\d+),\s*(\d+)\)", t)
        return f"numeric({m.group(1)},{m.group(2)})" if m else "numeric"
    return TIPOS.get(t, "text")


def con_duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE SECRET p (TYPE s3, KEY_ID '{os.environ["POC_MINIO_ACCESS_KEY"]}',
            SECRET '{os.environ["POC_MINIO_SECRET_KEY"]}', REGION 'us-east-1',
            ENDPOINT '{os.environ["POC_MINIO_ENDPOINT"]}', URL_STYLE 'path', USE_SSL 'false')
    """)
    return con


def con_pg():
    return psycopg2.connect(
        host=os.environ["POC_PG_HOST"], port=os.environ["POC_PG_PORT"],
        user=os.environ["POC_PG_USER"], password=os.environ["POC_PG_PASSWORD"],
        dbname=os.environ["POC_PG_DBNAME"],
    )


def leitura(tabela: str, particao: "str | None") -> str:
    """Expressão read_parquet da bronze — com ou sem particionamento Hive.

    Duas armadilhas do pg_duckdb aqui, ambas do PARSER do Postgres (a query é analisada
    por ele antes de chegar ao DuckDB):

      - `hive_types` recebe um struct literal do DuckDB (`{'col':'TIPO'}`) e o Postgres
        rejeita com "syntax error at or near {". Sem ele a chave de partição chega como
        texto e quem restaura o tipo é o CAST da própria view.
      - argumento nomeado precisa de `:=`, não `=`. Com `=` o Postgres lê
        `hive_partitioning` como nome de coluna e falha com "column does not exist".
    """
    if particao:
        return (f"read_parquet('s3://{BUCKET}/bronze/{tabela}/*/*.parquet', "
                f"hive_partitioning := true)")
    # Sem partition_by, a materialização `external` grava o COPY exatamente no `location`
    # declarado no model (sem acrescentar `.parquet`) — é essa chave literal que existe no
    # MinIO, não `bronze/<tabela>.parquet`.
    return f"read_parquet('s3://{BUCKET}/bronze/{tabela}')"


def esquema_do_parquet(con, expr_leitura: str) -> list:
    expr_local = expr_leitura.replace(f"s3://{BUCKET}/", f"s3://{BUCKET}/")
    return [(n, t) for n, t, *_ in con.sql(f"DESCRIBE SELECT * FROM {expr_local}").fetchall()]


def sql_view(tabela: str, colunas: list, expr_leitura: str,
             particao: "str | None", tipo_particao: str = "integer") -> str:
    partes = []
    for nome, tipo in colunas:
        if nome == particao:
            # A chave de partição vem do nome do diretório, sempre como texto. Quando o
            # valor é nulo o DuckDB grava o diretório literalmente como `col=NULL`, então
            # o cast direto quebraria: o nullif desarma isso.
            # O tipo vem do YAML, não do footer: sem hive_types o DuckDB devolve a chave
            # de partição como VARCHAR, e o footer não sabe qual era o tipo original.
            expr = f"(nullif(r['{nome}'], 'NULL'))::{tipo_particao}"
        else:
            expr = f"(r['{nome}'])::{tipo_pg(tipo)}"
        partes.append(f"    {expr} AS \"{nome}\"")
    return (
        f"CREATE OR REPLACE VIEW {SCHEMA_VIEW}.{tabela} AS\nSELECT\n"
        + ",\n".join(partes)
        + f"\nFROM {expr_leitura} r"
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--familia", help="Gera só esta família (default: todas com YAML).")
    p.add_argument("--dry-run", action="store_true", help="Só imprime o SQL.")
    args = p.parse_args()

    ymls = sorted(DIR_TIPOS.glob("*.yml"))
    if args.familia:
        ymls = [y for y in ymls if y.stem == args.familia]

    con, pg = con_duck(), con_pg()
    with pg.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_VIEW}")
        cur.execute(f"""
            SELECT duckdb.create_simple_secret(
                type := 'S3', key_id := '{os.environ["POC_MINIO_ACCESS_KEY"]}',
                secret := '{os.environ["POC_MINIO_SECRET_KEY"]}', region := 'us-east-1',
                url_style := 'path',
                endpoint := '{os.environ["POC_MINIO_ENDPOINT_INTERNO"]}', use_ssl := 'false')
        """)
        pg.commit()

    for yml in ymls:
        spec = yaml.safe_load(yml.read_text(encoding="utf-8"))
        tabela, particao = spec["tabela"], spec.get("particao")
        expr = leitura(tabela, particao)
        colunas = esquema_do_parquet(con, expr)
        tipo_part = "integer"
        if particao:
            declarado = spec["colunas"].get(particao, {}).get("tipo", "integer")
            tipo_part = {"integer": "integer", "bigint": "bigint"}.get(declarado, "text")
        sql = sql_view(tabela, colunas, expr, particao, tipo_part)

        print(f"\n-- {tabela}  ({len(colunas)} colunas, geradas do footer do parquet)")
        print(sql)
        if args.dry_run:
            continue

        with pg.cursor() as cur:
            cur.execute(f"DROP VIEW IF EXISTS {SCHEMA_VIEW}.{tabela}")
            cur.execute(sql)
            pg.commit()
            cur.execute(f"SELECT count(*) FROM {SCHEMA_VIEW}.{tabela}")
            n = cur.fetchone()[0]
            cur.execute("""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position
            """, (SCHEMA_VIEW, tabela))
            tipos = cur.fetchall()
        print(f"\n   OK: {n:,} linhas | tipos no catálogo do Postgres:")
        for nome, t in tipos:
            print(f"     {nome:26s} {t}")

    pg.close()


if __name__ == "__main__":
    main()
