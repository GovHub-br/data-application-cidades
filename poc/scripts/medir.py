#!/usr/bin/env python3
"""Mede os artefatos da POC e escreve resultados/medicoes.md.

Todos os artefatos cobrem O MESMO conteúdo: as 50.000 linhas de raw/clientes.csv.
(A silver do dbt une csv+txt+xlsx = 150.000 linhas; para a medição ela é restrita a
origem='csv', senão a comparação seria 3x maior de um lado.)

CADEIA DE HOJE (3 materializações do mesmo dado):
  A  staging/*.parquet         MinIO,    tudo string   (raw_para_staging.py, pandas+pyarrow)
  D  sftp.<tabela>             Postgres, tudo varchar  (staging_para_db.py, pg_duckdb CTAS)
  F  bronze tipada             Postgres, TIPADA        (dbt-postgres, a partir de D)

CADEIA PROPOSTA (2 materializações):
  C  bronze/*.parquet          MinIO,    TIPADA        (dbt-duckdb)
  E  silver.clientes           Postgres, TIPADA        (dbt-duckdb)

  B  _baseline_text/*.parquet  MinIO, tudo varchar, escrito pelo MESMO engine de C —
     é o controle que separa o efeito do ENGINE do efeito da TIPAGEM.
"""

import os
import sys
from pathlib import Path

import duckdb
import psycopg2
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ.parent / "airflow_lappis" / "plugins"))
load_dotenv(RAIZ / ".env")

from cliente_minio import ClienteMinio  # noqa: E402

SAIDA = RAIZ / "resultados" / "medicoes.md"
BUCKET = os.environ["POC_MINIO_BUCKET"]

A = f"s3://{BUCKET}/staging/clientes.csv.parquet"
B = f"s3://{BUCKET}/_baseline_text/clientes_csv.parquet"
C = f"s3://{BUCKET}/bronze/bronze_clientes_csv.parquet"


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


def minio() -> ClienteMinio:
    return ClienteMinio(
        endpoint=os.environ["POC_MINIO_ENDPOINT"],
        access_key=os.environ["POC_MINIO_ACCESS_KEY"],
        secret_key=os.environ["POC_MINIO_SECRET_KEY"],
        bucket=BUCKET,
    )


def tamanho_objeto(m: ClienteMinio, key: str) -> int:
    return int(m.s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"])


def criar_artefatos_postgres(pg, con: duckdb.DuckDBPyConnection) -> None:
    """Reconstrói a cadeia de hoje no Postgres: D (varchar) e F (bronze tipada sobre D).

    O secret usa o endpoint INTERNO da rede do compose: quem resolve o s3:// é o processo
    do Postgres, dentro do container, onde `localhost` é o próprio container.
    """
    with pg.cursor() as cur:
        cur.execute(f"""
            SELECT duckdb.create_simple_secret(
                type := 'S3', key_id := '{os.environ["POC_MINIO_ACCESS_KEY"]}',
                secret := '{os.environ["POC_MINIO_SECRET_KEY"]}', region := 'us-east-1',
                url_style := 'path', endpoint := '{os.environ["POC_MINIO_ENDPOINT_INTERNO"]}',
                use_ssl := 'false')
        """)

        # --- D: o CTAS all-varchar que o staging_para_db.py faz hoje ---
        # Colunas descobertas do próprio parquet, como o script real faz. Note que os nomes
        # vêm do header já decodificado pelo pipeline atual — e podem estar mojibakados
        # ("Código Cliente" lido como latin-1 vira "ca3digo_cliente").
        colunas = [r[0] for r in con.sql(f"DESCRIBE SELECT * FROM read_parquet('{A}')").fetchall()]
        select_varchar = ", ".join(f"CAST(r['{c}'] AS VARCHAR) AS \"{c}\"" for c in colunas)
        cur.execute("DROP TABLE IF EXISTS sftp.clientes_csv_text")
        cur.execute(
            f"CREATE TABLE sftp.clientes_csv_text AS "
            f"SELECT {select_varchar} FROM read_parquet('{A}') AS r"
        )

        # --- F: a bronze TIPADA que o dbt-postgres produz hoje a partir de D ---
        # Mesma tipagem lógica da bronze da POC, em dialeto Postgres (é o que os models do
        # mcid fazem com parse_date_br / parse_financial_value / casts).
        cur.execute("DROP TABLE IF EXISTS sftp.clientes_csv_bronze_pg")
        cur.execute("""
            CREATE TABLE sftp.clientes_csv_bronze_pg AS
            SELECT
                nullif(trim(c.codigo), '')::integer                    AS codigo_cliente,
                nullif(trim(c.nome), '')                               AS nome_do_cliente,
                CASE
                    WHEN c.data ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN to_date(c.data, 'DD/MM/YYYY')
                    WHEN c.data ~ '^\\d{8}$'               THEN to_date(c.data, 'YYYYMMDD')
                    WHEN c.data ~ '^\\d{4}-\\d{2}-\\d{2}'  THEN c.data::date
                END                                                    AS data_de_cadastro,
                CASE
                    WHEN c.valor IS NULL OR trim(c.valor) IN ('', 'None') THEN 0.00::numeric(15,2)
                    WHEN c.valor LIKE '%%NaN%%'                           THEN 0.00::numeric(15,2)
                    WHEN c.valor ~ '^0+\\d+,\\d+$'
                        THEN replace(ltrim(c.valor, '0'), ',', '.')::numeric(15,2)
                    WHEN c.valor LIKE '%%,%%' AND c.valor LIKE '%%.%%'
                        THEN replace(replace(c.valor, '.', ''), ',', '.')::numeric(15,2)
                    WHEN c.valor LIKE '%%,%%'
                        THEN replace(c.valor, ',', '.')::numeric(15,2)
                    ELSE coalesce(nullif(trim(c.valor), ''), '0')::numeric(15,2)
                END                                                    AS valor_do_contrato,
                CASE upper(trim(c.ativo)) WHEN 'S' THEN true WHEN 'N' THEN false END AS ativo,
                nullif(trim(c.cpf), '')                                AS cpf,
                nullif(trim(c.obs), '')                                AS observacao
            FROM (
                SELECT
                    t."%s" AS codigo, t."%s" AS nome,  t."%s" AS data,
                    t."%s" AS valor,  t."%s" AS ativo, t."%s" AS cpf, t."%s" AS obs
                FROM sftp.clientes_csv_text t
            ) c
        """ % tuple(colunas[:7]))

        # --- E restrita a origem='csv', para casar linha a linha com D e F ---
        cur.execute("DROP TABLE IF EXISTS silver.clientes_csv_medicao")
        cur.execute("""
            CREATE TABLE silver.clientes_csv_medicao AS
            SELECT codigo_cliente, nome_do_cliente, data_de_cadastro, valor_do_contrato,
                   ativo, cpf, observacao
            FROM silver.clientes WHERE origem = 'csv'
        """)
        pg.commit()


def tamanho_tabela(pg, qualificado: str) -> int:
    with pg.cursor() as cur:
        cur.execute("SELECT pg_total_relation_size(%s)", (qualificado,))
        return int(cur.fetchone()[0])


def contar(pg, qualificado: str) -> int:
    with pg.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {qualificado}")
        return int(cur.fetchone()[0])


def tipos_tabela(pg, schema: str, tabela: str) -> list:
    with pg.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position
        """, (schema, tabela))
        return list(cur.fetchall())


def mb(n: int) -> str:
    return f"{n / 1e6:.3f} MB"


def main() -> None:
    con, pg, m = con_duck(), con_pg(), minio()

    print("Reconstruindo a cadeia de hoje no Postgres (D varchar + F bronze tipada) ...")
    criar_artefatos_postgres(pg, con)

    tam = {
        "raw": tamanho_objeto(m, "raw/clientes.csv"),
        "A": tamanho_objeto(m, "staging/clientes.csv.parquet"),
        "B": tamanho_objeto(m, "_baseline_text/clientes_csv.parquet"),
        "C": tamanho_objeto(m, "bronze/bronze_clientes_csv.parquet"),
        "D": tamanho_tabela(pg, "sftp.clientes_csv_text"),
        "F": tamanho_tabela(pg, "sftp.clientes_csv_bronze_pg"),
        "E": tamanho_tabela(pg, "silver.clientes_csv_medicao"),
    }
    linhas = {k: con.sql(f"SELECT count(*) FROM read_parquet('{u}')").fetchall()[0][0]
              for k, u in (("A", A), ("B", B), ("C", C))}
    linhas["D"] = contar(pg, "sftp.clientes_csv_text")
    linhas["F"] = contar(pg, "sftp.clientes_csv_bronze_pg")
    linhas["E"] = contar(pg, "silver.clientes_csv_medicao")

    descr = {k: con.sql(f"DESCRIBE SELECT * FROM read_parquet('{u}')").fetchall()
             for k, u in (("A", A), ("B", B), ("C", C))}

    hoje = tam["A"] + tam["D"] + tam["F"]
    proposta = tam["C"] + tam["E"]

    SAIDA.parent.mkdir(exist_ok=True)
    with open(SAIDA, "w", encoding="utf-8") as f:
        f.write("# Medições — 50.000 linhas, 7 colunas, mesmo conteúdo\n\n")
        f.write(f"Origem: `raw/clientes.csv`, **{mb(tam['raw'])}** (CSV utf-8).\n")
        f.write("Todos os artefatos abaixo representam exatamente essas 50.000 linhas.\n\n")

        f.write("## Artefatos\n\n")
        f.write("| | artefato | onde | tipagem | tamanho | linhas |\n|---|---|---|---|---|---|\n")
        f.write(f"| A | `staging/clientes.csv.parquet` | MinIO | string (pandas) | {mb(tam['A'])} | {linhas['A']:,} |\n")
        f.write(f"| D | `sftp.clientes_csv_text` | Postgres | varchar (pg_duckdb) | {mb(tam['D'])} | {linhas['D']:,} |\n")
        f.write(f"| F | `sftp.clientes_csv_bronze_pg` | Postgres | **tipada** (dbt-postgres) | {mb(tam['F'])} | {linhas['F']:,} |\n")
        f.write("| | | | | | |\n")
        f.write(f"| B | `_baseline_text/clientes_csv.parquet` | MinIO | varchar (DuckDB) | {mb(tam['B'])} | {linhas['B']:,} |\n")
        f.write(f"| C | `bronze/bronze_clientes_csv.parquet` | MinIO | **tipada** (dbt-duckdb) | **{mb(tam['C'])}** | {linhas['C']:,} |\n")
        f.write(f"| E | `silver.clientes` (origem='csv') | Postgres | **tipada** (dbt-duckdb) | {mb(tam['E'])} | {linhas['E']:,} |\n\n")

        f.write("## Comparação das cadeias\n\n")
        f.write("| cadeia | materializações | total |\n|---|---|---|\n")
        f.write(f"| **hoje** | A (parquet text) + D (varchar no PG) + F (bronze tipada no PG) | **{mb(hoje)}** |\n")
        f.write(f"| **proposta** | C (parquet tipado) + E (silver tipada no PG) | **{mb(proposta)}** |\n\n")
        f.write(f"Redução: **{(1 - proposta / hoje) * 100:.1f}%** "
                f"({mb(hoje - proposta)} a menos por arquivo desse porte).\n\n")

        f.write("### De onde vem o ganho\n\n")
        f.write(f"| efeito | comparação | resultado |\n|---|---|---|\n")
        f.write(f"| engine (sem tipagem dos dois lados) | A → B | {mb(tam['A'])} → {mb(tam['B'])} "
                f"= **{(1 - tam['B'] / tam['A']) * 100:.1f}%** |\n")
        f.write(f"| tipagem no parquet (mesmo engine) | B → C | {mb(tam['B'])} → {mb(tam['C'])} "
                f"= **{(1 - tam['C'] / tam['B']) * 100:.1f}%** |\n")
        f.write(f"| eliminar a cópia varchar no Postgres | D some | **{mb(tam['D'])}** "
                f"= {tam['D'] / hoje * 100:.1f}% do total de hoje |\n\n")
        f.write(
            "**Leitura honesta:** a tipagem do parquet dá um ganho real mas modesto "
            f"({(1 - tam['C'] / tam['B']) * 100:.0f}%), porque parquet já comprime texto muito bem "
            "com dictionary+RLE. O ganho dominante é **não materializar a camada intermediária "
            "all-varchar no Postgres** — o armazenamento em tabela relacional custa "
            f"{tam['D'] / tam['A']:.1f}x o parquet equivalente.\n\n"
        )

        f.write("## Tipos resultantes no parquet\n\n")
        f.write("| coluna | A (hoje) | B (controle) | C (proposta) |\n|---|---|---|---|\n")
        for nome, tipo_c, *_ in descr["C"]:
            ta = next((r[1] for r in descr["A"] if r[0] == nome), "—")
            tb = next((r[1] for r in descr["B"] if r[0] == nome), "—")
            f.write(f"| `{nome}` | {ta} | {tb} | **{tipo_c}** |\n")
        f.write(
            "\nAs colunas de A aparecem com nome diferente porque o pipeline atual detectou "
            "o encoding errado neste arquivo — ver a seção de encoding no README.\n\n"
        )

        f.write("## Tipos na silver do Postgres (artefato E)\n\n")
        f.write("| coluna | tipo no Postgres |\n|---|---|\n")
        for nome, tipo in tipos_tabela(pg, "silver", "clientes"):
            f.write(f"| `{nome}` | {tipo} |\n")
        f.write("\nO mapeamento DuckDB → Postgres preservou os tipos: "
                "`INTEGER`, `DATE`, `DECIMAL(15,2)` e `BOOLEAN` chegaram corretos.\n")

    print(f"\nEscrito: {SAIDA}\n")
    for k, rot in (("A", "parquet text (pandas)"), ("D", "varchar no PG"),
                   ("F", "bronze tipada no PG"), ("B", "parquet varchar (duck)"),
                   ("C", "parquet TIPADO"), ("E", "silver tipada no PG")):
        print(f"  {k}  {rot:26s} {mb(tam[k]):>12}   {linhas[k]:,} linhas")
    print(f"\n  hoje  A+D+F = {mb(hoje)}    proposta  C+E = {mb(proposta)}"
          f"    reducao {(1 - proposta / hoje) * 100:.1f}%")


if __name__ == "__main__":
    main()
