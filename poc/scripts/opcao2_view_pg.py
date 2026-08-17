#!/usr/bin/env python3
"""Opção 2: bronze parquet tipada exposta ao dbt-postgres como VIEW via pg_duckdb.

Motivação: na Opção 1 (dbt-duckdb ponta a ponta) TODO o SQL de silver/gold passa a ser
dialeto DuckDB — os ~30 models do mcid e os UDFs Postgres precisariam ser reescritos.

Na Opção 2 o dbt-duckdb só faz RAW → bronze parquet tipada. O projeto dbt-postgres atual
continua existindo e enxerga a bronze como uma view sobre o parquet:

    CREATE VIEW bronze_view.clientes AS SELECT * FROM read_parquet('s3://.../bronze/...')

Isso elimina a mesma duplicação (a tabela all-varchar em sftp.* some, e a bronze tipada
vira view em vez de tabela) SEM tocar em silver/gold.

O custo é leitura remota a cada query. Este script mede esse custo.

Produz resultados/opcao2.md.
"""

import os
import statistics
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
load_dotenv(RAIZ / ".env")

SAIDA = RAIZ / "resultados" / "opcao2.md"
BUCKET = os.environ["POC_MINIO_BUCKET"]
BRONZE = f"s3://{BUCKET}/bronze/bronze_clientes_csv.parquet"

CONSULTAS = [
    ("count(*)", "SELECT count(*) FROM {alvo}"),
    ("filtro + agregação",
     "SELECT ativo, count(*), sum(valor_do_contrato) FROM {alvo} "
     "WHERE data_de_cadastro >= DATE '2023-01-01' GROUP BY 1"),
    ("busca por chave", "SELECT * FROM {alvo} WHERE codigo_cliente = 25000"),
]


def cronometrar(cur, sql: str, repeticoes: int = 5) -> float:
    tempos = []
    for _ in range(repeticoes):
        t0 = time.perf_counter()
        cur.execute(sql)
        cur.fetchall()
        tempos.append((time.perf_counter() - t0) * 1000)
    return statistics.median(tempos)


def main() -> None:
    conn = psycopg2.connect(
        host=os.environ["POC_PG_HOST"], port=os.environ["POC_PG_PORT"],
        user=os.environ["POC_PG_USER"], password=os.environ["POC_PG_PASSWORD"],
        dbname=os.environ["POC_PG_DBNAME"],
    )
    cur = conn.cursor()
    cur.execute(f"""
        SELECT duckdb.create_simple_secret(
            type := 'S3', key_id := '{os.environ["POC_MINIO_ACCESS_KEY"]}',
            secret := '{os.environ["POC_MINIO_SECRET_KEY"]}', region := 'us-east-1',
            url_style := 'path', endpoint := '{os.environ["POC_MINIO_ENDPOINT_INTERNO"]}',
            use_ssl := 'false')
    """)

    # Variante A: SELECT * — a forma óbvia, e a que NÃO serve.
    print("Criando view pg_duckdb sobre o parquet tipado ...")
    cur.execute("DROP VIEW IF EXISTS bronze_view.clientes_select_estrela")
    cur.execute(f"""
        CREATE VIEW bronze_view.clientes_select_estrela AS
        SELECT * FROM read_parquet('{BRONZE}')
    """)
    conn.commit()
    cur.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = 'bronze_view' AND table_name = 'clientes_select_estrela'
    """)
    tipos_estrela = cur.fetchall()
    print(f"  SELECT * -> {len(tipos_estrela)} coluna(s): {tipos_estrela}")

    # Variante B: colunas enumeradas com CAST — mesmo padrão do staging_para_db.py.
    # Só assim os tipos chegam ao catálogo do Postgres e o dbt-postgres enxerga a bronze.
    colunas = [
        ("codigo_cliente", "integer"), ("nome_do_cliente", "text"),
        ("data_de_cadastro", "date"), ("valor_do_contrato", "numeric(15,2)"),
        ("ativo", "boolean"), ("cpf", "text"), ("observacao", "text"),
    ]
    select = ", ".join(f"(r['{c}'])::{t} AS {c}" for c, t in colunas)
    cur.execute("DROP VIEW IF EXISTS bronze_view.clientes")
    cur.execute(f"CREATE VIEW bronze_view.clientes AS SELECT {select} FROM read_parquet('{BRONZE}') r")
    conn.commit()

    cur.execute("SELECT count(*) FROM bronze_view.clientes")
    n = cur.fetchone()[0]
    print(f"  view com colunas enumeradas responde: {n:,} linhas")

    # Os tipos chegam ao catálogo do Postgres através da view?
    cur.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = 'bronze_view' AND table_name = 'clientes'
        ORDER BY ordinal_position
    """)
    tipos_view = cur.fetchall()

    cur.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = 'sftp' AND table_name = 'clientes_csv_bronze_pg'
        ORDER BY ordinal_position
    """)
    tipos_tabela = cur.fetchall()

    print("\nMedindo latência (view remota vs tabela materializada) ...")
    medicoes = []
    for rotulo, molde in CONSULTAS:
        t_view = cronometrar(cur, molde.format(alvo="bronze_view.clientes"))
        t_tab = cronometrar(cur, molde.format(alvo="sftp.clientes_csv_bronze_pg"))
        medicoes.append((rotulo, t_view, t_tab))
        print(f"  {rotulo:22s} view={t_view:8.1f} ms   tabela={t_tab:8.1f} ms   "
              f"({t_view / t_tab:.1f}x)")

    SAIDA.parent.mkdir(exist_ok=True)
    with open(SAIDA, "w", encoding="utf-8") as f:
        f.write("# Opção 2 — bronze parquet tipada exposta como view (pg_duckdb)\n\n")
        f.write("Na Opção 1 (dbt-duckdb ponta a ponta) todo o SQL de silver/gold vira dialeto "
                "DuckDB: os models do `mcid` e os UDFs Postgres (`normalize_apf`, `parse_date_br`) "
                "teriam de ser reescritos. A Opção 2 evita isso.\n\n")
        f.write("```sql\nCREATE VIEW bronze_view.clientes AS\n"
                f"SELECT * FROM read_parquet('{BRONZE}');\n```\n\n")
        f.write(f"**Funciona:** a view responde `{n:,}` linhas — mas só na forma correta.\n\n")

        f.write("### Armadilha: `SELECT *` não serve\n\n")
        f.write("A forma óbvia expõe **uma única coluna opaca**, não o schema:\n\n")
        f.write("| coluna | tipo |\n|---|---|\n")
        for nome, tipo in tipos_estrela:
            f.write(f"| `{nome}` | `{tipo}` |\n")
        f.write("\nO dbt-postgres enxergaria uma coluna `read_parquet` do tipo `duckdb.\"row\"` — "
                "inútil como source. É preciso enumerar as colunas com CAST explícito, "
                "exatamente o padrão que o `staging_para_db.py` já usa (`r['coluna']`). "
                "Isso significa que a Opção 2 também exige **schema declarado por arquivo**.\n\n")

        f.write("## Os tipos sobrevivem à view (com colunas enumeradas)?\n\n")
        f.write("| coluna | view sobre o parquet | tabela materializada |\n|---|---|---|\n")
        for (nome, tipo_v), (_, tipo_t) in zip(tipos_view, tipos_tabela):
            marca = "" if tipo_v == tipo_t else " ⚠"
            f.write(f"| `{nome}` | {tipo_v}{marca} | {tipo_t} |\n")
        f.write("\n")

        f.write("## Custo: latência da leitura remota\n\n")
        f.write("| consulta | view (parquet no MinIO) | tabela no Postgres | razão |\n")
        f.write("|---|---|---|---|\n")
        for rotulo, t_view, t_tab in medicoes:
            f.write(f"| {rotulo} | {t_view:.1f} ms | {t_tab:.1f} ms | **{t_view / t_tab:.1f}x** |\n")
        f.write("\nMedianas de 5 execuções, 50.000 linhas, MinIO na mesma máquina "
                "(em produção o MinIO está atrás da VPN — a razão tende a piorar).\n\n")

        f.write("## Leitura\n\n")
        f.write("A Opção 2 elimina a mesma duplicação da Opção 1 (a tabela all-varchar em "
                "`sftp.*` some e a bronze deixa de ser tabela) com uma fração do custo de "
                "migração: nenhum model de silver/gold muda, e os UDFs Postgres continuam "
                "válidos. Em troca, cada consulta à bronze paga leitura remota — aceitável se "
                "a silver for materializada (ela lê a bronze uma vez por run), problemático se "
                "alguém consultar a bronze interativamente.\n")

    print(f"\nEscrito: {SAIDA}")
    conn.close()


if __name__ == "__main__":
    main()
