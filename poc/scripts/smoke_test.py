#!/usr/bin/env python3
"""Kill switches da POC: roda ANTES do dbt para separar problema de S3 de problema de dbt.

1. CREATE SECRET com use_ssl booleano (o que a doc do DuckDB documenta)
2. CREATE SECRET com use_ssl COMO STRING — é o que o secrets.py do dbt-duckdb gera;
   se só esta falhar, o profile precisa usar o bloco legado `settings:`
3. read_csv sobre s3://
4. read_xlsx sobre s3:// — a doc não confirma que o leitor de XLSX funcione por range
   request; se falhar, o XLSX precisa de pré-passo local e "um único dbt run" cai por terra
5. read_csv com encoding latin-1 (nativo) e cp1252 (extensão encodings)
"""

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

ENDPOINT = os.environ["POC_MINIO_ENDPOINT"]
KEY = os.environ["POC_MINIO_ACCESS_KEY"]
SECRET = os.environ["POC_MINIO_SECRET_KEY"]
BUCKET = os.environ["POC_MINIO_BUCKET"]

resultados = []


def testar(nome: str, fn) -> None:
    try:
        out = fn()
        resultados.append((nome, "OK", str(out)[:120]))
        print(f"  ✓ {nome}\n      {str(out)[:160]}")
    except Exception as e:  # noqa: BLE001 — o objetivo é justamente capturar e reportar
        resultados.append((nome, "FALHOU", f"{type(e).__name__}: {e}"))
        print(f"  ✗ {nome}\n      {type(e).__name__}: {str(e)[:300]}")


def conectar() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    return con


print(f"DuckDB {duckdb.__version__} | endpoint={ENDPOINT} bucket={BUCKET}\n")

print("[1] CREATE SECRET com USE_SSL booleano")
con = conectar()
testar("secret booleano", lambda: con.sql(f"""
    CREATE SECRET poc_bool (TYPE s3, KEY_ID '{KEY}', SECRET '{SECRET}',
        REGION 'us-east-1', ENDPOINT '{ENDPOINT}', URL_STYLE 'path', USE_SSL false)
""").fetchall())
testar("  -> read_csv via secret booleano", lambda: con.sql(
    f"SELECT count(*) FROM read_csv('s3://{BUCKET}/raw/clientes.csv', delim=';', all_varchar=true)"
).fetchall())

print("\n[2] CREATE SECRET com USE_SSL como STRING (o que o dbt-duckdb gera)")
con2 = conectar()
testar("secret string", lambda: con2.sql(f"""
    CREATE SECRET poc_str (TYPE s3, KEY_ID '{KEY}', SECRET '{SECRET}',
        REGION 'us-east-1', ENDPOINT '{ENDPOINT}', URL_STYLE 'path', USE_SSL 'false')
""").fetchall())
testar("  -> read_csv via secret string", lambda: con2.sql(
    f"SELECT count(*) FROM read_csv('s3://{BUCKET}/raw/clientes.csv', delim=';', all_varchar=true)"
).fetchall())

print("\n[3] Leitura por formato (usando a conexão do teste 1)")
testar("read_csv delim=';'", lambda: con.sql(
    f"SELECT count(*) FROM read_csv('s3://{BUCKET}/raw/clientes.csv', delim=';', header=true, all_varchar=true)"
).fetchall())
testar("read_csv delim='|' (txt)", lambda: con.sql(
    f"SELECT count(*) FROM read_csv('s3://{BUCKET}/raw/clientes.txt', delim='|', header=true, all_varchar=true)"
).fetchall())

print("\n[4] KILL SWITCH: read_xlsx sobre s3://")
testar("install/load excel", lambda: con.execute("INSTALL excel; LOAD excel;") and "loaded")
testar("read_xlsx remoto", lambda: con.sql(
    f"SELECT count(*) FROM read_xlsx('s3://{BUCKET}/raw/clientes.xlsx', header=true, all_varchar=true)"
).fetchall())

print("\n[5] Encoding")
testar("read_csv encoding='latin-1' (nativo)", lambda: con.sql(
    f"""SELECT "Nome do Cliente" FROM read_csv('s3://{BUCKET}/raw/clientes_latin1.csv',
        delim=';', header=true, all_varchar=true, encoding='latin-1') LIMIT 3"""
).fetchall())
testar("install/load encodings", lambda: con.execute("INSTALL encodings; LOAD encodings;") and "loaded")
# 'cp1252' é o nome aceito; 'windows-1252' NÃO é (a extensão usa nomes de catálogo ICU).
testar("read_csv encoding='cp1252'", lambda: con.sql(
    f"""SELECT "Nome do Cliente" FROM read_csv('s3://{BUCKET}/raw/clientes_cp1252.csv',
        delim=';', header=true, all_varchar=true, encoding='cp1252') LIMIT 3"""
).fetchall())

print("\n" + "=" * 70)
falhas = [r for r in resultados if r[1] != "OK"]
for nome, status, detalhe in resultados:
    print(f"{status:8s} {nome}")
print("=" * 70)
print(f"{len(resultados) - len(falhas)}/{len(resultados)} OK")
