#!/usr/bin/env python3
"""Testes negativos de formato: o que o DuckDB NÃO resolve.

Não basta assumir pela documentação — a POC precisa PROVAR, para o README poder afirmar
que o pré-passo Python não desaparece, apenas encolhe.

  .xls  (OLE2/BIFF, formato legado) — hoje UNSUPPORTED no raw_para_staging.py
  .mdb  (Access/JET)                — hoje lido via binário mdbtools

Produz resultados/formatos_negativos.md.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ.parent / "airflow_lappis" / "plugins"))
sys.path.insert(0, str(RAIZ.parent / "scripts"))
load_dotenv(RAIZ / ".env")

from cliente_minio import ClienteMinio  # noqa: E402

SAIDA = RAIZ / "resultados" / "formatos_negativos.md"
BUCKET = os.environ["POC_MINIO_BUCKET"]
XLS_LOCAL = RAIZ / "data" / "sintetico" / "clientes_legado.xls"


def conectar() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL excel; LOAD excel;")
    con.execute(f"""
        CREATE SECRET p (TYPE s3, KEY_ID '{os.environ["POC_MINIO_ACCESS_KEY"]}',
            SECRET '{os.environ["POC_MINIO_SECRET_KEY"]}', REGION 'us-east-1',
            ENDPOINT '{os.environ["POC_MINIO_ENDPOINT"]}', URL_STYLE 'path', USE_SSL 'false')
    """)
    return con


def tentar(con, rotulo: str, sql: str) -> tuple:
    try:
        n = con.sql(sql).fetchall()
        return rotulo, "LEU", str(n[:1])[:70]
    except Exception as e:  # noqa: BLE001
        return rotulo, "FALHOU", str(e).splitlines()[0][:110]


def main() -> None:
    minio = ClienteMinio(
        endpoint=os.environ["POC_MINIO_ENDPOINT"],
        access_key=os.environ["POC_MINIO_ACCESS_KEY"],
        secret_key=os.environ["POC_MINIO_SECRET_KEY"],
        bucket=BUCKET,
    )
    if XLS_LOCAL.exists():
        minio.upload_arquivo(str(XLS_LOCAL), f"raw/{XLS_LOCAL.name}")

    con = conectar()
    url_xls = f"s3://{BUCKET}/raw/{XLS_LOCAL.name}"
    resultados = [
        tentar(con, "read_xlsx sobre .xls (s3://)",
               f"SELECT count(*) FROM read_xlsx('{url_xls}', header=true, all_varchar=true)"),
        tentar(con, "read_xlsx sobre .xls (local)",
               f"SELECT count(*) FROM read_xlsx('{XLS_LOCAL}', header=true, all_varchar=true)"),
        tentar(con, "read_csv sobre .xls (fallback ingênuo)",
               f"SELECT count(*) FROM read_csv('{XLS_LOCAL}', all_varchar=true, ignore_errors=true)"),
    ]

    # O que o ecossistema Python resolve hoje
    xlrd_ok, xlrd_msg = False, "xlrd não instalado"
    try:
        import xlrd  # noqa: PLC0415

        livro = xlrd.open_workbook(str(XLS_LOCAL))
        aba = livro.sheet_by_index(0)
        xlrd_ok, xlrd_msg = True, f"{aba.nrows - 1} linhas, {aba.ncols} colunas"
    except Exception as e:  # noqa: BLE001
        xlrd_msg = str(e)[:80]

    mdbtools = shutil.which("mdb-tables")
    if mdbtools:
        ver = subprocess.run(["mdb-ver"], capture_output=True, text=True).stdout.strip()
        mdb_msg = f"mdbtools presente ({mdbtools}) {ver}"
    else:
        mdb_msg = "mdbtools ausente nesta máquina (está instalado na imagem do Airflow)"

    SAIDA.parent.mkdir(exist_ok=True)
    with open(SAIDA, "w", encoding="utf-8") as f:
        f.write("# Formatos que o DuckDB não resolve\n\n")
        f.write(f"DuckDB {duckdb.__version__} com a extensão `excel` carregada.\n\n")
        f.write("## `.xls` (OLE2/BIFF, Excel legado)\n\n")
        f.write("| tentativa | resultado | mensagem |\n|---|---|---|\n")
        for rotulo, status, msg in resultados:
            f.write(f"| {rotulo} | **{status}** | `{msg}` |\n")
        f.write(f"\nArquivo de teste: `{XLS_LOCAL.name}`, magic "
                f"`{XLS_LOCAL.read_bytes()[:8].hex()}` (OLE2 legítimo, escrito com xlwt).\n\n")
        f.write(f"Fora do DuckDB, `xlrd` lê o mesmo arquivo: **{'sim' if xlrd_ok else 'não'}** "
                f"— {xlrd_msg}.\n\n")
        f.write("Hoje o `raw_para_staging.py` marca `.xls` como `skipped_unsupported`. "
                "O DuckDB **não muda esse status**; quem mudaria seria um pré-passo com `xlrd`.\n\n")

        f.write("## `.mdb` / `.accdb` (Access/JET)\n\n")
        f.write("O DuckDB não tem leitor JET — nem nas extensões core nem nas da comunidade. "
                "Não há sintaxe a testar: não existe função de leitura para esse formato.\n\n")
        f.write(f"O caminho atual continua obrigatório: `mdbtools` ({mdb_msg}), "
                "usado por `lake_utils.mdb_export_para_csv`.\n\n")
        f.write("## Conclusão\n\n")
        f.write("O pré-passo Python **não desaparece com dbt-duckdb; ele encolhe**. Continua "
                "necessário para:\n\n")
        f.write("- converter `.mdb`/`.accdb` (mdbtools)\n")
        f.write("- converter `.xls` legado (xlrd), se um dia deixar de ser ignorado\n")
        f.write("- detectar encoding e delimitador (o DuckDB exige que sejam declarados)\n")
        f.write("- calcular `_source_hash` para a linhagem\n")

    print(f"Escrito: {SAIDA}\n")
    for rotulo, status, msg in resultados:
        print(f"  {status:7s} {rotulo:42s} {msg}")
    print(f"  {'OK' if xlrd_ok else 'FALHOU':7s} {'xlrd lê o mesmo .xls':42s} {xlrd_msg}")
    print(f"  ----    mdbtools: {mdb_msg}")


if __name__ == "__main__":
    main()
