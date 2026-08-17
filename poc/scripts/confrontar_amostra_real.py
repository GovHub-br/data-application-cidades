#!/usr/bin/env python3
"""Confronta o DuckDB com o pipeline atual sobre a AMOSTRA REAL do lake.

Para cada arquivo da amostra, usa o encoding/delimitador que o `lake_utils` detectou
(gravados em resultados/amostra.json) e compara:
  - o DuckDB consegue ler?
  - a contagem de linhas bate com a do pandas?
  - os nomes de coluna batem?
  - a acentuação sai igual?

Produz resultados/amostra_real.md.
"""

import json
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ.parent / "scripts"))
load_dotenv(RAIZ / ".env")

from lake_utils import normalizar_colunas  # noqa: E402

SAIDA = RAIZ / "resultados" / "amostra_real.md"
GABARITO = RAIZ / "resultados" / "amostra.json"
DADOS = RAIZ / "data" / "amostra_real"
BUCKET = os.environ["POC_MINIO_BUCKET"]


def conectar() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL encodings; LOAD encodings; "
                "INSTALL excel; LOAD excel;")
    con.execute(f"""
        CREATE SECRET p (TYPE s3, KEY_ID '{os.environ["POC_MINIO_ACCESS_KEY"]}',
            SECRET '{os.environ["POC_MINIO_SECRET_KEY"]}', REGION 'us-east-1',
            ENDPOINT '{os.environ["POC_MINIO_ENDPOINT"]}', URL_STYLE 'path', USE_SSL 'false')
    """)
    return con


def ler_duckdb(con, info: dict) -> dict:
    nome = info["arquivo"]
    url = f"s3://{BUCKET}/raw/{nome}"
    if nome.lower().endswith(".xlsx"):
        q = f"read_xlsx('{url}', header=true, all_varchar=true)"
    else:
        enc = info["encoding_lake_utils"]
        delim = info["delimitador"].replace("\\", "\\\\").replace("'", "''")
        q = (f"read_csv('{url}', delim='{delim}', header=true, all_varchar=true, "
             f"encoding='{enc}')")
    # materializa: count(*) direto sofre pushdown e mentiria sobre linhas descartadas
    con.execute("DROP TABLE IF EXISTS _amostra")
    con.execute(f"CREATE TABLE _amostra AS SELECT * FROM {q}")
    cols = [r[0] for r in con.sql("DESCRIBE _amostra").fetchall()]
    n = con.sql("SELECT count(*) FROM _amostra").fetchall()[0][0]
    return {"linhas": n, "colunas": cols}


def ler_pandas(info: dict) -> dict:
    caminho = DADOS / info["arquivo"]
    if info["arquivo"].lower().endswith(".xlsx"):
        df = pd.read_excel(caminho, dtype=str)
    else:
        df = pd.read_csv(
            caminho, sep=info["delimitador"], encoding=info["encoding_lake_utils"],
            dtype=str, na_filter=False, keep_default_na=False, engine="python",
            on_bad_lines="skip",
        )
    return {"linhas": len(df), "colunas": list(df.columns)}


def main() -> None:
    registros = json.loads(GABARITO.read_text(encoding="utf-8"))
    con = conectar()
    linhas_md = []

    for info in registros:
        nome = info["arquivo"]
        try:
            duck = ler_duckdb(con, info)
            erro_duck = None
        except Exception as e:  # noqa: BLE001
            duck, erro_duck = None, str(e).splitlines()[0][:110]
        try:
            pan = ler_pandas(info)
            erro_pan = None
        except Exception as e:  # noqa: BLE001
            pan, erro_pan = None, str(e).splitlines()[0][:110]

        if duck and pan:
            norm_duck, _ = normalizar_colunas(duck["colunas"])
            norm_pan, _ = normalizar_colunas(pan["colunas"])
            iguais_linhas = duck["linhas"] == pan["linhas"]
            iguais_cols = norm_duck == norm_pan
            veredito = "OK" if (iguais_linhas and iguais_cols) else "DIVERGE"
            detalhe = []
            if not iguais_linhas:
                detalhe.append(f"linhas {duck['linhas']} vs {pan['linhas']}")
            if not iguais_cols:
                detalhe.append(f"colunas {len(norm_duck)} vs {len(norm_pan)}")
            linhas_md.append((nome, info.get("encoding_lake_utils", "—"),
                              repr(info.get("delimitador", "—")),
                              str(duck["linhas"]), str(pan["linhas"]),
                              veredito, "; ".join(detalhe) or "—"))
        else:
            linhas_md.append((nome, info.get("encoding_lake_utils", "—"),
                              repr(info.get("delimitador", "—")),
                              "erro" if erro_duck else str(duck["linhas"]),
                              "erro" if erro_pan else str(pan["linhas"]),
                              "FALHA", erro_duck or erro_pan or "—"))

    ok = sum(1 for r in linhas_md if r[5] == "OK")
    SAIDA.parent.mkdir(exist_ok=True)
    with open(SAIDA, "w", encoding="utf-8") as f:
        f.write("# Amostra real do lake — DuckDB vs pipeline atual\n\n")
        f.write(f"{len(linhas_md)} arquivos amostrados de `raw/` em produção (só objetos que "
                "passaram pelo mascaramento). CSV/TXT truncados por HTTP Range em ~2 MB.\n\n")
        f.write("O DuckDB recebeu o encoding e o delimitador que o `lake_utils` detectou — "
                "a POC não pede ao DuckDB que os adivinhe (ele não sabe: só detecta BOM).\n\n")
        f.write("| arquivo | encoding | delim | linhas DuckDB | linhas pandas | veredito | obs |\n")
        f.write("|---|---|---|---|---|---|---|\n")
        for r in linhas_md:
            nome = r[0].replace("amostra__", "")
            nome = nome[:46] + "…" if len(nome) > 47 else nome
            f.write(f"| `{nome}` | {r[1]} | `{r[2]}` | {r[3]} | {r[4]} | **{r[5]}** | {r[6]} |\n")
        f.write(f"\n**{ok}/{len(linhas_md)} idênticos** entre DuckDB e o pipeline atual.\n")

    print(f"Escrito: {SAIDA}\n")
    for r in linhas_md:
        print(f"  {r[5]:8s} {r[0][:58]:58s} {r[1]:8s} duck={r[3]:>7s} pandas={r[4]:>7s}  {r[6]}")
    print(f"\n  {ok}/{len(linhas_md)} idênticos")


if __name__ == "__main__":
    main()
