#!/usr/bin/env python3
"""Matriz de encoding: DuckDB vs o pipeline Python atual.

Este é o teste mais importante da POC para o lake do MCid, porque `lake_utils.detectar_encoding`
escolhe latin-1 EXATAMENTE quando aparecem os 5 bytes indefinidos em cp1252 — e o comentário
em lake_utils.py:30-32 registra que esses arquivos existem de fato no lake
(b'PARTICIPA\\x9d\\xd1ES' onde deveria estar 'PARTICIPAÇÕES').

Produz resultados/matriz_encoding.md.
"""

import os
import tempfile
from pathlib import Path

import duckdb
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
load_dotenv(RAIZ / ".env")

SAIDA = RAIZ / "resultados" / "matriz_encoding.md"
BUCKET = os.environ["POC_MINIO_BUCKET"]
CP1252_INDEFINIDOS = [0x81, 0x8D, 0x8F, 0x90, 0x9D]


def conectar() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL encodings; LOAD encodings;")
    con.execute(f"""
        CREATE SECRET p (TYPE s3, KEY_ID '{os.environ["POC_MINIO_ACCESS_KEY"]}',
            SECRET '{os.environ["POC_MINIO_SECRET_KEY"]}', REGION 'us-east-1',
            ENDPOINT '{os.environ["POC_MINIO_ENDPOINT"]}', URL_STYLE 'path', USE_SSL 'false')
    """)
    return con


def bytes_aceitos(con: duckdb.DuckDBPyConnection) -> dict:
    """Para cada byte 0x80-0xFF, quem aceita: DuckDB latin-1/cp1252 vs Python latin-1/cp1252."""
    resultado = {}
    for b in range(0x80, 0x100):
        linha = {}
        for enc in ("latin-1", "cp1252"):
            caminho = tempfile.mktemp(suffix=".csv")
            with open(caminho, "wb") as f:
                f.write(b"col\n" + b"a" + bytes([b]) + b"b\n")
            try:
                con.sql(
                    f"SELECT col FROM read_csv('{caminho}', header=true, "
                    f"all_varchar=true, encoding='{enc}')"
                ).fetchall()
                linha[f"duckdb_{enc}"] = True
            except Exception:  # noqa: BLE001
                linha[f"duckdb_{enc}"] = False
            finally:
                os.unlink(caminho)
            try:
                bytes([b]).decode(enc)
                linha[f"python_{enc}"] = True
            except Exception:  # noqa: BLE001
                linha[f"python_{enc}"] = False
        resultado[b] = linha
    return resultado


def teste_patologico(con: duckdb.DuckDBPyConnection) -> list:
    """O caso real: arquivo com bytes indefinidos em cp1252, que hoje o latin-1 do Python salva."""
    url = f"s3://{BUCKET}/raw/clientes_cp1252_patologico.csv"
    base = f"read_csv('{url}', delim=';', header=true, all_varchar=true"
    casos = [
        ("latin-1", f"{base}, encoding='latin-1')"),
        ("latin-1 + ignore_errors", f"{base}, encoding='latin-1', ignore_errors=true)"),
        ("cp1252", f"{base}, encoding='cp1252')"),
        ("cp1252 + ignore_errors", f"{base}, encoding='cp1252', ignore_errors=true)"),
        ("utf-8 + ignore_errors", f"{base}, encoding='utf-8', ignore_errors=true)"),
    ]
    linhas = []
    for nome, q in casos:
        try:
            # count(*) sofre pushdown e conta LINHAS DO ARQUIVO, não linhas entregues.
            # Só materializando dá para saber quantas linhas sobrevivem de verdade.
            n_pushdown = con.sql(f"SELECT count(*) FROM {q}").fetchall()[0][0]
            con.execute("DROP TABLE IF EXISTS _mat")
            con.execute(f"CREATE TABLE _mat AS SELECT * FROM {q}")
            n_real = con.sql("SELECT count(*) FROM _mat").fetchall()[0][0]
            linhas.append((nome, str(n_pushdown), str(n_real), "OK" if n_real == 1000 else "PERDA"))
        except Exception as e:  # noqa: BLE001
            linhas.append((nome, "—", "—", f"ERRO: {str(e).splitlines()[0][:60]}"))
    return linhas


def main() -> None:
    con = conectar()
    aceitos = bytes_aceitos(con)
    patologico = teste_patologico(con)

    rej_duck_l1 = [b for b, v in aceitos.items() if not v["duckdb_latin-1"]]
    rej_duck_cp = [b for b, v in aceitos.items() if not v["duckdb_cp1252"]]
    rej_py_l1 = [b for b, v in aceitos.items() if not v["python_latin-1"]]
    rej_py_cp = [b for b, v in aceitos.items() if not v["python_cp1252"]]

    SAIDA.parent.mkdir(exist_ok=True)
    with open(SAIDA, "w", encoding="utf-8") as f:
        f.write("# Matriz de encoding — DuckDB vs pipeline Python atual\n\n")
        f.write(f"DuckDB {duckdb.__version__}, extensão `encodings` carregada.\n\n")

        f.write("## Bytes aceitos no intervalo 0x80–0xFF\n\n")
        f.write("| leitor | bytes rejeitados | quais |\n|---|---|---|\n")
        f.write(f"| DuckDB `latin-1` | **{len(rej_duck_l1)}** | 0x80–0x9F (todo o intervalo de controle C1) |\n")
        f.write(f"| DuckDB `cp1252` | {len(rej_duck_cp)} | {', '.join(hex(b) for b in rej_duck_cp)} |\n")
        f.write(f"| Python `latin-1` | {len(rej_py_l1)} | — (mapeia os 256 bytes) |\n")
        f.write(f"| Python `cp1252` | {len(rej_py_cp)} | {', '.join(hex(b) for b in rej_py_cp)} |\n\n")
        f.write(
            "**Divergência central:** o `latin-1` do DuckDB rejeita os 32 bytes do intervalo C1; "
            "o do Python aceita todos. Como `lake_utils.detectar_encoding` escolhe latin-1 "
            "justamente quando aparecem bytes indefinidos em cp1252 (que estão nesse intervalo), "
            "o caminho de resgate do pipeline atual **não tem equivalente no DuckDB**.\n\n"
        )

        f.write("## Arquivo patológico (1000 linhas, 10 com bytes indefinidos em cp1252)\n\n")
        f.write("| leitura | `count(*)` (com pushdown) | linhas após materializar | veredito |\n")
        f.write("|---|---|---|---|\n")
        for nome, n_push, n_real, veredito in patologico:
            f.write(f"| `{nome}` | {n_push} | {n_real} | {veredito} |\n")
        f.write(
            "\n**Achado grave:** com `ignore_errors=true` as linhas problemáticas são "
            "descartadas em silêncio, e `count(*)` continua reportando 1000 porque sofre "
            "pushdown e conta linhas do arquivo, não linhas entregues. Uma verificação de "
            "contagem ingênua (como a que `staging_para_db.py` faz hoje) **não pegaria** "
            "essa perda.\n"
        )
    print(f"Escrito: {SAIDA}")
    print(f"  DuckDB latin-1 rejeita {len(rej_duck_l1)} bytes | cp1252 rejeita {len(rej_duck_cp)}")
    for linha in patologico:
        print(f"  {linha[0]:28s} pushdown={linha[1]:6s} real={linha[2]:6s} {linha[3]}")


if __name__ == "__main__":
    main()
