#!/usr/bin/env python3
"""Etapa de MANIFESTO: o que o DuckDB não sabe fazer sozinho.

Substitui o papel de DETECÇÃO do raw_para_staging.py (encoding, delimitador, header),
mas não converte nada — o parquet passa a ser escrito pelo dbt-duckdb. Reusa
`scripts/lake_utils.py` do projeto, então a heurística de encoding é literalmente a mesma
que roda hoje em produção.

Grava em `<schema>._manifesto_bronze`, com a mesma chave de idempotência que o
`_staging_log` usa hoje: (minio_key, source_hash).

Agrupa os arquivos em FAMÍLIAS (mesmo layout, datas diferentes): é a família que vira um
model dbt, não o arquivo. 2.703 arquivos em produção = 641 famílias.
"""

import argparse
import os
import re
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ.parent / "airflow_lappis" / "plugins"))
sys.path.insert(0, str(RAIZ.parent / "scripts"))
load_dotenv(RAIZ / ".env")

from cliente_minio import ClienteMinio  # noqa: E402
from lake_utils import detectar_dialeto, detectar_encoding, normalizar_colunas  # noqa: E402

SCHEMA = os.environ.get("POC_SCHEMA", "manifesto")
TABELA = "_manifesto_bronze"
TABULARES = {".csv", ".txt"}
EXCEL = {".xlsx"}

# Bytes que o latin-1 do DuckDB rejeita (todo o intervalo de controle C1). O latin-1 do
# Python aceita os 256 — é essa divergência que faz alguns arquivos do lake não terem
# caminho no DuckDB. O manifesto marca; a decisão de como tratar é da etapa seguinte.
C1 = frozenset(range(0x80, 0xA0))


def detectar_encoding_corrigido(sample: bytes) -> str:
    """`lake_utils.detectar_encoding`, mas tentando UTF-8 ANTES do atalho de bytes
    indefinidos em cp1252.

    ACHADO da POC: a produção checa bytes indefinidos em cp1252 (0x81/0x8D/0x8F/0x90/0x9D)
    e devolve latin-1 antes de sequer tentar UTF-8. Esses mesmos bytes aparecem como
    SEGUNDO byte de sequências UTF-8 válidas (0xC3 0x8D = 'Í', 0xC3 0x93 = 'Ó' etc.), então
    um arquivo genuinamente UTF-8 cai em latin-1 só por conter essas letras acentuadas.

    Testado contra os 5 arquivos reais do lake com tem_bytes_c1=true: 4 de 5 são UTF-8
    válido de ponta a ponta (inclusive o arquivo de produção
    amostra__dados_historicos__caixa_001_2016_grafico_mcmv). Como latin-1 nunca lança
    UnicodeDecodeError, o fallback de `raw_para_staging._converter_tabular` nunca dispara
    para esses casos — hoje eles são gravados como mojibake em silêncio (raiz do
    "ca3digo_cliente" já visto nesta POC: 'código' -> UTF-8 bytes c3 b3 -> lido como
    latin-1 vira 'Ã³' -> normalizar_colunas degrada pra 'a3').

    Isto é uma correção CANDIDATA para `lake_utils.detectar_encoding` (produção), não
    aplicada lá — só usada aqui para provar que o bloqueador de C1 no DuckDB é bem menor
    do que parecia. Ver poc/resultados/bloqueador_c1.md.
    """
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError as e:
        if e.start >= len(sample) - 3:  # possível char utf-8 cortado no fim do sample
            return "utf-8"
    return detectar_encoding(sample)


def reescrever_utf8(minio: ClienteMinio, key: str) -> str:
    """Caso residual genuíno (nem UTF-8 nem cp1252 decodificam certo): reescreve o objeto
    como UTF-8, preservando CADA BYTE no mesmo código que o pipeline Python já assume hoje
    (latin-1 mapeia os 256 valores de byte 1:1 e nunca lança erro — é por isso que o
    pipeline usa latin-1 como fim de linha do fallback). Não é uma normalização nova: é a
    mesma leitura que a produção já faz, só persistida em bytes que o DuckDB consegue ler
    (UTF-8 não tem o intervalo C1 problemático, porque U+0080..U+009F vira uma sequência de
    2 bytes válida, não um byte de controle solto).

    A cópia fica em `raw_utf8/`, espelhando o caminho — `raw/` nunca é tocado, mesma
    política do `staging/` de hoje: uma camada derivada e descartável.
    """
    corpo = minio.s3.get_object(Bucket=minio.bucket, Key=key)["Body"].read()
    utf8 = corpo.decode("latin-1").encode("utf-8")
    nova_chave = key.replace("raw/", "raw_utf8/", 1)
    try:
        existente = minio.s3.head_object(Bucket=minio.bucket, Key=nova_chave)
        if existente["ContentLength"] == len(utf8):
            return nova_chave  # já reescrito nesta rodada (idempotente)
    except Exception:
        pass
    minio.s3.put_object(Bucket=minio.bucket, Key=nova_chave, Body=utf8)
    return nova_chave


def familia(key: str) -> str:
    """Nome da família: o arquivo sem a data e sem os sufixos de colisão.

    CAIXA_AF_GEHIS_ANDAMENTO_OBRA_M20250612.TXT -> caixa_af_gehis_andamento_obra_m
    Os 219 arquivos dessa família viram UM model dbt.
    """
    nome = key.split("/")[-1]
    nome = nome.rsplit(".", 1)[0]
    nome = re.sub(r"\d{4}[-_]\d{2}[-_]\d{2}", "", nome)   # 2026-01-01
    nome = re.sub(r"\d{8}", "", nome)                      # 20250612
    nome = re.sub(r"\d{6}", "", nome)                      # 202506
    nome = re.sub(r"_\d{4}$", "", nome)                    # sufixo de colisão _0001
    nome = re.sub(r"\d+", "", nome)
    nome = re.sub(r"[^0-9a-zA-Z]+", "_", nome).strip("_").lower()
    return re.sub(r"_+", "_", nome)


def ext(key: str) -> str:
    pos = key.rfind(".")
    return key[pos:].lower() if pos != -1 else ""


def delimitador_dos_dados(linhas: list, candidatos=(";", "|", "\t", ",")) -> "str | None":
    """Delimitador que melhor explica as LINHAS DE DADOS (não o header).

    `lake_utils.detectar_dialeto` decide olhando o primeiro registro — o header. Existe no
    lake pelo menos um arquivo cujo header é separado por TAB e cujos dados são separados
    por PIPE (CAIXA_AF_GEHIS_ANDAMENTO_OBRA_M20230602). Nesse caso o delimitador do header
    produz uma única coluna nos dados e o arquivo inteiro vira lixo — silenciosamente, tanto
    no pandas quanto no DuckDB.

    Escolhe o candidato com maior nº de campos e contagem estável entre as linhas.
    """
    amostra = [l for l in linhas[1:21] if l.strip()]
    if not amostra:
        return None
    melhor, melhor_score = None, 1
    for cand in candidatos:
        contagens = [len(l.split(cand)) for l in amostra]
        if min(contagens) < 2:
            continue
        estavel = len(set(contagens)) == 1
        score = min(contagens) + (10 if estavel else 0)
        if score > melhor_score:
            melhor, melhor_score = cand, score
    return melhor


def criar_tabela(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABELA} (
                id             SERIAL PRIMARY KEY,
                minio_key      TEXT NOT NULL,
                familia        TEXT NOT NULL,
                formato        TEXT NOT NULL,
                encoding       TEXT,
                delimitador    TEXT,
                n_colunas      INTEGER,
                header         JSONB,
                header_ok      BOOLEAN DEFAULT TRUE,
                tem_bytes_c1   BOOLEAN DEFAULT FALSE,
                legivel_duckdb BOOLEAN DEFAULT TRUE,
                motivo         TEXT,
                source_hash    TEXT,
                bytes          BIGINT,
                criado_em      TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (minio_key, source_hash)
            )
        """)
        # CREATE TABLE IF NOT EXISTS não acrescenta colunas a uma tabela já existente.
        for coluna, tipo in (("header_ok", "BOOLEAN DEFAULT TRUE"),
                             ("tem_bytes_c1", "BOOLEAN DEFAULT FALSE"),
                             ("chave_leitura", "TEXT")):
            cur.execute(f"ALTER TABLE {SCHEMA}.{TABELA} "
                        f"ADD COLUMN IF NOT EXISTS {coluna} {tipo}")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABELA}_familia "
                    f"ON {SCHEMA}.{TABELA} (familia)")
        conn.commit()


def analisar(minio: ClienteMinio, key: str, tamanho: int) -> dict:
    """Detecta encoding/dialeto/header a partir de um sample de 64 KB (HTTP Range)."""
    reg: dict = {
        "minio_key": key, "familia": familia(key), "formato": ext(key).lstrip("."),
        "bytes": tamanho, "legivel_duckdb": True, "motivo": None, "header_ok": True,
        "encoding": None, "delimitador": None, "n_colunas": None,
        "header": None, "tem_bytes_c1": False, "chave_leitura": None,
        # ETag do MinIO como source_hash: já é o md5 do objeto para uploads simples e
        # muda quando o conteúdo muda — que é tudo o que a idempotência precisa.
        "source_hash": minio.s3.head_object(Bucket=minio.bucket, Key=key)["ETag"].strip('"'),
    }

    if ext(key) in EXCEL:
        # Planilha não tem encoding nem delimitador; o schema sai do próprio arquivo.
        reg["motivo"] = "xlsx: schema lido pelo read_xlsx"
        return reg

    if ext(key) not in TABULARES:
        reg["legivel_duckdb"] = False
        reg["motivo"] = f"formato {ext(key)!r} sem leitor no DuckDB"
        return reg

    sample = minio.sample_bytes(key, 65536)
    encoding = detectar_encoding_corrigido(sample)
    reg["encoding"] = encoding
    reg["tem_bytes_c1"] = any(b in C1 for b in sample)

    # Caso residual genuíno: nem UTF-8 nem cp1252 decodificam o arquivo (detectar_encoding
    # já teria caído em latin-1 mesmo tentando UTF-8 primeiro). O DuckDB não lê latin-1 com
    # bytes C1, então reescreve uma cópia em UTF-8 e aponta a leitura pra ela — ver
    # reescrever_utf8(). legivel_duckdb permanece True: a cópia resolve.
    if encoding == "latin-1" and reg["tem_bytes_c1"]:
        reg["chave_leitura"] = reescrever_utf8(minio, key)
        reg["motivo"] = (
            f"bytes no intervalo C1 fora de UTF-8 válido: reescrito para UTF-8 em "
            f"{reg['chave_leitura']} (mesmo mapeamento byte->código que o latin-1 já usa "
            f"hoje, só persistido num container que o DuckDB lê)"
        )

    dialeto = detectar_dialeto(sample, encoding)
    if not dialeto:
        reg["legivel_duckdb"] = False
        reg["motivo"] = reg["motivo"] or "não parece tabular (sem delimitador detectável)"
        return reg

    delim, _lineterm, _fq = dialeto
    linhas = [l.rstrip("\r") for l in sample.decode(encoding, errors="replace").split("\n")]

    # O delimitador do header pode não ser o dos dados. Quando divergem, o dos dados manda:
    # é ele que determina se as colunas serão separadas ou se o arquivo vira uma coluna só.
    delim_dados = delimitador_dos_dados(linhas)
    if delim_dados and delim_dados != delim and len(linhas[0].split(delim_dados)) == 1:
        reg["motivo"] = (
            f"header usa {delim!r} e os dados usam {delim_dados!r} — arquivo inconsistente "
            f"na origem; adotado o dos dados e o header descartado"
        )
        delim = delim_dados
        # O header não é utilizável como nome de coluna: separado pelo delimitador errado,
        # ele vira um campo só. O codegen dá a esse arquivo um grupo de leitura próprio,
        # com header=false e os nomes canônicos vindos do YAML da família.
        reg["header_ok"] = False

    reg["delimitador"] = delim
    header = [h.strip('"') for h in linhas[0].split(delim)]
    norm, _ = normalizar_colunas(header)

    # Há arquivos cujo header tem MENOS campos que as linhas de dados (delimitador faltando
    # no header). O DuckDB só lê esses arquivos com null_padding=true, e aí batiza os campos
    # excedentes de column{i} (índice 0-based). O manifesto precisa registrar esses nomes,
    # senão o codegen não sabe que eles existem e o dado excedente se perde — que é
    # exatamente o que o pandas faz hoje, em silêncio.
    max_campos = max(
        (len(l.rstrip("\r").split(delim)) for l in linhas[1:-1] if l.strip()),
        default=len(norm),
    )
    if max_campos > len(norm):
        norm = norm + [f"column{i}" for i in range(len(norm), max_campos)]
        reg["motivo"] = (
            f"header com {len(header)} campos e dados com {max_campos}: "
            f"exige null_padding (o pandas descarta os excedentes)"
        )

    reg["header"] = norm
    reg["n_colunas"] = len(norm)
    return reg


def gravar(conn, registros: list) -> None:
    import json

    with conn.cursor() as cur:
        for r in registros:
            cur.execute(f"""
                INSERT INTO {SCHEMA}.{TABELA}
                    (minio_key, familia, formato, encoding, delimitador, n_colunas,
                     header, header_ok, tem_bytes_c1, legivel_duckdb, motivo,
                     source_hash, bytes, chave_leitura)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (minio_key, source_hash) DO UPDATE SET
                    familia = EXCLUDED.familia, encoding = EXCLUDED.encoding,
                    delimitador = EXCLUDED.delimitador, n_colunas = EXCLUDED.n_colunas,
                    header = EXCLUDED.header, header_ok = EXCLUDED.header_ok,
                    tem_bytes_c1 = EXCLUDED.tem_bytes_c1,
                    legivel_duckdb = EXCLUDED.legivel_duckdb, motivo = EXCLUDED.motivo,
                    chave_leitura = EXCLUDED.chave_leitura
            """, (
                r["minio_key"], r["familia"], r["formato"], r["encoding"], r["delimitador"],
                r["n_colunas"], json.dumps(r["header"]) if r["header"] else None, r["header_ok"],
                r["tem_bytes_c1"], r["legivel_duckdb"], r["motivo"], r["source_hash"], r["bytes"],
                r["chave_leitura"],
            ))
        conn.commit()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--prefixo", default="raw/", help="Prefixo a varrer (default raw/).")
    p.add_argument("--pattern", default="", help="Filtra por substring na key.")
    args = p.parse_args()

    minio = ClienteMinio(
        endpoint=os.environ["POC_MINIO_ENDPOINT"],
        access_key=os.environ["POC_MINIO_ACCESS_KEY"],
        secret_key=os.environ["POC_MINIO_SECRET_KEY"],
        bucket=os.environ["POC_MINIO_BUCKET"],
    )
    conn = psycopg2.connect(
        host=os.environ["POC_PG_HOST"], port=os.environ["POC_PG_PORT"],
        user=os.environ["POC_PG_USER"], password=os.environ["POC_PG_PASSWORD"],
        dbname=os.environ["POC_PG_DBNAME"],
    )
    criar_tabela(conn)

    objetos = [(k, t) for k, t in minio.listar_objetos(args.prefixo)
               if args.pattern.lower() in k.lower()]
    print(f"{len(objetos)} objeto(s) em {args.prefixo}\n")

    registros = [analisar(minio, k, t) for k, t in objetos]
    gravar(conn, registros)

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT familia, count(*), count(DISTINCT encoding), count(DISTINCT delimitador),
                   count(DISTINCT n_colunas), bool_or(NOT legivel_duckdb)
            FROM {SCHEMA}.{TABELA} GROUP BY familia ORDER BY count(*) DESC
        """)
        print(f"{'familia':44s} {'arq':>4s} {'enc':>4s} {'del':>4s} {'ncol':>5s}  bloqueio")
        for fam, n, ne, nd, nc, bloq in cur.fetchall():
            print(f"{fam[:44]:44s} {n:4d} {ne:4d} {nd:4d} {nc:5d}  {'SIM' if bloq else '-'}")

        cur.execute(f"SELECT count(*) FROM {SCHEMA}.{TABELA} WHERE NOT legivel_duckdb")
        bloqueados = cur.fetchone()[0]
        cur.execute(f"SELECT minio_key, motivo FROM {SCHEMA}.{TABELA} WHERE NOT legivel_duckdb")
        if bloqueados:
            print(f"\n{bloqueados} arquivo(s) sem caminho no DuckDB:")
            for k, m in cur.fetchall():
                print(f"   {k.split('/')[-1][:56]:56s} {m}")
    conn.close()
    print(f"\nManifesto em {SCHEMA}.{TABELA}")


if __name__ == "__main__":
    main()
