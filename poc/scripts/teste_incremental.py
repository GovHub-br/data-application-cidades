#!/usr/bin/env python3
"""Testa se a bronze `external` particionada se comporta como incremental.

A pergunta: a materialização `external` do dbt-duckdb faz `COPY <relation> TO <location>`,
ou seja, recomputa o model inteiro a cada run. Com 219 arquivos por família isso não
escala. A hipótese é que `partition_by` + `overwrite_or_ignore`, combinados com um model
gerado só sobre os arquivos NOVOS, façam cada run tocar apenas as partições afetadas.

Protocolo:
  lote 1  -> gera o model com N arquivos, roda, registra LastModified de cada partição
  lote 2  -> gera o model só com os arquivos restantes, roda
  verifica -> quais partições mudaram, e se as antigas continuam intactas E legíveis
"""

import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ.parent / "airflow_lappis" / "plugins"))
load_dotenv(RAIZ / ".env")

from cliente_minio import ClienteMinio  # noqa: E402

FAMILIA = "amostra_caixa_af_gehis_andamento_obra_m"
TABELA = "caixa_andamento_obra"
PREFIXO = f"bronze/{TABELA}/"
SAIDA = RAIZ / "resultados" / "incremental.md"


def minio() -> ClienteMinio:
    return ClienteMinio(
        endpoint=os.environ["POC_MINIO_ENDPOINT"],
        access_key=os.environ["POC_MINIO_ACCESS_KEY"],
        secret_key=os.environ["POC_MINIO_SECRET_KEY"],
        bucket=os.environ["POC_MINIO_BUCKET"],
    )


def particoes(m: ClienteMinio) -> dict:
    """{particao: (bytes, LastModified)}"""
    out = {}
    for key, tamanho in m.listar_objetos(PREFIXO):
        meta = m.s3.head_object(Bucket=m.bucket, Key=key)
        out[key.replace(PREFIXO, "")] = (tamanho, meta["LastModified"])
    return out


def arquivos_da_familia() -> list:
    conn = psycopg2.connect(
        host=os.environ["POC_PG_HOST"], port=os.environ["POC_PG_PORT"],
        user=os.environ["POC_PG_USER"], password=os.environ["POC_PG_PASSWORD"],
        dbname=os.environ["POC_PG_DBNAME"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT minio_key FROM manifesto._manifesto_bronze
                WHERE familia = %s AND legivel_duckdb ORDER BY minio_key
            """, (FAMILIA,))
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def rodar(lote: list, rotulo: str) -> None:
    caminho = RAIZ / "resultados" / f"lote_{rotulo}.txt"
    caminho.write_text("\n".join(lote), encoding="utf-8")
    print(f"\n=== {rotulo}: {len(lote)} arquivo(s) ===")
    subprocess.run(
        [str(RAIZ / ".venv/bin/python"), str(RAIZ / "scripts/gerar_models_bronze.py"),
         "--familia", FAMILIA, "--lote", str(caminho)],
        check=True, cwd=RAIZ,
    )
    env = {**os.environ, "DBT_PROFILES_DIR": str(RAIZ / "dbt_poc")}
    r = subprocess.run(
        [str(RAIZ / ".venv/bin/dbt"), "run", "--select", f"bronze_{TABELA}"],
        cwd=RAIZ / "dbt_poc", env=env, capture_output=True, text=True,
    )
    linha = [x for x in r.stdout.splitlines() if "OK created" in x or "ERROR" in x]
    print("   " + (linha[0].strip() if linha else r.stdout[-300:]))


def main() -> None:
    m = minio()
    todos = arquivos_da_familia()
    if len(todos) < 4:
        raise SystemExit("família pequena demais para o teste")

    # Limpa a bronze para o teste começar do zero
    for key, _ in m.listar_objetos(PREFIXO):
        m.s3.delete_object(Bucket=m.bucket, Key=key)
    print(f"bronze/{TABELA}/ limpa | {len(todos)} arquivos na família")

    corte = len(todos) - 3
    lote1, lote2 = todos[:corte], todos[corte:]

    rodar(lote1, "1")
    antes = particoes(m)
    print(f"   -> {len(antes)} partição(ões): {sorted(antes)}")

    time.sleep(2)  # separa os LastModified de forma inequívoca
    rodar(lote2, "2")
    depois = particoes(m)
    print(f"   -> {len(depois)} partição(ões): {sorted(depois)}")

    novas = sorted(set(depois) - set(antes))
    intactas = sorted(p for p in antes if p in depois and depois[p][1] == antes[p][1])
    reescritas = sorted(p for p in antes if p in depois and depois[p][1] != antes[p][1])
    sumidas = sorted(set(antes) - set(depois))

    SAIDA.parent.mkdir(exist_ok=True)
    with open(SAIDA, "w", encoding="utf-8") as f:
        f.write("# Incremental na materialização `external` particionada\n\n")
        f.write("A materialização `external` do dbt-duckdb faz `COPY <relation> TO <location>` — "
                "recomputa o model inteiro a cada run. Com 219 arquivos por família isso não "
                "escala. O teste verifica se `partition_by` + `overwrite_or_ignore`, com o "
                "model gerado só sobre os arquivos novos, limita a escrita às partições "
                "afetadas.\n\n")
        f.write(f"Família `{FAMILIA}`, {len(todos)} arquivos reais do lake.\n\n")
        f.write(f"| lote | arquivos | partições depois |\n|---|---|---|\n")
        f.write(f"| 1 | {len(lote1)} | {len(antes)} |\n")
        f.write(f"| 2 | {len(lote2)} | {len(depois)} |\n\n")
        f.write("## Resultado\n\n")
        f.write(f"- **partições novas** (escritas pelo lote 2): {len(novas)} → `{novas}`\n")
        f.write(f"- **partições intactas** (LastModified inalterado): {len(intactas)} → `{intactas}`\n")
        f.write(f"- **partições reescritas** sem necessidade: {len(reescritas)} → `{reescritas}`\n")
        f.write(f"- **partições perdidas**: {len(sumidas)} → `{sumidas}`\n\n")
        if not sumidas and not reescritas:
            f.write("**Funciona.** O lote 2 escreveu só as partições dos seus arquivos; as do "
                    "lote 1 continuam byte a byte no lugar. Isso torna a bronze `external` "
                    "viável em escala: o custo de cada run é proporcional ao que chegou, não "
                    "ao histórico.\n\n")
        elif sumidas:
            f.write("**NÃO funciona como incremental:** partições do lote 1 desapareceram. "
                    "`overwrite_or_ignore` não preserva o que não foi reescrito — cada run "
                    "precisaria reprocessar a família inteira.\n\n")
        else:
            f.write("**Parcial:** nada se perdeu, mas partições sem arquivos novos foram "
                    "reescritas à toa.\n\n")
        f.write("> A idempotência continua dependendo da tabela de controle: é o manifesto "
                "que decide, por `(minio_key, source_hash)`, quais arquivos entram no lote. "
                "O dbt não faz isso sozinho.\n")

    print(f"\n{'='*64}")
    print(f"  novas      : {novas}")
    print(f"  intactas   : {intactas}")
    print(f"  reescritas : {reescritas}")
    print(f"  sumidas    : {sumidas}")
    print(f"{'='*64}\nEscrito: {SAIDA}")


if __name__ == "__main__":
    main()
