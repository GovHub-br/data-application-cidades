"""Leva o lake e as tabelas de controle para a raiz da SNH (`snh/`).

O pipeline passou a gravar embaixo de `snh/` (`snh/raw/`, `snh/staging/`...; ver
`LAKE_ROOT` em plugins/datalake/keys.py). Este script move o que já estava gravado sem a
raiz, para as bronzes e as tabelas de controle continuarem achando tudo:

1. copia, no próprio servidor, cada objeto do bucket que não está embaixo de `snh/` para
   `snh/<chave>`, com o metadado (o `md5` das APIs) e as tags (a liberação da máscara,
   que o raw_para_staging lê);
2. só se todas as cópias deram certo, acrescenta `snh/` às chaves de
   `lake._ingest_minio_log`, `lake._masking_log` e `lake._staging_log`, numa transação.

Não apaga os originais: confira as bronzes e apague a raiz antiga à mão depois.
Sem `--apply`, só conta o que faria. Rodar de novo é seguro (pula a cópia que já existe
com o mesmo tamanho e só atualiza chave que ainda não começa com `snh/`).

    uv run python scripts/migrar_prefixo_snh.py            # .env com DL_* e DB_DW_*_MCID
    uv run python scripts/migrar_prefixo_snh.py --apply
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from pathlib import Path

import boto3
import psycopg2
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))
from datalake.keys import LAKE_ROOT  # noqa: E402

# tabela -> colunas que guardam chave do lake
CONTROL_KEYS = {
    "_ingest_minio_log": ("minio_key",),
    "_masking_log": ("minio_key",),
    "_staging_log": ("raw_key", "staging_key"),
}


def _s3():
    endpoint = os.environ["DL_ENDPOINT"]
    return boto3.client(
        "s3",
        endpoint_url=endpoint if "://" in endpoint else f"http://{endpoint}",
        aws_access_key_id=os.environ["DL_ACCESS_KEY"],
        aws_secret_access_key=os.environ["DL_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _size(s3, bucket: str, key: str):
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def copy_objects(s3, bucket: str, apply: bool) -> Counter:
    counts = Counter()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.startswith(LAKE_ROOT):
                continue
            dest = LAKE_ROOT + key
            counts[key.split("/", 1)[0] + "/"] += 1
            if not apply:
                continue
            if _size(s3, bucket, dest) == obj["Size"]:
                counts["ja_copiado"] += 1
                continue
            # copia gerenciada: objetos acima de 5 GB vão por partes; metadado e tags à mão,
            # porque a cópia por partes não os leva
            head = s3.head_object(Bucket=bucket, Key=key)
            s3.copy(
                {"Bucket": bucket, "Key": key},
                bucket,
                dest,
                ExtraArgs={"Metadata": head.get("Metadata", {}), "MetadataDirective": "REPLACE"},
            )
            tags = s3.get_object_tagging(Bucket=bucket, Key=key)["TagSet"]
            if tags:
                s3.put_object_tagging(Bucket=bucket, Key=dest, Tagging={"TagSet": tags})
            if _size(s3, bucket, dest) != obj["Size"]:
                raise RuntimeError(f"cópia de {key} ficou com tamanho diferente")
            counts["copiado"] += 1
    return counts


def update_control(conn, apply: bool) -> Counter:
    counts = Counter()
    with conn.cursor() as cur:
        for table, columns in CONTROL_KEYS.items():
            for column in columns:
                where = f"{column} IS NOT NULL AND {column} NOT LIKE %s"
                params = (LAKE_ROOT + "%",)
                if apply:
                    cur.execute(
                        f"UPDATE lake.{table} SET {column} = %s || {column} WHERE {where}",
                        (LAKE_ROOT, *params),
                    )
                    counts[f"{table}.{column}"] = cur.rowcount
                else:
                    cur.execute(f"SELECT count(*) FROM lake.{table} WHERE {where}", params)
                    counts[f"{table}.{column}"] = cur.fetchone()[0]
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="copia e atualiza (sem: só conta)")
    args = parser.parse_args()
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    bucket = os.environ.get("DL_BUCKET", "data-lake-mcid")

    print(f"objetos fora de {LAKE_ROOT} em {bucket}:")
    for name, n in sorted(copy_objects(_s3(), bucket, args.apply).items()):
        print(f"  {name}: {n}")

    conn = psycopg2.connect(
        host=os.environ["DB_DW_HOST_MCID"],
        port=int(os.environ.get("DB_DW_PORT_MCID", 5432)),
        user=os.environ["DB_DW_USER_MCID"],
        password=os.environ["DB_DW_PASSWORD_MCID"],
        dbname=os.environ["DB_DW_DBNAME_MCID"],
    )
    with conn:  # uma transação: ou todas as chaves mudam, ou nenhuma
        print("chaves sem a raiz nas tabelas de controle:")
        for name, n in update_control(conn, args.apply).items():
            print(f"  {name}: {n}")
    conn.close()
    if not args.apply:
        print("dry-run: nada foi copiado nem alterado (use --apply)")


if __name__ == "__main__":
    main()
