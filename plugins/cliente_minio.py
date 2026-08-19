"""Cliente MinIO (S3-compatível) para o data lake do conjuntura contínuo.

Camadas no bucket `data-lake-mcid` (full-refresh, sem partição de data):
    raw/<fonte>/<dado>.json        -> payload cru da API (Extração p/ MinIO)
    staging/<fonte>/<dado>.parquet -> parquet JÁ TIPADO (Transformação)

A carga do parquet para o Postgres (silver) NÃO passa por aqui: é o pg_duckdb,
via `select * from read_parquet('s3://...')` nos modelos dbt.

Usa `boto3` (já presente no ambiente) apontando para o endpoint do MinIO. As
credenciais vêm das envs MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY /
MINIO_BUCKET.
"""

import json
import logging
import os
from typing import Any

import boto3

BUCKET = os.environ.get("MINIO_BUCKET", "data-lake-mcid")


def get_s3_client() -> Any:
    """Cria um client S3 (boto3) apontando para o MinIO."""
    endpoint = os.environ["MINIO_ENDPOINT"]
    if not endpoint.startswith(("http://", "https://")):
        endpoint = f"http://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
    )


def raw_key(fonte: str, dado: str, ext: str = "json") -> str:
    """Chave do objeto raw: raw/<fonte>/<dado>.<ext> (json/csv/txt/xlsx)."""
    return f"raw/{fonte}/{dado}.{ext}"


def fallback_json_key(fonte: str, dado: str) -> str:
    """Chave do json de conveniência: raw/<fonte>/fallback_json/<dado>.json.

    Usado quando o raw nativo NÃO é json (xlsx/csv): guarda-se também os
    registros já transformados em json, dentro de fallback_json/.
    """
    return f"raw/{fonte}/fallback_json/{dado}.json"


def staging_key(fonte: str, dado: str) -> str:
    """Chave do objeto staging: staging/<fonte>/<dado>.parquet."""
    return f"staging/{fonte}/{dado}.parquet"


def s3_staging_uri(fonte: str, dado: str) -> str:
    """URI s3:// do parquet de staging (para uso no read_parquet do pg_duckdb)."""
    return f"s3://{BUCKET}/{staging_key(fonte, dado)}"


def upload_raw_bytes(
    fonte: str,
    dado: str,
    data: bytes,
    ext: str,
    content_type: str = "application/octet-stream",
) -> None:
    """Etapa 01: sobe um arquivo cru (csv/txt/xlsx/...) para a raw (full-refresh)."""
    key = raw_key(fonte, dado, ext)
    get_s3_client().put_object(
        Bucket=BUCKET, Key=key, Body=data, ContentType=content_type
    )
    logging.info(f"[cliente_minio] raw enviado: s3://{BUCKET}/{key} ({len(data)} bytes)")


def download_raw_bytes(fonte: str, dado: str, ext: str = "json") -> bytes:
    """Lê os bytes crus do objeto raw do MinIO."""
    key = raw_key(fonte, dado, ext)
    obj = get_s3_client().get_object(Bucket=BUCKET, Key=key)
    body: bytes = obj["Body"].read()
    return body


def upload_raw_json(fonte: str, dado: str, payload: Any) -> None:
    """Etapa 01: sobe o payload cru da API como json (full-refresh).

    Usa default=str para tolerar tipos não serializáveis (numpy, pandas
    Timestamp, Decimal) que aparecem quando os registros vêm de um DataFrame.
    """
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    upload_raw_bytes(fonte, dado, body, ext="json", content_type="application/json")


def download_raw_json(fonte: str, dado: str) -> Any:
    """Lê de volta o json raw do MinIO (entrada do passo de tipagem/parquet)."""
    return json.loads(download_raw_bytes(fonte, dado, ext="json").decode("utf-8"))


def upload_fallback_json(fonte: str, dado: str, payload: Any) -> None:
    """Sobe os registros transformados como json em raw/<fonte>/fallback_json/.

    Complementa o raw nativo (xlsx/csv) com um json de conveniência.
    """
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    key = fallback_json_key(fonte, dado)
    get_s3_client().put_object(
        Bucket=BUCKET, Key=key, Body=body, ContentType="application/json"
    )
    logging.info(
        f"[cliente_minio] fallback_json enviado: s3://{BUCKET}/{key} "
        f"({len(body)} bytes)"
    )


def upload_staging_parquet(fonte: str, dado: str, data: bytes) -> None:
    """1.2 Transformação: sobe o parquet TIPADO para a staging (full-refresh)."""
    key = staging_key(fonte, dado)
    get_s3_client().put_object(
        Bucket=BUCKET, Key=key, Body=data, ContentType="application/octet-stream"
    )
    logging.info(
        f"[cliente_minio] staging enviado: s3://{BUCKET}/{key} ({len(data)} bytes)"
    )
