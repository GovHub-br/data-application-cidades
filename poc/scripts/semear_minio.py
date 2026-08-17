#!/usr/bin/env python3
"""Sobe os arquivos locais da POC para raw/ no MinIO DA POC.

Reusa o ClienteMinio de airflow_lappis/plugins/ (mesmo padrão de sys.path dos scripts
do pipeline) — a POC não reimplementa cliente de S3.

GUARDA: aborta se o endpoint alvo não for o da POC. As credenciais vêm das POC_MINIO_*,
nunca das MINIO_* do .env da raiz (que apontam para produção).
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

_RAIZ = Path(__file__).resolve().parents[2]
_PLUGINS = _RAIZ / "airflow_lappis" / "plugins"
if str(_PLUGINS) not in sys.path:
    sys.path.insert(0, str(_PLUGINS))

from cliente_minio import ClienteMinio  # noqa: E402

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

ENDPOINT = os.environ["POC_MINIO_ENDPOINT"]
BUCKET = os.environ["POC_MINIO_BUCKET"]

# O MinIO da POC é sempre local. Se isto não bater, alguém apontou a POC para o lake real.
if not ENDPOINT.startswith(("localhost", "127.0.0.1")):
    raise SystemExit(f"ABORTADO: POC_MINIO_ENDPOINT={ENDPOINT!r} não é local.")


def cliente() -> ClienteMinio:
    return ClienteMinio(
        endpoint=ENDPOINT,
        access_key=os.environ["POC_MINIO_ACCESS_KEY"],
        secret_key=os.environ["POC_MINIO_SECRET_KEY"],
        bucket=BUCKET,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dir", default="sintetico",
        help="Subdiretório de poc/data/ a subir (sintetico | amostra_real)",
    )
    args = parser.parse_args()

    origem = Path(__file__).resolve().parents[1] / "data" / args.dir
    if not origem.is_dir():
        raise SystemExit(f"Diretório não encontrado: {origem}")

    minio = cliente()
    minio.garantir_bucket()

    arquivos = sorted(p for p in origem.iterdir() if p.is_file())
    if not arquivos:
        raise SystemExit(f"Nenhum arquivo em {origem}")

    for path in arquivos:
        key = minio.upload_arquivo(str(path), f"raw/{path.name}")
        print(f"  ✓ {path.name:38s} -> s3://{BUCKET}/{key}  ({path.stat().st_size / 1e6:.2f} MB)")

    print(f"\n{len(arquivos)} arquivo(s) em s3://{BUCKET}/raw/")


if __name__ == "__main__":
    main()
