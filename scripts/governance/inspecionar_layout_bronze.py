# scripts/governance/inspecionar_layout_bronze.py

"""
Lista as colunas de uma remessa da staging e imprime o bloco `sem_drift_de_colunas` pronto
para colar no schema.yml da bronze correspondente.

Serve para fechar o critério de documentação das bronzes novas (INT058, INT064, INT042 e as
que vierem): o contrato da bronze é a lista de colunas da origem, e essa lista só se conhece
lendo o arquivo.

SOMENTE LEITURA e SEM VALORES: imprime nomes de colunas, tipos e a contagem de linhas. Nunca
imprime conteúdo de células, porque as famílias (INT042, INT058, INT064) são dado pessoal.

Uso (na raiz do repo, com o .env do MinIO, na VPN):
    poetry run python scripts/governance/inspecionar_layout_bronze.py INT058_MinisterioCidades_PNHR_BB_PF
    poetry run python scripts/governance/inspecionar_layout_bronze.py INT064_MinisterioCidades_PNHR_CAIXA_PF
    poetry run python scripts/governance/inspecionar_layout_bronze.py INT042_MinisterioCidades_FDS_CAIXA_PF
    poetry run python scripts/governance/inspecionar_layout_bronze.py <trecho> --todas   # compara remessas

Credenciais: lidas do ambiente pelo `plugins/cliente_minio.py`. Nenhuma é impressa.
"""

import argparse
import io
import re
import sys
from pathlib import Path

try:
    import pyarrow.parquet as pq
except ImportError:  # ambiente sem pyarrow (ou com NumPy incompatível): cai para o DuckDB
    pq = None
from dotenv import load_dotenv

_plugins = Path(__file__).resolve().parents[2] / "plugins"
if _plugins.is_dir() and str(_plugins) not in sys.path:
    sys.path.insert(0, str(_plugins))

from cliente_minio import BUCKET, get_s3_client  # noqa: E402

load_dotenv()

EXCLUIR = ("VALIDACAO", "SUBSTITUIDO", "ALTERACAO_LAYOUT")


def listar(s3, trecho):
    achados = []
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="staging/"):
        for obj in pagina.get("Contents", []):
            chave = obj["Key"]
            nome = chave.rsplit("/", 1)[-1]
            if trecho in nome and nome.endswith(".parquet") and not any(x in nome for x in EXCLUIR):
                achados.append((chave, obj["Size"]))
    return sorted(achados, key=lambda c: c[0].rsplit("/", 1)[-1])


def schema(s3, chave):
    corpo = s3.get_object(Bucket=BUCKET, Key=chave)["Body"].read()
    if pq is not None:
        arq = pq.ParquetFile(io.BytesIO(corpo))
        return [(f.name, str(f.type)) for f in arq.schema_arrow], arq.metadata.num_rows
    import os
    import tempfile

    import duckdb

    with tempfile.TemporaryDirectory() as pasta:
        caminho = os.path.join(pasta, "remessa.parquet")
        with open(caminho, "wb") as f:
            f.write(corpo)
        con = duckdb.connect()
        cols = [(r[0], r[1]) for r in con.execute(f"describe select * from read_parquet('{caminho.replace(chr(92), '/')}')").fetchall()]
        linhas = con.execute(f"select count(*) from read_parquet('{caminho.replace(chr(92), '/')}')").fetchone()[0]
        con.close()
    return cols, linhas


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("trecho", help="trecho do nome do arquivo, ex.: INT058_MinisterioCidades_PNHR_BB_PF")
    ap.add_argument("--todas", action="store_true", help="compara o layout de todas as remessas")
    args = ap.parse_args()

    s3 = get_s3_client()
    arquivos = listar(s3, args.trecho)
    if not arquivos:
        print(f"nenhum parquet na staging com '{args.trecho}' no nome", file=sys.stderr)
        return 1
    print(f"{len(arquivos)} remessas; mais antiga: {arquivos[0][0].rsplit('/', 1)[-1]}")
    print(f"mais recente: {arquivos[-1][0].rsplit('/', 1)[-1]}")

    cols, linhas = schema(s3, arquivos[-1][0])
    print(f"\nLayout da remessa mais recente: {len(cols)} colunas, {linhas} linhas")
    for nome, tipo in cols:
        print(f"  {nome:<45} {tipo}")

    if args.todas:
        base = {n for n, _ in cols}
        print("\nDiferenças de layout por remessa (colunas que entram/saem em relação à mais recente):")
        for chave, _ in arquivos[:-1]:
            outras, _n = schema(s3, chave)
            nomes = {n for n, _ in outras}
            if nomes != base:
                print(f"  {chave.rsplit('/', 1)[-1]}: +{sorted(nomes - base)} -{sorted(base - nomes)}")

    print("\n# Cole no schema.yml, dentro da entrada da bronze:")
    print("    data_tests:")
    print("      - sem_drift_de_colunas:")
    print("          arguments:")
    print("            colunas:")
    for nome, _ in cols:
        if not re.match(r"^\s*$", nome):
            print(f"              - {nome}")
    # a bronze lê com filename => true, então a coluna existe na tabela sem estar no parquet
    if "filename" not in {n for n, _ in cols}:
        print("              - filename")
    return 0


if __name__ == "__main__":
    sys.exit(main())
