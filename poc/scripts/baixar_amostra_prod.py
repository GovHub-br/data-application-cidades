#!/usr/bin/env python3
"""Baixa uma amostra PEQUENA e READ-ONLY de raw/ do lake real e semeia o MinIO da POC.

Guardas (não negociáveis):
  1. Só chama métodos de LEITURA do ClienteMinio no cliente de produção. Nenhuma escrita.
  2. Só copia objeto COMPROVADAMENTE analisado pelo mascaramento: ou tem a tag
     masked=true, ou está em sftp._masking_log com status skipped_no_pii (analisado e
     sem coluna de PII). Objeto sem nenhum dos dois nunca passou pelo mascaramento e
     não entra num MinIO local sem TLS.
  3. CSV/TXT são TRUNCADOS por HTTP Range (~2 MB, cortando no último \\n) — o arquivo
     resultante é sintaticamente válido e minúsculo. XLSX/XLS/MDB são containers binários
     que não podem ser truncados: só entram se forem menores que --max-binario-mb.
  4. Aborta se o endpoint de destino não for local.

Grava o GABARITO em resultados/amostra.json: encoding e dialeto segundo lake_utils, e o
header normalizado. É contra esse gabarito que o comportamento do DuckDB é comparado.
"""

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ.parent / "airflow_lappis" / "plugins"))
sys.path.insert(0, str(RAIZ.parent / "scripts"))

import psycopg2  # noqa: E402

from cliente_minio import ClienteMinio  # noqa: E402
from lake_utils import detectar_dialeto, detectar_encoding, normalizar_colunas  # noqa: E402

load_dotenv(RAIZ / ".env")
load_dotenv(RAIZ.parent / ".env")  # credenciais de PRODUÇÃO (só leitura)

DEST_DIR = RAIZ / "data" / "amostra_real"
GABARITO = RAIZ / "resultados" / "amostra.json"

TABULARES = {".csv", ".txt"}
BINARIOS = {".xlsx", ".xls", ".mdb", ".accdb"}


def cliente_prod() -> ClienteMinio:
    return ClienteMinio(
        endpoint=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        bucket=os.environ["MINIO_BUCKET"],
    )


def cliente_poc() -> ClienteMinio:
    endpoint = os.environ["POC_MINIO_ENDPOINT"]
    if not endpoint.startswith(("localhost", "127.0.0.1")):
        raise SystemExit(f"ABORTADO: POC_MINIO_ENDPOINT={endpoint!r} não é local.")
    return ClienteMinio(
        endpoint=endpoint,
        access_key=os.environ["POC_MINIO_ACCESS_KEY"],
        secret_key=os.environ["POC_MINIO_SECRET_KEY"],
        bucket=os.environ["POC_MINIO_BUCKET"],
    )


def ext(key: str) -> str:
    pos = key.rfind(".")
    return key[pos:].lower() if pos != -1 else ""


def keys_sem_pii() -> set:
    """minio_keys que o mascaramento ANALISOU e considerou sem PII.

    Junto com a tag masked=true, é o que caracteriza um objeto seguro para sair do lake.
    Objeto ausente das duas listas nunca passou pelo mascaramento.
    """
    conn = psycopg2.connect(
        host=os.environ["DB_DW_HOST_MCID"], port=os.environ.get("DB_DW_PORT_MCID", 5432),
        user=os.environ["DB_DW_USER_MCID"], password=os.environ["DB_DW_PASSWORD_MCID"],
        dbname=os.environ["DB_DW_DBNAME_MCID"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT minio_key FROM sftp._masking_log
                WHERE status = 'skipped_no_pii'
            """)
            return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


def escolher_familia(prod: ClienteMinio, padrao: str, limite: int, seguros: set) -> list:
    """Arquivos de UMA família (substring na key), para o protótipo de bronze por família.

    Diferente de `escolher`, não distribui por extensão: pega os menores da família, que é
    onde estão as variações de encoding/delimitador que o model precisa absorver.
    """
    objetos = [
        (k, t) for k, t in prod.listar_objetos("raw/")
        if padrao.lower() in k.lower() and ext(k) in TABULARES and k in seguros
    ]
    objetos.sort(key=lambda x: x[1])
    print(f"  família {padrao!r}: {len(objetos)} objeto(s) seguro(s) -> {min(limite, len(objetos))}")
    return objetos[:limite]


def escolher(prod: ClienteMinio, por_ext: int, max_binario: int, seguros: set) -> list:
    """Um punhado de objetos por extensão, preferindo os menores — só entre os SEGUROS.

    O filtro de segurança vem ANTES da escolha por tamanho: filtrar depois faria os
    menores (quase sempre não analisados) consumirem as vagas e a amostra sair vazia.
    """
    objetos = list(prod.listar_objetos("raw/"))
    print(f"  raw/ em produção: {len(objetos)} objetos")

    por_grupo: dict = {}
    for key, tamanho in objetos:
        e = ext(key)
        if (e in TABULARES or e in BINARIOS) and key in seguros:
            por_grupo.setdefault(e, []).append((key, tamanho))

    escolhidos = []
    for e, itens in sorted(por_grupo.items()):
        itens.sort(key=lambda x: x[1])
        if e in BINARIOS:
            itens = [i for i in itens if i[1] <= max_binario * 1024 * 1024]
        escolhidos.extend(itens[:por_ext])
        print(f"    {e:8s} {len(itens):5d} candidato(s) -> {min(por_ext, len(itens))} escolhido(s)")
    return escolhidos


def baixar(prod: ClienteMinio, key: str, tamanho: int, bytes_max: int) -> bytes:
    e = ext(key)
    if e in TABULARES:
        # Range: nunca puxa o arquivo inteiro (há objetos de vários GB em raw/)
        dados = prod.sample_bytes(key, bytes_max)
        corte = dados.rfind(b"\n")
        return dados[: corte + 1] if corte > 0 else dados
    caminho = prod.baixar_para_tempfile(key, suffix=e, tmpdir="/var/tmp")
    try:
        return Path(caminho).read_bytes()
    finally:
        os.unlink(caminho)


def gabarito(nome: str, dados: bytes) -> dict:
    info: dict = {"arquivo": nome, "bytes": len(dados)}
    if ext(nome) not in TABULARES:
        return info
    encoding = detectar_encoding(dados[:65536])
    info["encoding_lake_utils"] = encoding
    dialeto = detectar_dialeto(dados[:65536], encoding)
    if dialeto:
        delim, lineterm, fully_quoted = dialeto
        info["delimitador"] = delim
        info["lineterm"] = "\\r\\n" if lineterm == "\r\n" else "\\n"
        info["fully_quoted"] = fully_quoted
        header = dados.split(b"\n", 1)[0].decode(encoding, errors="replace").strip("\r").split(delim)
        header = [h.strip('"') for h in header]
        norm, _ = normalizar_colunas(header)
        info["header_original"] = header[:25]
        info["header_normalizado"] = norm[:25]
        info["n_colunas"] = len(header)
    return info


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="Sem isto, só lista o que faria.")
    p.add_argument("--por-ext", type=int, default=2, help="Objetos por extensão (default 2).")
    p.add_argument("--bytes-max", type=int, default=2 * 1024 * 1024,
                   help="Bytes lidos por CSV/TXT via Range (default 2 MB).")
    p.add_argument("--max-binario-mb", type=int, default=5,
                   help="Tamanho máximo de XLSX/XLS/MDB a copiar inteiro (default 5 MB).")
    p.add_argument("--familia", metavar="PADRAO",
                   help="Em vez de amostrar por extensão, baixa arquivos de UMA família "
                        "(substring na key). Ex.: CAIXA_AF_GEHIS_ANDAMENTO_OBRA")
    p.add_argument("--limit", type=int, default=8, help="Com --familia: quantos arquivos.")
    p.add_argument("--key", action="append", dest="keys", metavar="MINIO_KEY",
                   help="Baixa exatamente esta key (repetível). Útil para pegar as variantes "
                        "raras de encoding/delimitador de uma família.")
    args = p.parse_args()

    prod, poc = cliente_prod(), cliente_poc()
    if prod.s3.meta.endpoint_url == poc.s3.meta.endpoint_url:
        raise SystemExit("ABORTADO: origem e destino são o mesmo endpoint.")
    print(f"Origem  (READ-ONLY): {prod.s3.meta.endpoint_url}/{prod.bucket}")
    print(f"Destino            : {poc.s3.meta.endpoint_url}/{poc.bucket}\n")

    sem_pii = keys_sem_pii()
    print(f"  {len(sem_pii)} objetos marcados 'skipped_no_pii' em sftp._masking_log\n")

    if args.keys:
        tamanhos = {k: t for k, t in prod.listar_objetos("raw/")}
        escolhidos = [(k, tamanhos.get(k, 0)) for k in args.keys if k in tamanhos]
        faltando = [k for k in args.keys if k not in tamanhos]
        if faltando:
            print(f"  ⚠ não encontradas em raw/: {faltando}")
    elif args.familia:
        escolhidos = escolher_familia(prod, args.familia, args.limit, sem_pii)
    else:
        escolhidos = escolher(prod, args.por_ext, args.max_binario_mb, sem_pii)
    print(f"\n{len(escolhidos)} objeto(s) selecionado(s):")

    DEST_DIR.mkdir(parents=True, exist_ok=True)
    registros = []
    for key, tamanho in escolhidos:
        if key in sem_pii:
            motivo = "analisado, sem PII"
        elif prod.esta_mascarado(key):
            motivo = "mascarado (tag)"
        else:
            print(f"  ⊘ {key}  — não passou pelo mascaramento, pulando (política de PII)")
            continue
        nome = "amostra__" + key.split("/", 1)[-1].replace("/", "__")
        print(f"  → {key}  ({tamanho / 1e6:.1f} MB, {motivo})  ->  {nome}")
        if not args.apply:
            continue
        dados = baixar(prod, key, tamanho, args.bytes_max)
        destino = DEST_DIR / nome
        destino.write_bytes(dados)
        info = gabarito(nome, dados)
        info["key_origem"] = key
        info["bytes_origem"] = tamanho
        registros.append(info)
        print(f"      {len(dados) / 1e6:.2f} MB salvos | {info.get('encoding_lake_utils', '—')} "
              f"| delim={info.get('delimitador', '—')!r} | {info.get('n_colunas', '—')} colunas")

    if not args.apply:
        print("\n(dry-run — use --apply para baixar)")
        return

    GABARITO.parent.mkdir(exist_ok=True)
    GABARITO.write_text(json.dumps(registros, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nGabarito: {GABARITO}")
    print("Agora rode: .venv/bin/python scripts/semear_minio.py --dir amostra_real")


if __name__ == "__main__":
    main()
