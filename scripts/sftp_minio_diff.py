"""
sftp_minio_diff.py: compara a arvore do SFTP com um prefixo no MinIO.

Uso:
    python scripts/sftp_minio_diff.py --limit 50
    python scripts/sftp_minio_diff.py --json diff_sftp_minio.json

Variaveis esperadas em .env ou local.env:
    SFTP_HOST
    SFTP_PORT
    SFTP_USER
    SFTP_PASSWORD
    MINIO_ENDPOINT
    MINIO_ACCESS_KEY
    MINIO_SECRET_KEY
    MINIO_BUCKET
    MINIO_PREFIX
"""

import argparse
import json
import logging
import os
import stat
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import boto3
import paramiko
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).parent.parent
load_dotenv(ROOT_DIR / ".env")
load_dotenv(ROOT_DIR / "local.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
log = logging.getLogger(__name__)

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT") or 22)
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET") or "data-lake-mcid"
MINIO_PREFIX = os.getenv("MINIO_PREFIX") or "raw/"
SUPPORTED_IN_ZIP_EXTENSIONS = {".csv", ".txt", ".xlsx"}

IGNORED_EXTENSIONS = {
    ".bashrc",
    ".bash_logout",
    ".bash_history",
    ".profile",
    ".mkshrc",
    ".viminfo",
    ".filepart",
    ".rpt",
}


def format_size(size_bytes: int) -> str:
    value: float = size_bytes
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PB"


def format_mtime(epoch: int | None) -> str:
    if not epoch:
        return "?"
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M")


def normalize_path(path: str) -> str:
    return path.removeprefix("./").lstrip("/")


def comparison_key(path: str, match_mode: str) -> str:
    normalized = normalize_path(path)
    if match_mode == "filename":
        return Path(normalized).name
    return normalized


def ensure_endpoint_url(endpoint: str | None) -> str:
    if not endpoint:
        raise ValueError("MINIO_ENDPOINT nao encontrado em .env/local.env")
    if endpoint.startswith(("http://", "https://")):
        return endpoint.rstrip("/")
    return f"http://{endpoint.rstrip('/')}"


def connect_sftp() -> tuple[paramiko.Transport, paramiko.SFTPClient]:
    if not SFTP_HOST or not SFTP_USER or not SFTP_PASSWORD:
        raise ValueError("Credenciais SFTP incompletas em .env/local.env")
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
    transport.set_keepalive(30)
    sftp = paramiko.SFTPClient.from_transport(transport)
    if sftp is None:
        raise RuntimeError("Falha ao abrir sessao SFTP")
    return transport, sftp


def connect_minio(endpoint: str):
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise ValueError("Credenciais MinIO incompletas em .env/local.env")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def capture_snapshot(files: dict[str, dict], source: str, host: str | None) -> dict:
    total_size = sum(f["size"] for f in files.values())
    log.info("%s: %d arquivos | %s", source, len(files), format_size(total_size))
    return {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "host": host,
        "file_count": len(files),
        "total_size": total_size,
        "files": files,
    }


def zipinfo_mtime(info: zipfile.ZipInfo) -> int | None:
    try:
        return int(datetime(*info.date_time).timestamp())
    except (TypeError, ValueError):
        return None


def add_file(
    files: dict[str, dict],
    path: str,
    match_mode: str,
    size: int,
    mtime: int | None,
    **extra: str,
) -> None:
    key = comparison_key(path, match_mode)
    files[key] = {
        "size": size,
        "mtime": mtime,
        "original_path": normalize_path(path),
        **extra,
    }


def add_zip_members(
    sftp: paramiko.SFTPClient,
    files: dict[str, dict],
    remote_path: str,
    relative_path: str,
    match_mode: str,
    zip_size: int,
    zip_mtime: int | None,
) -> None:
    try:
        remote_file = sftp.open(remote_path, "rb")
    except Exception as exc:
        log.error("erro ao abrir zip %s: %s", remote_path, exc)
        return

    try:
        with remote_file:
            with zipfile.ZipFile(remote_file) as zf:
                inner_files = [
                    info
                    for info in zf.infolist()
                    if not info.is_dir()
                    and Path(info.filename).suffix.lower()
                    in SUPPORTED_IN_ZIP_EXTENSIONS
                    and not Path(info.filename).name.startswith(".")
                ]
                if not inner_files:
                    log.warning("zip sem arquivos suportados: %s", relative_path)
                    return

                for info in inner_files:
                    inner_path = normalize_path(info.filename)
                    compare_path = inner_path if match_mode == "filename" else (
                        f"{relative_path}::{inner_path}"
                    )
                    add_file(
                        files,
                        compare_path,
                        match_mode,
                        info.file_size,
                        zipinfo_mtime(info),
                        container_path=normalize_path(relative_path),
                        inner_path=inner_path,
                    )
    except zipfile.BadZipFile as exc:
        log.error("zip invalido %s: %s", relative_path, exc)
        add_file(
            files,
            relative_path,
            match_mode,
            zip_size,
            zip_mtime,
            zip_error="BadZipFile",
        )
    except Exception as exc:
        log.error("erro ao listar conteudo do zip %s: %s", relative_path, exc)


def capture_sftp_snapshot(root: str, match_mode: str, expand_zips: bool) -> dict:
    log.info("Coletando arvore do SFTP em %s...", root)
    transport, sftp = connect_sftp()
    files: dict[str, dict] = {}

    def walk(path: str, relative_base: str = "") -> None:
        try:
            items = sftp.listdir_attr(path)
        except Exception as exc:
            log.error("erro ao listar %s: %s", path, exc)
            return

        for item in items:
            remote_path = f"{path.rstrip('/')}/{item.filename}"
            relative_path = f"{relative_base}/{item.filename}".lstrip("/")
            ext = Path(item.filename).suffix.lower()

            if item.st_mode is not None and stat.S_ISDIR(item.st_mode):
                walk(remote_path, relative_path)
            elif ext in IGNORED_EXTENSIONS or item.filename.startswith("."):
                continue
            elif ext == ".zip" and expand_zips:
                add_zip_members(
                    sftp,
                    files,
                    remote_path,
                    relative_path,
                    match_mode,
                    int(item.st_size or 0),
                    int(item.st_mtime) if item.st_mtime else None,
                )
            else:
                add_file(
                    files,
                    relative_path,
                    match_mode,
                    int(item.st_size or 0),
                    int(item.st_mtime) if item.st_mtime else None,
                )

    try:
        walk(root)
    finally:
        sftp.close()
        transport.close()

    return capture_snapshot(files, "sftp", SFTP_HOST)


def capture_minio_snapshot(
    endpoint: str, bucket: str, prefix: str, match_mode: str
) -> dict:
    log.info("Coletando arvore do MinIO: bucket=%s prefix=%s...", bucket, prefix)
    client = connect_minio(endpoint)
    paginator = client.get_paginator("list_objects_v2")
    files: dict[str, dict] = {}
    normalized_prefix = prefix.lstrip("/")

    for page in paginator.paginate(Bucket=bucket, Prefix=normalized_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            path_key = key.removeprefix(normalized_prefix).lstrip("/")
            if not path_key:
                continue
            key = comparison_key(path_key, match_mode)
            files[key] = {
                "size": int(obj.get("Size") or 0),
                "mtime": int(obj["LastModified"].timestamp())
                if obj.get("LastModified")
                else None,
                "original_path": normalize_path(path_key),
            }

    return capture_snapshot(files, "minio", endpoint)


def diff_snapshots(old: dict, new: dict, compare_mtime: bool = False) -> dict:
    old_files: dict[str, dict] = old["files"]
    new_files: dict[str, dict] = new["files"]

    added = {p: m for p, m in new_files.items() if p not in old_files}
    removed = {p: m for p, m in old_files.items() if p not in new_files}

    modified: list[dict] = []
    unchanged = 0
    for path in old_files.keys() & new_files.keys():
        old_meta = old_files[path]
        new_meta = new_files[path]
        size_changed = old_meta["size"] != new_meta["size"]
        mtime_changed = old_meta["mtime"] != new_meta["mtime"]

        if size_changed or (compare_mtime and mtime_changed):
            modified.append(
                {
                    "path": path,
                    "old_size": old_meta["size"],
                    "new_size": new_meta["size"],
                    "old_mtime": old_meta["mtime"],
                    "new_mtime": new_meta["mtime"],
                }
            )
        else:
            unchanged += 1

    return {
        "old_source": old.get("source"),
        "new_source": new.get("source"),
        "old_captured_at": old.get("captured_at"),
        "new_captured_at": new.get("captured_at"),
        "added": [{"path": p, **m} for p, m in sorted(added.items())],
        "removed": [{"path": p, **m} for p, m in sorted(removed.items())],
        "modified": sorted(modified, key=lambda x: x["path"]),
        "unchanged": unchanged,
    }


def print_report(diff: dict, limit: int | None) -> None:
    def section(title: str, items: list, fmt) -> None:
        print(f"\n## {title} ({len(items)})")
        shown = items if limit is None else items[:limit]
        for item in shown:
            print("  " + fmt(item))
        if limit is not None and len(items) > limit:
            print(f"  ... e mais {len(items) - limit}")

    print("=" * 70)
    print(
        f"DIFF  {diff['old_source']}:{diff['old_captured_at']}  ->  "
        f"{diff['new_source']}:{diff['new_captured_at']}"
    )
    print("=" * 70)
    print(
        f"so_no_minio={len(diff['added'])}  "
        f"diferentes={len(diff['modified'])}  "
        f"so_no_sftp={len(diff['removed'])}  "
        f"iguais={diff['unchanged']}"
    )

    section(
        "SO NO MINIO",
        diff["added"],
        lambda x: f"+ {x['path']}  ({format_size(x['size'])}, {format_mtime(x['mtime'])})",
    )
    section(
        "DIFERENTES",
        diff["modified"],
        lambda x: (
            f"~ {x['path']}  "
            f"{format_size(x['old_size'])}->{format_size(x['new_size'])}  "
            f"mtime {format_mtime(x['old_mtime'])}->{format_mtime(x['new_mtime'])}"
        ),
    )
    section(
        "SO NO SFTP",
        diff["removed"],
        lambda x: f"- {x['path']}  ({format_size(x['size'])}, {format_mtime(x['mtime'])})",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compara arquivos do SFTP com MinIO.")
    parser.add_argument("--sftp-root", default=".", help="pasta raiz no SFTP")
    parser.add_argument("--minio-endpoint", default=MINIO_ENDPOINT)
    parser.add_argument("--minio-bucket", default=MINIO_BUCKET)
    parser.add_argument("--minio-prefix", default=MINIO_PREFIX)
    parser.add_argument(
        "--match-mode",
        choices=["path", "filename"],
        default="filename",
        help="path compara caminho relativo; filename compara so o nome do arquivo",
    )
    parser.add_argument(
        "--no-expand-zips",
        action="store_true",
        help="compara arquivos .zip como unidade, sem listar conteudo interno",
    )
    parser.add_argument("--compare-mtime", action="store_true")
    parser.add_argument("--json", metavar="ARQUIVO.json", help="grava o diff em JSON")
    parser.add_argument("--limit", type=int, default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    endpoint = ensure_endpoint_url(args.minio_endpoint)

    sftp_snapshot = capture_sftp_snapshot(
        args.sftp_root,
        args.match_mode,
        expand_zips=not args.no_expand_zips,
    )
    minio_snapshot = capture_minio_snapshot(
        endpoint,
        args.minio_bucket,
        args.minio_prefix,
        args.match_mode,
    )
    diff = diff_snapshots(
        sftp_snapshot,
        minio_snapshot,
        compare_mtime=args.compare_mtime,
    )
    print_report(diff, args.limit)

    if args.json:
        Path(args.json).write_text(
            json.dumps(diff, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info("Diff em JSON: %s", args.json)


if __name__ == "__main__":
    main()
