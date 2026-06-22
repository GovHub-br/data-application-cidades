"""
sftp_snapshot.py: captura snapshots do estado do SFTP e gera diffs estilo git.

Objetivo: entender COMO o SFTP é atualizado entre uma carga e outra
(os arquivos são sobrescritos? os antigos são movidos para outra pasta?).
Para isso gravamos uma "fotografia" da árvore de arquivos (path, tamanho, mtime)
e, na próxima vez, comparamos o estado atual contra essa fotografia.

Uso:
    # grava a fotografia do estado atual (rode logo após a 1ª ingestão)
    python sftp_snapshot.py snapshot

    # compara o SFTP atual contra o último snapshot gravado
    python sftp_snapshot.py diff

    # idem, mas também grava o estado atual como novo snapshot (nova baseline)
    python sftp_snapshot.py diff --save

    # diff offline entre dois snapshots já gravados (não conecta no SFTP)
    python sftp_snapshot.py diff --from snapshots/a.json --to snapshots/b.json

    # lista os snapshots gravados
    python sftp_snapshot.py list

    # emite o diff também em JSON (para alimentar a futura DAG)
    python sftp_snapshot.py diff --json diff.json
"""

import argparse
import json
import logging
import os
import stat
import sys
from datetime import datetime
from pathlib import Path

import paramiko
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
log = logging.getLogger(__name__)

SFTP_HOST     = os.getenv("SFTP_HOST")
SFTP_PORT     = os.getenv("SFTP_PORT")
SFTP_USER     = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")

SNAPSHOT_DIR = Path(__file__).parent / "snapshots"

# Ruído que não interessa rastrear (config de shell, temporários do SFTP).
IGNORED_EXTENSIONS = {
    ".bashrc", ".bash_logout", ".bash_history", ".profile",
    ".mkshrc", ".viminfo", ".filepart", ".rpt",
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


# Conexão SFTP
def connect_sftp() -> tuple[paramiko.Transport, paramiko.SFTPClient]:
    if not SFTP_PASSWORD:
        raise ValueError("SFTP_PASSWORD não encontrada no ambiente/.env")
    transport = paramiko.Transport((SFTP_HOST, int(SFTP_PORT)))
    transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
    transport.set_keepalive(30)
    sftp = paramiko.SFTPClient.from_transport(transport)
    if sftp is None:
        raise RuntimeError("Falha ao abrir sessão SFTP")
    return transport, sftp


def walk_sftp(sftp: paramiko.SFTPClient) -> dict[str, dict]:
    """Percorre a árvore inteira e devolve {path: {size, mtime}}."""
    files: dict[str, dict] = {}

    def walk(path: str):
        try:
            items = sftp.listdir_attr(path)
        except Exception as e:
            log.error("erro ao listar %s: %s", path, e)
            return
        for item in items:
            full_path = f"{path}/{item.filename}"
            ext = Path(item.filename).suffix.lower()
            if item.st_mode is not None and stat.S_ISDIR(item.st_mode):
                walk(full_path)
            elif ext in IGNORED_EXTENSIONS or item.filename.startswith("."):
                continue
            else:
                files[full_path] = {
                    "size": int(item.st_size or 0),
                    "mtime": int(item.st_mtime) if item.st_mtime else None,
                }

    walk(".")
    return files


# Snapshots
def capture_snapshot(sftp: paramiko.SFTPClient) -> dict:
    log.info("Coletando árvore de arquivos do SFTP...")
    files = walk_sftp(sftp)
    total_size = sum(f["size"] for f in files.values())
    log.info("%d arquivos | %s", len(files), format_size(total_size))
    return {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "host": SFTP_HOST,
        "file_count": len(files),
        "total_size": total_size,
        "files": files,
    }


def save_snapshot(snap: dict) -> Path:
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = SNAPSHOT_DIR / f"snapshot_{ts}.json"
    path.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Snapshot gravado: %s", path)
    return path


def load_snapshot(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def list_snapshots() -> list[Path]:
    if not SNAPSHOT_DIR.exists():
        return []
    return sorted(SNAPSHOT_DIR.glob("snapshot_*.json"))


def latest_snapshot() -> Path | None:
    snaps = list_snapshots()
    return snaps[-1] if snaps else None


# Diff
def detect_moves(added: dict, removed: dict) -> list[dict]:
    """Casa arquivos que sumiram com arquivos que apareceram pelo par
    (nome do arquivo, tamanho): sinal forte de arquivamento/movimentação.
    Remove os pares casados de `added`/`removed` (mutação intencional)."""
    index: dict[tuple[str, int], list[str]] = {}
    for path, meta in removed.items():
        index.setdefault((Path(path).name, meta["size"]), []).append(path)

    moves: list[dict] = []
    for path in list(added.keys()):
        key = (Path(path).name, added[path]["size"])
        candidates = index.get(key)
        if candidates:
            src = candidates.pop(0)
            moves.append({"from": src, "to": path, "size": added[path]["size"]})
            del added[path]
            del removed[src]
    return moves


def diff_snapshots(old: dict, new: dict) -> dict:
    old_files: dict[str, dict] = old["files"]
    new_files: dict[str, dict] = new["files"]

    added   = {p: m for p, m in new_files.items() if p not in old_files}
    removed = {p: m for p, m in old_files.items() if p not in new_files}

    modified: list[dict] = []
    unchanged = 0
    for path in old_files.keys() & new_files.keys():
        o, n = old_files[path], new_files[path]
        if o["size"] != n["size"] or o["mtime"] != n["mtime"]:
            modified.append({
                "path": path,
                "old_size": o["size"], "new_size": n["size"],
                "old_mtime": o["mtime"], "new_mtime": n["mtime"],
            })
        else:
            unchanged += 1

    moves = detect_moves(added, removed)  # consome added/removed casados

    return {
        "old_captured_at": old.get("captured_at"),
        "new_captured_at": new.get("captured_at"),
        "added":     [{"path": p, **m} for p, m in sorted(added.items())],
        "removed":   [{"path": p, **m} for p, m in sorted(removed.items())],
        "modified":  sorted(modified, key=lambda x: x["path"]),
        "moved":     sorted(moves, key=lambda x: x["to"]),
        "unchanged": unchanged,
    }


def interpret(d: dict) -> str:
    """Heurística para responder: como o SFTP atualiza?"""
    if not (d["added"] or d["removed"] or d["modified"] or d["moved"]):
        return "Nada mudou desde o último snapshot."
    sinais = []
    if d["modified"]:
        sinais.append(
            f"{len(d['modified'])} arquivo(s) alterado(s) no mesmo caminho "
            "→ indício de SOBRESCRITA in-place."
        )
    if d["moved"]:
        sinais.append(
            f"{len(d['moved'])} arquivo(s) com mesmo nome+tamanho mudaram de pasta "
            "→ indício de que os ANTIGOS são MOVIDOS/ARQUIVADOS."
        )
    if d["added"] and not d["modified"] and not d["moved"]:
        sinais.append(
            f"{len(d['added'])} arquivo(s) novo(s) sem mexer nos antigos "
            "→ indício de carga puramente INCREMENTAL (append)."
        )
    if d["removed"]:
        sinais.append(
            f"{len(d['removed'])} arquivo(s) sumiram sem destino óbvio "
            "→ podem ter sido DELETADOS."
        )
    return " ".join(sinais)


def print_report(d: dict, limit: int | None) -> None:
    def section(title: str, items: list, fmt) -> None:
        print(f"\n## {title} ({len(items)})")
        shown = items if limit is None else items[:limit]
        for it in shown:
            print("  " + fmt(it))
        if limit is not None and len(items) > limit:
            print(f"  ... e mais {len(items) - limit}")

    print("=" * 70)
    print(f"DIFF  {d['old_captured_at']}  →  {d['new_captured_at']}")
    print("=" * 70)
    print(
        f"novos={len(d['added'])}  modificados={len(d['modified'])}  "
        f"movidos={len(d['moved'])}  removidos={len(d['removed'])}  "
        f"inalterados={d['unchanged']}"
    )

    section("NOVOS", d["added"],
            lambda x: f"+ {x['path']}  ({format_size(x['size'])}, {format_mtime(x['mtime'])})")
    section("MODIFICADOS", d["modified"],
            lambda x: (f"~ {x['path']}  "
                       f"{format_size(x['old_size'])}→{format_size(x['new_size'])}  "
                       f"mtime {format_mtime(x['old_mtime'])}→{format_mtime(x['new_mtime'])}"))
    section("MOVIDOS (provável arquivamento)", d["moved"],
            lambda x: f"» {x['from']}\n      → {x['to']}  ({format_size(x['size'])})")
    section("REMOVIDOS", d["removed"],
            lambda x: f"- {x['path']}  ({format_size(x['size'])})")

    print("\n" + "-" * 70)
    print("INTERPRETAÇÃO:", interpret(d))
    print("-" * 70)


# Comandos
def cmd_snapshot(args) -> None:
    transport, sftp = connect_sftp()
    try:
        snap = capture_snapshot(sftp)
    finally:
        sftp.close()
        transport.close()
    save_snapshot(snap)


def cmd_diff(args) -> None:
    # Modo offline: dois snapshots em disco, sem se conectar ao SFTP.
    if args.from_snap and args.to_snap:
        old = load_snapshot(Path(args.from_snap))
        new = load_snapshot(Path(args.to_snap))
    else:
        base = latest_snapshot()
        if base is None:
            log.error("Nenhum snapshot encontrado em %s. "
                      "Rode 'python sftp_snapshot.py snapshot' primeiro.", SNAPSHOT_DIR)
            sys.exit(1)
        log.info("Baseline: %s", base.name)
        old = load_snapshot(base)
        transport, sftp = connect_sftp()
        try:
            new = capture_snapshot(sftp)
        finally:
            sftp.close()
            transport.close()
        if args.save:
            save_snapshot(new)

    d = diff_snapshots(old, new)
    print_report(d, args.limit)

    if args.json:
        Path(args.json).write_text(
            json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("Diff em JSON: %s", args.json)


def cmd_list(args) -> None:
    snaps = list_snapshots()
    if not snaps:
        print(f"Nenhum snapshot em {SNAPSHOT_DIR}")
        return
    for s in snaps:
        meta = load_snapshot(s)
        print(f"{s.name}  {meta.get('captured_at')}  "
              f"{meta.get('file_count')} arquivos  "
              f"{format_size(meta.get('total_size', 0))}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Snapshots e diffs do estado do SFTP.")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("snapshot", help="grava fotografia do estado atual do SFTP")

    pd = sub.add_parser("diff", help="compara SFTP atual (ou dois snapshots) e mostra o diff")
    pd.add_argument("--save", action="store_true",
                    help="grava o estado atual como novo snapshot (nova baseline)")
    pd.add_argument("--from", dest="from_snap", metavar="SNAPSHOT.json",
                    help="diff offline: snapshot de origem")
    pd.add_argument("--to", dest="to_snap", metavar="SNAPSHOT.json",
                    help="diff offline: snapshot de destino")
    pd.add_argument("--json", metavar="ARQUIVO.json", help="também grava o diff em JSON")
    pd.add_argument("--limit", type=int, default=None,
                    help="limita itens listados por seção (padrão: todos)")

    sub.add_parser("list", help="lista snapshots gravados")
    return p


def main() -> None:
    args = build_parser().parse_args()
    if args.cmd == "snapshot":
        cmd_snapshot(args)
    elif args.cmd == "diff":
        cmd_diff(args)
    elif args.cmd == "list":
        cmd_list(args)


if __name__ == "__main__":
    main()
