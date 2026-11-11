"""`extract` of the SFTP of the MCid (ADR-0004): download, mask, land in raw with load_id.

The DAG builds the hooks (SFTPHook, S3Hook, PostgresHook) and this module calls their
methods, so it is tested with in-memory fakes of them. A file whose masking fails is not uploaded: the other files go
on, and the run fails at the end (the `report` task).
"""

from __future__ import annotations

import json
import os
import stat
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Optional

from datalake.chaves import raw_key
from datalake.mascara import ACCESS, EXCEL, TABULAR, export_access, mask_file

FOLDERS = (
    "/home/fabrica/Analise_SNH",
    "/home/fabrica/GEFUS",  # inclui GEFUS/CadUnico; a antiga /home/fabrica/CadUnico não existe mais
    "/home/caixa.geavo/GEAVO",
)
SUPPORTED = TABULAR | EXCEL | ACCESS | {".zip"}
UNSUPPORTED = {".xls"}  # neither the masking nor the conversion reads it (no xlrd)
MAX_DEPTH = 10
INGEST_FIELDS = ["sftp_path", "file_name", "file_size", "file_mtime", "file_hash", "minio_key",
                 "status", "error_message", "started_at", "finished_at"]  # fmt: skip
MASKING_FIELDS = ["execution_id", "minio_key", "file_name", "source_hash", "masked_hash",
                  "masked_columns", "registros_total", "registros_alterados", "status"]  # fmt: skip


def walk(sftp, folder: str, depth: int = 0) -> Iterator[tuple[str, int, int]]:
    """(path, size, mtime) of every file below `folder`, in a stable order.

    Not `SFTPHook.walktree`: it gives only the path, and skipping without download needs
    the size and mtime that the listing already brings.
    """
    for attr in sorted(sftp.list_directory_with_attr(folder), key=lambda a: a.filename):
        path = f"{folder}/{attr.filename}"
        if stat.S_ISDIR(attr.st_mode or 0):
            if depth < MAX_DEPTH:
                yield from walk(sftp, path, depth + 1)
        else:
            yield path, attr.st_size or 0, int(attr.st_mtime or 0)


def mirror(sftp_path: str) -> str:
    """Folder below raw/ that mirrors the server (ADR-0005): /home/fabrica/GEFUS/x -> sftp/fabrica/GEFUS."""
    folder = str(Path(sftp_path).parent).removeprefix("/home/").strip("/")
    return f"sftp/{folder}" if folder else "sftp"


class Control:
    """lake._ingest_minio_log and lake._masking_log, through the PostgresHook.

    `write=False` is the dry run: it still skips what was landed, and records nothing.
    """

    def __init__(self, pg, execution_id: str, write: bool = True):
        self.pg, self.execution_id, self.write = pg, execution_id, write

    def ingested(self) -> set:
        """(path, mtime) already landed or refused: skipped without download.

        The legacy ingestion logged ZIP members as `<zip>@<member>` with the ZIP mtime, so
        the part before `@` keys the ZIP itself; size is left out for the same reason.
        """
        rows = self.pg.get_records(
            "SELECT split_part(sftp_path, '@', 1), EXTRACT(EPOCH FROM file_mtime)::bigint "
            "FROM lake._ingest_minio_log WHERE file_hash IS NOT NULL OR status = 'unsupported'"
        )
        return {(p, m) for p, m in rows}

    def hashes(self) -> set:
        rows = self.pg.get_records(
            "SELECT DISTINCT file_hash FROM lake._ingest_minio_log WHERE file_hash IS NOT NULL"
        )
        return {r[0] for r in rows}

    def ingest(self, path, key, size, mtime, file_hash, status, error=None) -> None:
        if not self.write:
            return
        # the rows without hash (error, unsupported) sit on a partial unique index that
        # ON CONFLICT cannot name: the new row of the path replaces them
        self.pg.run(
            "DELETE FROM lake._ingest_minio_log WHERE sftp_path = %s AND file_hash IS NULL",
            parameters=(path,),
        )
        now = datetime.now(timezone.utc)
        self.pg.insert_rows(
            "lake._ingest_minio_log",
            [
                (
                    path,
                    Path(path).name,
                    size,
                    datetime.fromtimestamp(mtime, timezone.utc),
                    file_hash,
                    key,
                    status,
                    error,
                    now,
                    now,
                )
            ],  # fmt: skip
            INGEST_FIELDS,
            replace=True,
            replace_index=["sftp_path", "file_hash"],
        )

    def mask(self, key: str, rec: dict, source_hash: str, landed_hash: str) -> None:
        """Written BEFORE the upload: no object reaches raw without a masking record."""
        if not self.write:
            return
        self.pg.insert_rows(
            "lake._masking_log",
            [
                (
                    self.execution_id,
                    key,
                    Path(key).name,
                    source_hash,
                    landed_hash,
                    json.dumps(rec["masked_columns"]),
                    rec["registros_total"],
                    rec["registros_alterados"],
                    rec["status"],
                )
            ],  # fmt: skip
            MASKING_FIELDS,
            replace=True,
            replace_index=["minio_key", "source_hash"],
            replace_target=[],  # a retry masks to the same bytes: keep the first record
        )


def _land(
    local: str, name: str, folder: str, load: str, workdir: str
) -> list[tuple[str, str, dict]]:
    """(raw key, local file to upload, masking record) of one delivered file."""
    ext = Path(name).suffix.lower()
    if ext == ".zip":
        out = []
        with zipfile.ZipFile(local) as archive:
            for member in sorted(archive.namelist()):
                if member.endswith("/") or Path(member).suffix.lower() not in SUPPORTED - {".zip"}:
                    continue
                inner = archive.extract(member, os.path.join(workdir, "zip"))
                out += _land(inner, f"{name}/{member}", folder, load, workdir)
        return out
    if ext in ACCESS:
        return [
            (raw_key(folder, f"{name}/{table}.csv", load), path, rec)
            for table, path, rec in export_access(local, workdir)
        ]
    key = raw_key(folder, name, load)
    path, rec = mask_file(local, key)
    return [(key, path, rec)]


def _one(sftp, upload, control, load, md5, tmpdir, entry, known_hashes, result) -> None:
    """Download, mask and land one delivered file; a failure is collected, never raised."""
    path, size, mtime = entry
    with tempfile.TemporaryDirectory(dir=tmpdir) as workdir:
        local = os.path.join(workdir, Path(path).name)
        try:
            sftp.retrieve_file(path, local)
            file_hash = md5(local)
            if file_hash in known_hashes:  # moved or re-sent without change
                result["duplicates"].append(path)
                control.ingest(path, None, size, mtime, file_hash, "duplicate")
                return
            landed = _land(local, Path(path).name, mirror(path), load, workdir)
            for key, file, rec in landed:
                control.mask(key, rec, file_hash, md5(file))
                upload(file, key)
            first = landed[0][0] if landed else None
            control.ingest(path, first, size, mtime, file_hash, "success")
            known_hashes.add(file_hash)
            result["keys"] += [key for key, _, _ in landed]
        except Exception as error:  # noqa: BLE001 — the run fails at the end, in `report`
            result["errors"].append(f"{path}: {type(error).__name__}: {error}"[:500])
            control.ingest(path, None, size, mtime, None, "error", str(error)[:500])


def extract(
    sftp,
    upload: Callable[[str, str], None],
    control: Control,
    load: str,
    md5: Callable[[str], str],
    tmpdir: Optional[str] = None,
    limit: int = 0,
    folders: tuple = FOLDERS,
    files: tuple = (),
) -> dict:
    """Lands every new file of the monitored folders in raw/sftp/.../load_id=<load>/.

    `files` (full SFTP paths) forces just those, even if already landed: no skip by
    path/mtime nor by hash.
    Returns {"keys": landed raw keys, "errors": [...], "unsupported": [...],
    "duplicates": [...]}; only keys travel between tasks, never content.
    """
    wanted = set(files)
    seen, known_hashes = (set(), set()) if wanted else (control.ingested(), control.hashes())
    if wanted:  # only the folders that hold a forced file
        folders = tuple(f for f in folders if any(w.startswith(f + "/") for w in wanted))
    result = {"keys": [], "errors": [], "unsupported": [], "duplicates": []}
    done = 0
    for folder in folders:
        try:  # listing first: a vanished folder must not stop the others
            listed = list(walk(sftp, folder))
        except OSError as error:
            result["errors"].append(f"{folder}: pasta inexistente ou sem acesso ({error})")
            continue
        for path, size, mtime in listed:
            ext = Path(path).suffix.lower()
            if wanted and path not in wanted:
                continue
            wanted.discard(path)
            if (path, mtime) in seen or ext not in SUPPORTED | UNSUPPORTED:
                continue
            if ext in UNSUPPORTED:
                result["unsupported"].append(path)
                control.ingest(path, None, size, mtime, None, "unsupported")
                continue
            if limit and done >= limit:
                return result
            done += 1
            _one(
                sftp, upload, control, load, md5, tmpdir, (path, size, mtime), known_hashes, result
            )
    result["errors"] += [
        f"{path}: não encontrado nas pastas monitoradas" for path in sorted(wanted)
    ]
    return result
