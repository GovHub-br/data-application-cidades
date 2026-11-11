"""`transform` and `report` of the new ingestion (ADR-0004, ADR-0006).

`transform` converts one landed raw key through the reader of its contract. The structural
check runs at the door of staging: the conversion writes the Parquet locally, `Gate` reads
its footer (the column names exactly as the conversion normalized them) and only lets it
into staging when the contract allows. `report` sums up the run and fails it on errors.
"""

from __future__ import annotations

import html
import json
import os
import re
from collections import Counter
from typing import Callable, Optional

import pyarrow.parquet as pq

from datalake import contrato, leitores

BLOCKING = ("error", "quarantined")


class Lake:
    """The three calls the conversion makes on the lake, over an S3Hook (Connection lake_mcid).

    `prefix` moves every read and write below it: the dry run works under `dryrun/`.
    """

    def __init__(self, hook, bucket: str, prefix: str = ""):
        self.hook, self.bucket, self.prefix = hook, bucket, prefix

    def sample_bytes(self, key: str, n: int = 65536, inicio: int = 0) -> bytes:
        obj = self.hook.get_key(self.prefix + key, self.bucket)
        return obj.get(Range=f"bytes={inicio}-{inicio + n - 1}")["Body"].read()

    def baixar_para_tempfile(self, key: str, suffix: str = "", tmpdir: Optional[str] = None) -> str:
        # keeps the file name: openpyxl refuses a workbook without the .xlsx extension
        # shortcut: the conversion removes the file but not its airflow_tmp_dir_*; clean LAKE_TMPDIR if it piles up
        return self.hook.download_file(
            self.prefix + key, self.bucket, local_path=tmpdir, preserve_file_name=True
        )

    def upload_arquivo(self, path: str, key: str) -> str:
        self.hook.load_file(path, self.prefix + key, bucket_name=self.bucket, replace=True)
        return key


def _rel(staging_key: str) -> str:
    """The key without its first folder: staging/ and staging_dryrun/ name the same table."""
    return staging_key.split("/", 1)[-1]


class Gate:
    """Stands in for the lake while the conversion runs; holds back quarantined Parquet."""

    def __init__(self, lake, dataset: contrato.Dataset, previous_rows: Optional[int]):
        self.lake, self.dataset, self.previous_rows = lake, dataset, previous_rows
        self.verdicts: dict[str, tuple[str, dict]] = {}

    def __getattr__(self, name):
        return getattr(self.lake, name)

    def upload_arquivo(self, path: str, key: str) -> str:
        meta = pq.read_metadata(path)
        header = [c for c in meta.schema.names if not c.startswith("_")]  # lineage out
        verdict = contrato.structural_status(
            header, self.dataset, meta.num_rows, self.previous_rows
        )
        self.verdicts[_rel(key)] = verdict
        if verdict[0] != "quarantined":
            self.lake.upload_arquivo(path, key)
        return key


def previous_rows(pg) -> Callable[[contrato.Dataset, list], Optional[int]]:
    """Rows of the newest converted load of a dataset, for the row-variation rule."""
    # shortcut: reads every converted row of _staging_log; filter by folder if it grows large
    rows = pg.get_records(
        "SELECT raw_key, n_linhas FROM lake._staging_log "
        "WHERE status = 'converted' AND n_linhas > 0 ORDER BY created_at DESC"
    )

    def find(dataset: contrato.Dataset, datasets: list) -> Optional[int]:
        return next((n for key, n in rows if contrato.match(key, datasets) == dataset), None)

    return find


def transform(
    lake,
    key: str,
    execution_id: str,
    apply: bool,
    datasets: list,
    previous: Callable[[contrato.Dataset, list], Optional[int]],
) -> list[dict]:
    """_staging_log records of one landed raw key; `structural` carries the drift status."""
    from conversao.processamento import _novo_registro

    dataset = contrato.match(key, datasets)
    if dataset is None:  # stays in raw, goes to the e-mail (ADR-0006)
        return [{**_novo_registro(execution_id, key), "status": "uncataloged"}]
    gate = Gate(lake, dataset, previous(dataset, datasets))
    recs = leitores.convert(gate, key, execution_id, apply, set(), reader_name=dataset.reader)
    for rec in recs:
        status, details = gate.verdicts.get(_rel(rec["staging_key"] or ""), (None, {}))
        rec["structural"] = status
        if status == "quarantined":
            rec.update(status="quarantined", error_message=json.dumps(details)[:500])
        elif status in ("additive_drift", "known_rename"):
            rec["error_message"] = json.dumps(details)[:500]
    return recs


STAGING_FIELDS = ["execution_id", "raw_key", "staging_key", "source_hash", "n_linhas",
                  "n_colunas", "n_bad_lines", "encoding", "delimiter", "column_map", "status",
                  "error_message"]  # fmt: skip


def record(pg, recs: list[dict]) -> None:
    """The records of `transform` in lake._staging_log (same upsert as the legacy writer)."""
    rows = [[json.dumps(r[f]) if f == "column_map" else r[f] for f in STAGING_FIELDS] for r in recs]
    pg.insert_rows(
        "lake._staging_log",
        rows,
        STAGING_FIELDS,
        replace=True,
        replace_index=["raw_key", "staging_key", "source_hash"],
    )


def summary(extracted: dict, records: list[dict], lost: int = 0) -> tuple[str, bool, bool]:
    """(plain-text report, whether the run fails, whether it goes by e-mail).

    Keys and statuses only, never data. `lost` counts transform tasks that left no record.
    """
    lines, failed = [], bool(extracted.get("errors")) or lost > 0
    if lost:
        lines += [f"Conversões que falharam sem registro: {lost} (veja as tasks transform)", ""]
    for title, items in (
        ("Erros na extração (arquivo não pousou)", extracted.get("errors")),
        ("Formato sem suporte (ficou no SFTP)", extracted.get("unsupported")),
        ("Duplicados (mesmo conteúdo já ingerido)", extracted.get("duplicates")),
    ):
        if items:
            lines += [f"{title}: {len(items)}", *(f"  {i}" for i in items), ""]
    by_status: dict[str, list] = {}
    for rec in records:
        status = rec.get("structural") if rec["status"] in ("converted", "dry_run") else None
        by_status.setdefault(status or rec["status"], []).append(rec)
        failed |= rec["status"] in BLOCKING
    for status, recs in sorted(by_status.items()):
        lines.append(f"{status}: {len(recs)}")
        if status == "uncataloged":  # one line per folder: a new delivery repeats every day
            folders = Counter(
                re.sub(r"/load_id=[^/]+", "", os.path.dirname(r["raw_key"])) for r in recs
            )
            lines += [f"  {folder}: {n} arquivo(s)" for folder, n in sorted(folders.items())]
        elif status != "approved":
            lines += [f"  {r['raw_key']} {r.get('error_message') or ''}".rstrip() for r in recs]
    landed = len(extracted.get("keys") or [])
    notable = failed or bool(extracted.get("unsupported")) or set(by_status) - {"approved"}
    return "\n".join([f"Arquivos pousados em raw: {landed}", "", *lines]), failed, bool(notable)


def notify(subject: str, body: str, send: Optional[Callable[..., None]]) -> None:
    """E-mail to LAKE_ALERT_EMAIL through the SMTP provider; only the log when unset."""
    to = os.environ.get("LAKE_ALERT_EMAIL")
    if to and send:
        send(to=[t.strip() for t in to.split(",") if t.strip()], subject=subject,
             html_content=f"<pre>{html.escape(body)}</pre>")  # fmt: skip
