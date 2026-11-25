"""Masking of a delivered file BEFORE it lands in raw (ADR-0002 R0).

Thin adapter over the existing rules in scripts/mascaramento/ (same HMAC, same tokens as
the legacy path), applied to a local temporary file instead of an object already in raw.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

# what the masking reads on a local file; .mdb is exported per table (export_access)
TABULAR = {".csv", ".txt"}
EXCEL = {".xlsx"}
ACCESS = {".mdb", ".accdb"}


def _legacy_key(key: str) -> str:
    """The positional-columns map (files without header) is keyed by the old raw key."""
    return re.sub(r"/load_id=[^/]+/", "/", key)


def mask_file(src: str, key: str) -> tuple[str, dict]:
    """Masks the local file `src`, delivered as raw `key`.

    Returns (path to upload, record for lake._masking_log). The status follows the masking
    log: `masked` or `skipped_no_pii` (nothing to mask). Any error raises, and so does a
    text file whose columns cannot be told apart: the file must not be uploaded.
    """
    from lake_utils import detectar_dialeto, detectar_encoding
    from mascaramento.regras import targets_por_posicao

    ext = Path(key).suffix.lower()
    rec = {"masked_columns": [], "registros_total": None, "registros_alterados": None}
    dst = src + ".masked" + ext
    if ext in TABULAR:
        from mascaramento.tabular import _mascarar_tabular

        with open(src, "rb") as f:
            sample = f.read(1 << 16)
        encoding = detectar_encoding(sample)
        positional = targets_por_posicao(_legacy_key(key))
        dialect = detectar_dialeto(sample, encoding)
        if dialect is None and not positional:
            raise ValueError("no header and no positional map: the masking cannot be checked")
        delim, lineterm, fully_quoted = dialect or (";", "\n", False)
        targets, _, total, changed = _mascarar_tabular(
            src, dst, delim, lineterm, fully_quoted, encoding, positional
        )
    elif ext in EXCEL:
        from mascaramento.xlsx import _mascarar_xlsx, _xlsx_tem_alvo

        if not _xlsx_tem_alvo(src)[0]:
            return src, {**rec, "status": "skipped_no_pii"}
        targets, _, total, changed, _ = _mascarar_xlsx(src, dst)
    else:
        raise ValueError(f"no masking for {ext} ({key})")

    rec.update(registros_total=total, registros_alterados=changed)
    if not targets:
        os.remove(dst)
        return src, {**rec, "status": "skipped_no_pii"}
    columns = [{k: t[k] for k in ("column", "category", "action")} for t in targets]
    return dst, {**rec, "status": "masked", "masked_columns": columns}


def export_access(src: str, workdir: str) -> list[tuple[str, str, dict]]:
    """An Access file becomes one masked CSV per table; the .mdb itself never lands
    (ADR-0002 R0, mdbtools cannot rewrite it). Returns [(table, csv path, record)]."""
    from lake_utils import MDB_DELIM, MDB_ENCODING, mdb_export_para_csv, mdb_tabelas
    from mascaramento.tabular import mascarar_csv

    out = []
    for table in mdb_tabelas(src):
        raw_csv = os.path.join(workdir, f"{table}.csv")
        mdb_export_para_csv(src, table, raw_csv)
        masked = raw_csv + ".masked.csv"
        targets, total, changed = mascarar_csv(raw_csv, masked, MDB_DELIM, MDB_ENCODING)
        os.replace(masked, raw_csv)  # mascarar_csv always writes the full file
        rec = {
            "status": "masked" if targets else "skipped_no_pii",
            "masked_columns": [
                {k: t[k] for k in ("column", "category", "action")} for t in targets
            ],
            "registros_total": total,
            "registros_alterados": changed,
        }
        out.append((table, raw_csv, rec))
    return out
