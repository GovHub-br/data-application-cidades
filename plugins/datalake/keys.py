"""Load ids and lake keys (ADR-0005): every delivery lands in its own `load_id=` folder."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

# Root of the secretariat in the bucket: everything the pipeline writes lives below it
# (snh/raw/, snh/staging/...), the same cut as dags/snh/ and contracts/snh/. No project
# imports here: the legacy scripts import this module standalone (datalake.keys).
# shortcut: fixed on SNH, the only secretariat in the repo; becomes a parameter with the next one
LAKE_ROOT = "snh/"
RAW_PREFIX = f"{LAKE_ROOT}raw/"
STAGING_PREFIX = f"{LAKE_ROOT}staging/"
# Legacy channels (minio_transform_dag): the only ones masking and conversion sweep (B1,
# #248). The API DAGs write their own staging, and sweeping all of raw/ overwrote it.
LEGACY_PREFIXES = (f"{RAW_PREFIX}sftp/", f"{RAW_PREFIX}sharepoint/")

_LOAD_ID = re.compile(r"(?:^|/)load_id=(\d{8}T\d{6}Z)/")


def load_id(moment: datetime) -> str:
    """UTC load id of a run, e.g. 20261111T030000Z (sortable, so it orders the loads)."""
    return moment.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def raw_key(folder: str, file_name: str, load: str) -> str:
    """snh/raw/<channel>/<source path>/load_id=<id>/<file>, mirroring the source (ADR-0005)."""
    return f"{RAW_PREFIX}{folder.strip('/')}/load_id={load}/{file_name}"


def load_id_of(key: str) -> Optional[str]:
    """The load id inside a raw or staging key; None for keys from before the load folders."""
    found = _LOAD_ID.search(key)
    return found.group(1) if found else None
