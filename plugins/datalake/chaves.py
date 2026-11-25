"""Load ids and lake keys (ADR-0005): every delivery lands in its own `load_id=` folder."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

_LOAD_ID = re.compile(r"(?:^|/)load_id=(\d{8}T\d{6}Z)/")


def load_id(moment: datetime) -> str:
    """UTC load id of a run, e.g. 20261111T030000Z (sortable, so it orders the loads)."""
    return moment.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def raw_key(folder: str, file_name: str, load: str) -> str:
    """raw/<channel>/<source path>/load_id=<id>/<file>, mirroring the source (ADR-0005)."""
    return f"raw/{folder.strip('/')}/load_id={load}/{file_name}"


def load_id_of(key: str) -> Optional[str]:
    """The load id inside a raw or staging key; None for keys from before the load folders."""
    found = _LOAD_ID.search(key)
    return found.group(1) if found else None
