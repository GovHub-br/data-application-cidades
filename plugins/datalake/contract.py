"""The ODCS contracts of the sources (ADR-0006) as seen by the ingestion.

`load()` reads `contracts/*.odcs.yaml` at run time (never at DAG parse), `match()` finds
the dataset of a raw key, and `structural_status()` is the structural drift rule.
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
ACCESS = (".mdb", ".accdb")


@dataclass(frozen=True)
class Dataset:
    source: str
    name: str
    path: str  # staging path of the dataset (a glob)
    file_pattern: re.Pattern
    load_type: str = "full"
    max_row_variation: Optional[float] = None
    reader: Optional[str] = None
    columns: tuple = ()
    aliases: dict = field(default_factory=dict)  # column -> other names it arrived with

    @property
    def folder(self) -> str:
        """Folder of the dataset below raw/ and staging/ (a glob)."""
        return str(Path(self.path).parent).removeprefix("staging").strip("/") or "*"

    @property
    def access_table(self) -> Optional[str]:
        """For a table exported from an Access file, its name (`<file>__<table>.parquet`)."""
        stem = Path(self.path).stem
        return stem.rpartition("__")[2] if "__" in stem else None


def _custom(item: dict) -> dict:
    return {c["property"]: c.get("value") for c in item.get("customProperties") or []}


def load(folder: Path = CONTRACTS) -> list[Dataset]:
    datasets = []
    for path in sorted(folder.glob("*.odcs.yaml")):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        for obj in contract.get("schema") or []:
            props = _custom(obj)
            columns = tuple(p["name"] for p in obj.get("properties") or [])
            aliases = {
                p["name"]: tuple(_custom(p).get("aliases") or ())
                for p in obj.get("properties") or []
                if _custom(p).get("aliases")
            }
            variation = props.get("max_row_variation")
            datasets.append(
                Dataset(
                    source=contract["id"],
                    name=obj["name"],
                    path=props["path"],
                    file_pattern=re.compile(props["file_pattern"]),
                    load_type=props.get("load_type") or "full",
                    max_row_variation=float(variation) if variation is not None else None,
                    reader=props.get("reader"),
                    columns=columns,
                    aliases=aliases,
                )
            )
    return datasets


def _split(raw_key: str) -> tuple[str, str, Optional[str]]:
    """raw key -> (folder, delivered file name, Access table), without the load folder."""
    parts = [p for p in raw_key.removeprefix("raw/").split("/") if not p.startswith("load_id=")]
    if len(parts) >= 2 and parts[-2].lower().endswith(ACCESS):  # <file>.mdb/<table>.csv
        return "/".join(parts[:-2]), parts[-2], Path(parts[-1]).stem.lower()
    return "/".join(parts[:-1]), parts[-1], None


def match(raw_key: str, datasets: list[Dataset]) -> Optional[Dataset]:
    """The dataset a delivered file belongs to; None means `uncataloged` (ADR-0006)."""
    folder, name, table = _split(raw_key)
    for ds in datasets:
        if (
            ds.file_pattern.fullmatch(name)
            and fnmatch.fnmatch(folder, ds.folder)
            and (table or None) == ds.access_table
        ):
            return ds
    return None


def structural_status(
    header: list[str],
    dataset: Optional[Dataset],
    rows: Optional[int] = None,
    previous_rows: Optional[int] = None,
) -> tuple[str, dict]:
    """Structural drift of one delivered table (ADR-0006 §2) -> (status, details).

    `header` holds the column names as the conversion normalizes them. Only `quarantined`
    blocks the file; `uncataloged` stays in raw and goes to the report.
    """
    if dataset is None:
        return "uncataloged", {}
    got, expected = set(header), set(dataset.columns)
    missing, extra = expected - got, got - expected
    renamed = {
        col: alias for col in missing for alias in dataset.aliases.get(col, ()) if alias in extra
    }
    missing -= set(renamed)
    extra -= set(renamed.values())
    details = {"missing": sorted(missing), "new": sorted(extra), "renamed": renamed}
    if missing:
        return "quarantined", details
    if (
        dataset.load_type == "full"
        and dataset.max_row_variation is not None
        and rows is not None
        and previous_rows
        and abs(rows - previous_rows) / previous_rows > dataset.max_row_variation
    ):
        return "quarantined", {**details, "rows": rows, "previous_rows": previous_rows}
    if extra:
        return "additive_drift", details
    if renamed:
        return "known_rename", details
    return "approved", details
