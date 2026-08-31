#!/usr/bin/env python3
"""Gera o catálogo OpenMetadata sem persistir artefatos dbt brutos."""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT = ROOT / "dbt" / "mcid"
EXPORTER = ROOT / "scripts" / "governance" / "exportar_catalogo_openmetadata.py"
OUTPUT = ROOT / "docs-conjuntura" / "openmetadata_semantic_catalog.json"


def main() -> int:
    load_dotenv(ROOT / ".env", override=False)
    with tempfile.TemporaryDirectory(prefix="openmetadata-dbt-") as private_target:
        subprocess.run(
            [
                "poetry",
                "run",
                "dbt",
                "docs",
                "generate",
                "--profiles-dir",
                str(DBT_PROJECT),
                "--target-path",
                private_target,
            ],
            cwd=DBT_PROJECT,
            check=True,
        )
        subprocess.run(
            [
                sys.executable,
                str(EXPORTER),
                "--target-dir",
                private_target,
                "--output",
                str(OUTPUT),
            ],
            cwd=ROOT,
            check=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
