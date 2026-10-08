"""Backends de storage contra os quais o contrato roda."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from ingestion.storage import StorageBackend, StorageFactory

BACKENDS = ["local"]


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[StorageBackend]:
    if request.param == "local":
        yield StorageFactory.create("local", root=tmp_path / "lake")
