"""Backends de storage contra os quais o contrato roda."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from ingestion.storage import StorageBackend, StorageFactory

BACKENDS = ["local", "s3", pytest.param("minio", marks=pytest.mark.integration)]


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[StorageBackend]:
    if request.param == "local":
        yield StorageFactory.create("local", root=tmp_path / "lake")
    elif request.param == "s3":
        yield request.getfixturevalue("s3_backend")
    else:
        yield request.getfixturevalue("minio_backend")
