"""Backends de storage contra os quais o contrato roda."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from moto import mock_aws

from ingestion.storage import S3StorageBackend, StorageBackend, StorageFactory

BACKENDS = ["local", "s3"]

TEST_BUCKET = "test-lake"


@pytest.fixture
def fake_aws(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """S3 em memória (moto): o S3Hook sem Connection cai nas credenciais do ambiente."""
    for name, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(name, value)
    with mock_aws():
        yield


@pytest.fixture
def s3_backend(fake_aws: None) -> S3StorageBackend:
    backend = S3StorageBackend(bucket=TEST_BUCKET, conn_id=None)
    backend.hook.get_conn().create_bucket(Bucket=TEST_BUCKET)
    return backend


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[StorageBackend]:
    if request.param == "local":
        yield StorageFactory.create("local", root=tmp_path / "lake")
    else:
        yield request.getfixturevalue("s3_backend")
