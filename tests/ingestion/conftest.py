"""Fixtures compartilhadas da suíte da ingestão (`plugins/ingestion`).

O Airflow põe `plugins/` no PYTHONPATH; aqui o pytest faz o mesmo, para os testes
importarem `ingestion` como a DAG importa, sem depender do Makefile.
"""

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from moto import mock_aws

PLUGINS_DIR = Path(__file__).resolve().parents[2] / "plugins"

if str(PLUGINS_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGINS_DIR))

from ingestion.storage import S3StorageBackend  # noqa: E402  (precisa do sys.path acima)

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
