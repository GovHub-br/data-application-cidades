"""Fixtures compartilhadas da suíte da ingestão (`plugins/ingestion`).

O Airflow põe `plugins/` no PYTHONPATH; aqui o pytest faz o mesmo, para os testes
importarem `ingestion` como a DAG importa, sem depender do Makefile.
"""

import os
import socket
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlparse

import pytest
from moto import mock_aws

PLUGINS_DIR = Path(__file__).resolve().parents[2] / "plugins"

if str(PLUGINS_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGINS_DIR))

from ingestion.storage import (  # noqa: E402  (precisa do sys.path acima)
    S3StorageBackend,
    StorageBackend,
)

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


@pytest.fixture
def minio_backend() -> Iterator[StorageBackend]:
    """MinIO real, num bucket de teste, isolado num prefixo descartável.

    Pulado sem `INGESTION_TEST_BUCKET` ou se o endpoint da Connection não responder.
    Nunca usa o bucket do lake: o job de outro time varre o `data-lake-mcid`.
    """
    bucket = os.environ.get("INGESTION_TEST_BUCKET")
    if not bucket:
        pytest.skip("INGESTION_TEST_BUCKET não definida")
    inner = S3StorageBackend(
        bucket=bucket, conn_id=os.environ.get("INGESTION_STORAGE_CONN_ID", "minio_lake")
    )
    endpoint = inner.hook.conn_config.endpoint_url
    url = urlparse(endpoint or "")
    if not endpoint or not reachable(url.hostname, url.port or 80):
        pytest.skip(f"MinIO não responde em {endpoint!r}")

    backend = _PrefixedStorage(inner, f"_tests/{uuid.uuid4().hex}/")
    yield backend
    for key in inner.list(backend.prefix):
        inner.delete(key)


def reachable(host: str | None, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


class _PrefixedStorage(StorageBackend):
    """Vê só o que está sob `prefix` do backend real, como se fosse a raiz."""

    def __init__(self, inner: StorageBackend, prefix: str) -> None:
        self.inner = inner
        self.prefix = prefix

    def put_file(self, key: str, local_path: Path) -> None:
        self.inner.put_file(self.prefix + key, local_path)

    def get_file(self, key: str, local_path: Path) -> None:
        self.inner.get_file(self.prefix + key, local_path)

    def list(self, prefix: str) -> list[str]:
        return [key[len(self.prefix) :] for key in self.inner.list(self.prefix + prefix)]

    def delete(self, key: str) -> None:
        self.inner.delete(self.prefix + key)

    def exists(self, key: str) -> bool:
        return self.inner.exists(self.prefix + key)
