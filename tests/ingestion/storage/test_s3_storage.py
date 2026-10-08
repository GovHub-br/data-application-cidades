"""O que é próprio do backend S3 (MinIO), além do contrato."""

from pathlib import Path

from ingestion.storage import S3StorageBackend, StorageFactory
from tests.ingestion.conftest import TEST_BUCKET


def test_default_connection_is_minio_lake() -> None:
    backend = StorageFactory.create("s3", bucket="data-lake-mcid")

    assert isinstance(backend, S3StorageBackend)
    assert backend.conn_id == "minio_lake"


def test_hook_is_only_created_on_first_use() -> None:
    # Construir o backend não pode tocar Connection: isso acontece no parse da DAG.
    backend = S3StorageBackend(bucket="data-lake-mcid")

    assert backend._hook is None


def test_list_walks_every_page(fake_aws: None, tmp_path: Path) -> None:
    backend = S3StorageBackend(bucket=TEST_BUCKET, conn_id=None, page_size=2)
    backend.hook.get_conn().create_bucket(Bucket=TEST_BUCKET)
    source = tmp_path / "f"
    source.write_bytes(b"1")
    keys = [f"raw/ibge/sinapi/2026-10-08/060000/part-{n}.json" for n in range(5)]
    for key in keys:
        backend.put_file(key, source)

    assert backend.list("raw/ibge/sinapi/") == keys
