"""Storage num bucket S3 (MinIO), sobre o S3Hook do provider amazon."""

from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError

from ingestion.storage.base_storage import StorageBackend
from ingestion.storage.storage_errors import ObjectNotFoundError
from ingestion.storage.storage_registry import StorageFactory

_NOT_FOUND = {"404", "NoSuchKey", "NotFound"}


@StorageFactory.register("s3")
class S3StorageBackend(StorageBackend):
    """Chaves de um bucket, com credencial e endpoint vindos da Connection `conn_id`.

    O MinIO entra pela Connection (tipo `aws`, `endpoint_url` no extra). O hook só é
    criado no primeiro uso, dentro da task: construir o backend no parse da DAG não
    consulta Connection. `conn_id=None` usa as credenciais do ambiente (testes).
    """

    def __init__(
        self,
        bucket: str,
        conn_id: str | None = "minio_lake",
        page_size: int | None = None,
    ) -> None:
        self.bucket = bucket
        self.conn_id = conn_id
        self.page_size = page_size
        self._hook: S3Hook | None = None

    @property
    def hook(self) -> S3Hook:
        if self._hook is None:
            self._hook = S3Hook(aws_conn_id=self.conn_id)
        return self._hook

    def put_file(self, key: str, local_path: Path) -> None:
        # upload_file do boto por baixo: multipart acima do limiar, sem ler tudo.
        self.hook.load_file(
            filename=str(local_path), key=key, bucket_name=self.bucket, replace=True
        )

    def get_file(self, key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.hook.get_conn().download_file(self.bucket, key, str(local_path))
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in _NOT_FOUND:
                raise ObjectNotFoundError(key) from exc
            raise

    def list(self, prefix: str) -> list[str]:
        keys = self.hook.list_keys(
            bucket_name=self.bucket, prefix=prefix, page_size=self.page_size
        )
        return sorted(keys)

    def delete(self, key: str) -> None:
        # DeleteObject do S3 não acusa chave ausente; o contrato pede que acuse.
        if not self.exists(key):
            raise ObjectNotFoundError(key)
        self.hook.delete_objects(bucket=self.bucket, keys=key)

    def exists(self, key: str) -> bool:
        return bool(self.hook.check_for_key(key, bucket_name=self.bucket))
