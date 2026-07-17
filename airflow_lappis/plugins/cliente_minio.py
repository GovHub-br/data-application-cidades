import hashlib
import logging
import os
import tempfile
from typing import Iterator, List, Optional, Tuple

import boto3
from boto3.s3.transfer import TransferConfig


class _HashingReader:
    """Calcula hash SHA-256 enquanto os dados são lidos (upload sem temp file)."""

    def __init__(self, src: object) -> None:
        self._src = src
        self._hasher = hashlib.sha256()

    def read(self, n: int = -1) -> bytes:
        chunk = self._src.read(n)
        if chunk:
            self._hasher.update(chunk)
        return chunk

    @property
    def hexdigest(self) -> str:
        return self._hasher.hexdigest()


class ClienteMinio:
    """Cliente de I/O do MinIO (S3) usado pelos estágios do pipeline do data lake.

    Concentra as primitivas de conexão/transferência (listar, amostrar por Range, baixar,
    subir, tags de mascaramento, multipart). A lógica de transformação (mascaramento,
    conversão Parquet, carga DuckDB) mora nos scripts, que chamam este cliente para o I/O.
    """

    # upload multipart: 8 MB por parte, 2 conexões — estável em LAN e rápido o suficiente.
    _DEFAULT_TRANSFER = TransferConfig(
        multipart_chunksize=8 * 1024 * 1024, max_concurrency=2
    )

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None,
    ) -> None:
        endpoint = endpoint or os.environ["MINIO_ENDPOINT"]
        if not endpoint.startswith("http"):
            endpoint = f"http://{endpoint}"
        self.bucket = bucket or os.environ["MINIO_BUCKET"]
        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key or os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=secret_key or os.environ["MINIO_SECRET_KEY"],
        )
        logging.info(
            f"[cliente_minio.py] Initialized ClienteMinio endpoint={endpoint} "
            f"bucket={self.bucket}"
        )

    # Bucket / multipart
    def garantir_bucket(self) -> None:
        """Cria o bucket se não existir. A pasta raw/ surge no primeiro upload."""
        try:
            self.s3.head_bucket(Bucket=self.bucket)
            logging.info(f"[cliente_minio.py] Bucket '{self.bucket}' já existe.")
        except Exception:
            self.s3.create_bucket(Bucket=self.bucket)
            logging.info(f"[cliente_minio.py] Bucket '{self.bucket}' criado.")

    def abortar_multiparts_incompletos(self) -> None:
        """Aborta multipart uploads incompletos no bucket — evita locks no MinIO."""
        abortados = 0
        paginator = self.s3.get_paginator("list_multipart_uploads")
        try:
            for page in paginator.paginate(Bucket=self.bucket):
                for upload in page.get("Uploads", []):
                    self.s3.abort_multipart_upload(
                        Bucket=self.bucket, Key=upload["Key"], UploadId=upload["UploadId"]
                    )
                    abortados += 1
        except Exception as e:
            logging.warning(
                f"[cliente_minio.py] Não foi possível listar multipart uploads: {e}"
            )
            return
        if abortados:
            logging.info(
                f"[cliente_minio.py] {abortados} multipart upload(s) incompleto(s) "
                f"abortado(s)."
            )
        else:
            logging.info("[cliente_minio.py] Nenhum multipart incompleto encontrado.")

    def abortar_multiparts_chave(self, key: str) -> None:
        """Aborta uploads incompletos de uma chave específica antes de retentar."""
        try:
            paginator = self.s3.get_paginator("list_multipart_uploads")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=key):
                for upload in page.get("Uploads", []):
                    if upload["Key"] == key:
                        self.s3.abort_multipart_upload(
                            Bucket=self.bucket,
                            Key=upload["Key"],
                            UploadId=upload["UploadId"],
                        )
        except Exception:
            pass

    # Leitura
    def listar_objetos(self, prefix: str) -> Iterator[Tuple[str, int]]:
        """Itera (key, size) sobre os objetos do bucket sob `prefix`."""
        pag = self.s3.get_paginator("list_objects_v2")
        for page in pag.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"], obj["Size"]

    def sample_bytes(self, key: str, n: int = 65536) -> bytes:
        """Primeiros n bytes do objeto (HTTP Range) — p/ detecção de encoding/dialeto."""
        return self.s3.get_object(
            Bucket=self.bucket, Key=key, Range=f"bytes=0-{n - 1}"
        )["Body"].read()

    def baixar_para_tempfile(
        self, key: str, suffix: str = "", tmpdir: Optional[str] = None
    ) -> str:
        """Baixa o objeto para um tempfile em disco (evita OOM em arquivos grandes)."""
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=tmpdir)
        try:
            self.s3.download_fileobj(self.bucket, key, tmp)
            tmp.flush()
        finally:
            tmp.close()
        return tmp.name

    # Escrita
    def upload_arquivo(
        self,
        path: str,
        key: str,
        tentativas: int = 3,
        transfer_config: Optional[TransferConfig] = None,
    ) -> str:
        """Sobe um arquivo local para `key`, com retry e limpeza de multipart no meio."""
        config = transfer_config or self._DEFAULT_TRANSFER
        import time

        for tentativa in range(tentativas):
            try:
                with open(path, "rb") as f:
                    self.s3.upload_fileobj(f, self.bucket, key, Config=config)
                return key
            except Exception as e:
                if tentativa == tentativas - 1:
                    raise
                espera = 15 * (tentativa + 1)
                logging.warning(
                    f"[cliente_minio.py] Upload falhou (tentativa {tentativa + 1}/"
                    f"{tentativas}): {e}. Aguardando {espera}s..."
                )
                self.abortar_multiparts_chave(key)
                time.sleep(espera)
        raise RuntimeError("unreachable")

    def subir_stream_com_hash(
        self,
        key: str,
        stream_factory,
        tentativas: int = 3,
        transfer_config: Optional[TransferConfig] = None,
    ) -> Tuple[str, str]:
        """Upload a partir de um stream (sem temp file). Retorna (key, sha256).

        stream_factory é um callable que retorna um context manager com .read()."""
        config = transfer_config or self._DEFAULT_TRANSFER
        import time

        for tentativa in range(tentativas):
            try:
                with stream_factory() as src:
                    reader = _HashingReader(src)
                    self.s3.upload_fileobj(reader, self.bucket, key, Config=config)
                return key, reader.hexdigest
            except Exception as e:
                if tentativa == tentativas - 1:
                    raise
                espera = 15 * (tentativa + 1)
                logging.warning(
                    f"[cliente_minio.py] Upload stream falhou (tentativa {tentativa + 1}/"
                    f"{tentativas}): {e}. Aguardando {espera}s..."
                )
                self.abortar_multiparts_chave(key)
                time.sleep(espera)
        raise RuntimeError("unreachable")

    def put_object(self, key: str, body: bytes) -> None:
        """Grava bytes diretamente num objeto (usado pela auditoria)."""
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)

    # Tags de mascaramento
    def get_tagging(self, key: str) -> List[dict]:
        try:
            resp = self.s3.get_object_tagging(Bucket=self.bucket, Key=key)
            return resp.get("TagSet", [])
        except Exception:
            return []

    def esta_mascarado(self, key: str) -> bool:
        return any(
            t["Key"] == "masked" and t["Value"] == "true" for t in self.get_tagging(key)
        )

    def marcar_mascarado(self, key: str, execution_id: str, masked_hash: str) -> None:
        self.s3.put_object_tagging(
            Bucket=self.bucket,
            Key=key,
            Tagging={
                "TagSet": [
                    {"Key": "masked", "Value": "true"},
                    {"Key": "execution_id", "Value": execution_id},
                    {"Key": "masked_hash", "Value": masked_hash},
                ]
            },
        )
