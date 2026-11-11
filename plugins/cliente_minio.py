"""Cliente MinIO (S3-compatível) para o data lake do conjuntura contínuo.

Camadas no bucket `data-lake-mcid` (full-refresh, sem partição de data):
    raw/<fonte>/<dado>.json        -> payload cru da API (Extração p/ MinIO)
    staging/<fonte>/<dado>.parquet -> parquet JÁ TIPADO (Transformação)

A carga do parquet para o Postgres (silver) NÃO passa por aqui: é o pg_duckdb,
via `select * from read_parquet('s3://...')` nos modelos dbt.

Usa `boto3` (já presente no ambiente) apontando para o endpoint do MinIO. As
credenciais vêm das envs MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY /
MINIO_BUCKET.

Duas interfaces convivem aqui, com públicos distintos:

* as **funções** (`upload_raw_bytes`, `upload_staging_parquet`, ...) atendem o conjuntura
  contínuo, onde cada dado é um par (fonte, dado) e a chave é derivada desse par;
* a classe **`ClienteMinio`** atende o pipeline do data lake do SFTP, onde os arquivos
  chegam com nome próprio e o I/O precisa de streaming, multipart, hash e tags.
"""

import hashlib
import io
import json
import logging
import os
import shutil
import tempfile
import time
from typing import Any, Callable, Iterator, List, Optional, Tuple

import boto3
from boto3.s3.transfer import TransferConfig

BUCKET = os.environ.get("MINIO_BUCKET", "data-lake-mcid")
# Triagens do mascaramento que liberam o objeto para a staging sem reescrevê-lo.
TRIAGEM_LIBERADA = ("skipped_no_pii", "skipped_no_header", "skipped_unsupported")


def get_s3_client() -> Any:
    """Cria um client S3 (boto3) apontando para o MinIO."""
    endpoint = os.environ["MINIO_ENDPOINT"]
    if not endpoint.startswith(("http://", "https://")):
        endpoint = f"http://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
    )


def raw_key(fonte: str, dado: str, ext: str = "json") -> str:
    """Chave do objeto raw: raw/<fonte>/<dado>.<ext> (json/csv/txt/xlsx)."""
    return f"raw/{fonte}/{dado}.{ext}"


def fallback_json_key(fonte: str, dado: str) -> str:
    """Chave do json de conveniência: raw/<fonte>/fallback_json/<dado>.json.

    Usado quando o raw nativo NÃO é json (xlsx/csv): guarda-se também os
    registros já transformados em json, dentro de fallback_json/.
    """
    return f"raw/{fonte}/fallback_json/{dado}.json"


def staging_key(fonte: str, dado: str) -> str:
    """Chave do objeto staging: staging/<fonte>/<dado>.parquet."""
    return f"staging/{fonte}/{dado}.parquet"


def s3_staging_uri(fonte: str, dado: str) -> str:
    """URI s3:// do parquet de staging (para uso no read_parquet do pg_duckdb)."""
    return f"s3://{BUCKET}/{staging_key(fonte, dado)}"


def upload_raw_bytes(
    fonte: str,
    dado: str,
    data: bytes,
    ext: str,
    content_type: str = "application/octet-stream",
) -> None:
    """Etapa 01: sobe um arquivo cru (csv/txt/xlsx/...) para a raw (full-refresh)."""
    key = raw_key(fonte, dado, ext)
    get_s3_client().put_object(
        Bucket=BUCKET, Key=key, Body=data, ContentType=content_type
    )
    logging.info(f"[cliente_minio] raw enviado: s3://{BUCKET}/{key} ({len(data)} bytes)")


def download_raw_bytes(fonte: str, dado: str, ext: str = "json") -> bytes:
    """Lê os bytes crus do objeto raw do MinIO."""
    key = raw_key(fonte, dado, ext)
    obj = get_s3_client().get_object(Bucket=BUCKET, Key=key)
    body: bytes = obj["Body"].read()
    return body


def upload_raw_json(fonte: str, dado: str, payload: Any) -> None:
    """Etapa 01: sobe o payload cru da API como json (full-refresh).

    Usa default=str para tolerar tipos não serializáveis (numpy, pandas
    Timestamp, Decimal) que aparecem quando os registros vêm de um DataFrame.
    """
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    upload_raw_bytes(fonte, dado, body, ext="json", content_type="application/json")


def download_raw_json(fonte: str, dado: str) -> Any:
    """Lê de volta o json raw do MinIO (entrada do passo de tipagem/parquet)."""
    return json.loads(download_raw_bytes(fonte, dado, ext="json").decode("utf-8"))


def upload_fallback_json(fonte: str, dado: str, payload: Any) -> None:
    """Sobe os registros transformados como json em raw/<fonte>/fallback_json/.

    Complementa o raw nativo (xlsx/csv) com um json de conveniência.
    """
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    key = fallback_json_key(fonte, dado)
    get_s3_client().put_object(
        Bucket=BUCKET, Key=key, Body=body, ContentType="application/json"
    )
    logging.info(
        f"[cliente_minio] fallback_json enviado: s3://{BUCKET}/{key} ({len(body)} bytes)"
    )


def upload_staging_parquet(fonte: str, dado: str, data: bytes) -> None:
    """1.2 Transformação: sobe o parquet TIPADO para a staging (full-refresh)."""
    key = staging_key(fonte, dado)
    get_s3_client().put_object(
        Bucket=BUCKET, Key=key, Body=data, ContentType="application/octet-stream"
    )
    logging.info(
        f"[cliente_minio] staging enviado: s3://{BUCKET}/{key} ({len(data)} bytes)"
    )


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


def _codigo_erro(exc: Exception) -> str:
    # erros de rede do botocore têm `response = None`
    resposta = getattr(exc, "response", None) or {}
    return str(resposta.get("Error", {}).get("Code", ""))


def _erro_definitivo(exc: BaseException) -> bool:
    """Erro 4xx do S3 (fora SlowDown/RequestTimeout): retentar não adianta."""
    # o boto3 às vezes embrulha o ClientError em S3UploadFailedError
    for erro in (exc, exc.__context__):
        resposta = getattr(erro, "response", None)
        if resposta:
            status = resposta.get("ResponseMetadata", {}).get("HTTPStatusCode") or 0
            codigo = _codigo_erro(erro)
            return status < 500 and codigo not in ("SlowDown", "RequestTimeout")
    return False


class LeitorObjeto(io.RawIOBase):
    """Lê um objeto, ou o intervalo [inicio, fim), sem passar pelo disco.

    Retoma por Range se a conexão cair. O ETag fixa a versão lida: se o objeto mudar no
    meio, a leitura falha.
    """

    def __init__(
        self,
        s3: Any,
        bucket: str,
        key: str,
        tentativas: int = 20,
        inicio: int = 0,
        fim: Optional[int] = None,
        etag: Optional[str] = None,
    ) -> None:
        head = s3.head_object(Bucket=bucket, Key=key)
        self._inicio = inicio
        self._fim = int(head["ContentLength"]) if fim is None else fim
        self.tamanho = self._fim - inicio
        self._s3, self._bucket, self._key = s3, bucket, key
        self._etag = etag or head["ETag"]
        self._tentativas = tentativas
        self._pos = 0
        self._corpo: Any = None
        self.md5 = hashlib.md5()

    def readable(self) -> bool:
        return True

    def readinto(self, destino: Any) -> int:
        if self._pos >= self.tamanho:
            return 0
        falhas = 0
        while True:
            try:
                if self._corpo is None:
                    self._corpo = self._s3.get_object(
                        Bucket=self._bucket,
                        Key=self._key,
                        Range=f"bytes={self._inicio + self._pos}-{self._fim - 1}",
                        IfMatch=self._etag,
                    )["Body"]
                dados = self._corpo.read(min(len(destino), self.tamanho - self._pos))
                if not dados:
                    raise IOError(f"conexão fechada em {self._pos}/{self.tamanho} bytes")
                break
            except Exception as exc:
                self._corpo = None
                falhas += 1
                if _codigo_erro(exc) in ("PreconditionFailed", "412") or (
                    falhas >= self._tentativas
                ):
                    raise
                logging.warning(
                    "Leitura de %s interrompida em %d/%d bytes (%s); retomando (%d/%d)",
                    self._key,
                    self._pos,
                    self.tamanho,
                    exc,
                    falhas,
                    self._tentativas,
                )
                time.sleep(min(5 * falhas, 60))
        n = len(dados)
        destino[:n] = dados
        self._pos += n
        self.md5.update(dados)
        return n


class EscritorMultipart(io.RawIOBase):
    """Sobe o que é escrito em partes de um multipart upload.

    O objeto só muda em `concluir()`; `abortar()` descarta tudo e o original fica.
    """

    def __init__(
        self, s3: Any, bucket: str, key: str, tamanho_parte: int, tentativas: int = 5
    ) -> None:
        self._s3, self._bucket, self._key = s3, bucket, key
        self._tamanho_parte = tamanho_parte
        self._tentativas = tentativas
        self._upload_id = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
        self._buf = bytearray()
        self._partes: List[dict] = []
        self._abortado = False
        self.enviado = 0
        self.md5 = hashlib.md5()

    def writable(self) -> bool:
        return True

    def write(self, dados: Any) -> int:
        # o flush dos wrappers no garbage collector não pode reenviar depois do abort
        if self._abortado:
            raise IOError(f"multipart de {self._key} já abortado")
        self._buf += dados
        self.md5.update(dados)
        while len(self._buf) >= self._tamanho_parte:
            self._enviar(bytes(self._buf[: self._tamanho_parte]))
            del self._buf[: self._tamanho_parte]
        return len(dados)

    def _enviar(self, dados: bytes) -> None:
        numero = len(self._partes) + 1
        for tentativa in range(1, self._tentativas + 1):
            try:
                resp = self._s3.upload_part(
                    Bucket=self._bucket,
                    Key=self._key,
                    UploadId=self._upload_id,
                    PartNumber=numero,
                    Body=dados,
                )
                break
            except Exception as exc:
                # upload sumiu (abortado por fora): nova tentativa não resolve
                if _codigo_erro(exc) == "NoSuchUpload" or tentativa == self._tentativas:
                    raise
                logging.warning(
                    "Parte %d de %s falhou (%s); tentativa %d/%d",
                    numero,
                    self._key,
                    exc,
                    tentativa,
                    self._tentativas,
                )
                time.sleep(15 * tentativa)
        self._partes.append({"ETag": resp["ETag"], "PartNumber": numero})
        self.enviado += len(dados)
        logging.info(
            "Parte %d de %s enviada (%.2f GiB)", numero, self._key, self.enviado / 2**30
        )

    def concluir(self) -> None:
        if self._buf or not self._partes:
            self._enviar(bytes(self._buf))
            self._buf.clear()
        self._s3.complete_multipart_upload(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=self._upload_id,
            MultipartUpload={"Parts": self._partes},
        )

    def abortar(self) -> None:
        self._abortado = True
        try:
            self._s3.abort_multipart_upload(
                Bucket=self._bucket, Key=self._key, UploadId=self._upload_id
            )
        except Exception as exc:
            logging.warning("Não abortou o multipart de %s: %s", self._key, exc)


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

    def sample_bytes(self, key: str, n: int = 65536, inicio: int = 0) -> bytes:
        """n bytes do objeto a partir de `inicio` (HTTP Range)."""
        return self.s3.get_object(
            Bucket=self.bucket, Key=key, Range=f"bytes={inicio}-{inicio + n - 1}"
        )["Body"].read()

    def baixar_para_tempfile(
        self, key: str, suffix: str = "", tmpdir: Optional[str] = None
    ) -> str:
        """Baixa para disco com retomada por ``Range`` em caso de queda de conexão.

        O nome determinístico mantém o parcial entre tentativas e execuções. Isso é
        importante para os arquivos grandes do CadÚnico, para os quais reiniciar um
        download de dezenas de GB após uma oscilação da VPN é proibitivo.
        """
        pasta = tmpdir or tempfile.gettempdir()
        os.makedirs(pasta, exist_ok=True)
        identificador = hashlib.sha256(key.encode("utf-8")).hexdigest()[:20]
        final = os.path.join(pasta, f"minio-download-{identificador}{suffix}")
        path = final + ".part"
        tamanho_remoto = int(
            self.s3.head_object(Bucket=self.bucket, Key=key)["ContentLength"]
        )

        if os.path.exists(final):
            os.unlink(final)
        if os.path.exists(path) and os.path.getsize(path) > tamanho_remoto:
            os.unlink(path)
        open(path, "ab").close()  # objeto vazio também precisa do arquivo

        falhas = 0
        while True:
            inicio = os.path.getsize(path) if os.path.exists(path) else 0
            if inicio == tamanho_remoto:
                # o leitor decide o formato pela extensão: o openpyxl recusa `.part`
                os.replace(path, final)
                return final
            try:
                resposta = self.s3.get_object(
                    Bucket=self.bucket, Key=key, Range=f"bytes={inicio}-"
                )
                with open(path, "ab") as destino:
                    shutil.copyfileobj(resposta["Body"], destino, 8 * 1024 * 1024)
                    destino.flush()
                    os.fsync(destino.fileno())
                falhas = 0
            except Exception as exc:
                falhas += 1
                logging.warning(
                    "Download interrompido em %d/%d bytes (%s); retomando (%d/20)",
                    inicio,
                    tamanho_remoto,
                    exc,
                    falhas,
                )
                if falhas >= 20:
                    raise
                time.sleep(min(5 * falhas, 60))

    def abrir_leitura(
        self,
        key: str,
        inicio: int = 0,
        fim: Optional[int] = None,
        etag: Optional[str] = None,
    ) -> LeitorObjeto:
        """Stream de leitura do objeto, com md5 do que foi lido em `.md5`."""
        return LeitorObjeto(self.s3, self.bucket, key, inicio=inicio, fim=fim, etag=etag)

    def md5_objeto(self, key: str) -> str:
        """md5 do conteúdo, lido em streaming e sem gravar nada."""
        leitor = self.abrir_leitura(key)
        bloco = bytearray(8 * 1024 * 1024)
        while leitor.readinto(bloco):
            pass
        return leitor.md5.hexdigest()

    # Escrita
    def abrir_escrita(self, key: str, tamanho_estimado: int = 0) -> EscritorMultipart:
        """Stream de escrita em multipart; nada muda em `key` até `concluir()`."""
        # 64 MB por parte, crescendo para não passar das 10.000 partes do S3
        tamanho_parte = max(64 * 1024 * 1024, -(-tamanho_estimado // 9000))
        return EscritorMultipart(self.s3, self.bucket, key, tamanho_parte)

    def juntar_objetos(self, chaves: List[str], destino: str) -> None:
        """Concatena os objetos em `destino` no próprio MinIO, sem baixar nada.

        Cada origem vira uma parte de multipart: até 5 GiB, e todas menos a última com
        pelo menos 5 MiB.
        """
        upload_id = self.s3.create_multipart_upload(Bucket=self.bucket, Key=destino)[
            "UploadId"
        ]
        try:
            partes = []
            for numero, chave in enumerate(chaves, 1):
                resp = self.s3.upload_part_copy(
                    Bucket=self.bucket,
                    Key=destino,
                    UploadId=upload_id,
                    PartNumber=numero,
                    CopySource={"Bucket": self.bucket, "Key": chave},
                )
                partes.append(
                    {"ETag": resp["CopyPartResult"]["ETag"], "PartNumber": numero}
                )
            self.s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=destino,
                UploadId=upload_id,
                MultipartUpload={"Parts": partes},
            )
        except BaseException:
            self.s3.abort_multipart_upload(
                Bucket=self.bucket, Key=destino, UploadId=upload_id
            )
            raise

    def apagar_prefixo(self, prefix: str) -> int:
        """Apaga todos os objetos sob `prefix`. Retorna quantos apagou."""
        chaves = [k for k, _ in self.listar_objetos(prefix)]
        for chave in chaves:
            self.s3.delete_object(Bucket=self.bucket, Key=chave)
        return len(chaves)

    def upload_arquivo(
        self,
        path: str,
        key: str,
        tentativas: int = 10,
        transfer_config: Optional[TransferConfig] = None,
    ) -> str:
        """Sobe um arquivo local para `key`, com retry e limpeza de multipart no meio.

        A espera dobra a cada falha (30 s até 5 min): o default aguenta ~30 min de
        MinIO fora. Erro definitivo (permissão, bucket inexistente) não é retentado.
        """
        config = transfer_config or self._DEFAULT_TRANSFER

        for tentativa in range(tentativas):
            try:
                with open(path, "rb") as f:
                    self.s3.upload_fileobj(f, self.bucket, key, Config=config)
                return key
            except Exception as e:
                if tentativa == tentativas - 1 or _erro_definitivo(e):
                    self.abortar_multiparts_chave(key)
                    raise
                espera = min(30 * 2**tentativa, 300)
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
        progress_cb: Optional[Callable[[int], None]] = None,
    ) -> Tuple[str, str]:
        """Upload a partir de um stream (sem temp file). Retorna (key, sha256).

        stream_factory é um callable que retorna um context manager com .read()."""
        config = transfer_config or self._DEFAULT_TRANSFER
        import time

        for tentativa in range(tentativas):
            try:
                with stream_factory() as src:
                    reader = _HashingReader(src)
                    self.s3.upload_fileobj(
                        reader, self.bucket, key, Config=config, Callback=progress_cb
                    )
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

    def marcar_triagem(self, key: str, execution_id: str, status: str) -> None:
        """Objeto que o mascaramento liberou sem reescrever (ex.: sem PII)."""
        self.s3.put_object_tagging(
            Bucket=self.bucket,
            Key=key,
            Tagging={
                "TagSet": [
                    {"Key": "triagem", "Value": status},
                    {"Key": "execution_id", "Value": execution_id},
                ]
            },
        )

    def pronto_para_staging(self, key: str) -> bool:
        # regravar o objeto apaga as tags: reingerido, ele volta a esperar o mascaramento
        tags = {t["Key"]: t["Value"] for t in self.get_tagging(key)}
        return tags.get("masked") == "true" or tags.get("triagem") in TRIAGEM_LIBERADA
