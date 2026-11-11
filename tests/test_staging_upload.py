"""Upload para a staging: aguenta o MinIO fora por um tempo e não perde a conversão."""

import hashlib
import sys

import pyarrow.parquet as pq
import pytest
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError, EndpointConnectionError

from tests.conftest import fresh_import

KEY = "raw/sftp/fabrica/GEFUS/CadUnico/PESSOA.TXT"
CONTEUDO = b"CPF;NOME\n" + b"".join(b"%011d;tok%d\n" % (i, i) for i in range(50))


def _erro_s3(codigo: str, status: int) -> ClientError:
    resposta = {"Error": {"Code": codigo}, "ResponseMetadata": {"HTTPStatusCode": status}}
    return ClientError(resposta, "UploadPart")


@pytest.fixture
def staging(monkeypatch, tmp_path):
    # pelo ambiente, não por monkeypatch: cada módulo do pacote copia o TMPDIR no import
    return fresh_import(monkeypatch, "conversao.processamento", LAKE_TMPDIR=str(tmp_path))


@pytest.fixture
def cliente(staging, monkeypatch):
    cliente_minio = sys.modules["cliente_minio"]
    esperas = []
    monkeypatch.setattr(cliente_minio.time, "sleep", esperas.append)

    class S3:
        def __init__(self):
            self.falhas, self.enviados, self.abortados = [], {}, 0

        def upload_fileobj(self, f, bucket, key, Config=None):
            if self.falhas:
                raise self.falhas.pop(0)
            self.enviados[key] = f.read()

        def get_paginator(self, _nome):
            raise EndpointConnectionError(endpoint_url="http://minio:9000")

    c = cliente_minio.ClienteMinio.__new__(cliente_minio.ClienteMinio)
    c.s3, c.bucket, c.esperas = S3(), "teste", esperas
    return c


@pytest.fixture
def arquivo(tmp_path):
    path = tmp_path / "x.parquet"
    path.write_bytes(b"dados")
    return str(path)


def test_espera_o_minio_voltar(cliente, arquivo):
    cliente.s3.falhas = [EndpointConnectionError(endpoint_url="http://minio")] * 3
    assert cliente.upload_arquivo(arquivo, "staging/x.parquet") == "staging/x.parquet"
    assert cliente.esperas == [30, 60, 120]
    assert cliente.s3.enviados == {"staging/x.parquet": b"dados"}


def test_desiste_depois_de_uns_30_minutos(cliente, arquivo):
    cliente.s3.falhas = [EndpointConnectionError(endpoint_url="http://minio")] * 10
    with pytest.raises(EndpointConnectionError):
        cliente.upload_arquivo(arquivo, "staging/x.parquet")
    assert cliente.esperas == [30, 60, 120, 240, 300, 300, 300, 300, 300]
    assert 30 * 60 <= sum(cliente.esperas) <= 35 * 60


def test_erro_5xx_e_slowdown_sao_retentados(cliente, arquivo):
    cliente.s3.falhas = [_erro_s3("InternalError", 500), _erro_s3("SlowDown", 400)]
    cliente.upload_arquivo(arquivo, "staging/x.parquet")
    assert len(cliente.esperas) == 2


@pytest.mark.parametrize("embrulhado", [False, True])
def test_erro_definitivo_nao_espera(cliente, arquivo, embrulhado):
    erro = _erro_s3("AccessDenied", 403)
    if embrulhado:
        try:
            try:
                raise erro
            except ClientError as e:
                raise S3UploadFailedError(f"Failed to upload: {e}")
        except S3UploadFailedError as e:
            erro = e
    cliente.s3.falhas = [erro]
    with pytest.raises(type(erro)):
        cliente.upload_arquivo(arquivo, "staging/x.parquet")
    assert cliente.esperas == []


class MinioFalso:
    def __init__(self, tmp_path, falhar):
        self.tmp_path, self.falhar, self.enviados = tmp_path, falhar, {}

    def sample_bytes(self, key):
        return CONTEUDO[:65536]

    def baixar_para_tempfile(self, key, ext, tmpdir):
        path = self.tmp_path / f"download{ext}"
        path.write_bytes(CONTEUDO)
        return str(path)

    def upload_arquivo(self, path, key):
        if self.falhar:
            raise EndpointConnectionError(endpoint_url="http://minio")
        self.enviados[key] = pq.read_table(path)


def _processar(staging, minio):
    liberado = {hashlib.md5(CONTEUDO).hexdigest()}
    return staging.processar_objeto(minio, KEY, "exec", True, set(), 1000, "skip", liberado)[0]


def _pendentes(tmp_path):
    pasta = tmp_path / "staging_pendente"
    if not pasta.exists():
        return []
    return sorted(p.name.split(".", 1)[1] for p in pasta.iterdir())


def test_upload_que_falha_guarda_o_parquet(staging, tmp_path):
    rec = _processar(staging, MinioFalso(tmp_path, falhar=True))
    assert rec["status"] == "error"
    assert _pendentes(tmp_path) == ["parquet", "parquet.json"]
    # só sobra o pendente: download e parquet temporário saem
    assert sorted(p.name for p in tmp_path.iterdir()) == ["staging_pendente"]


def test_proxima_execucao_so_reenvia(staging, tmp_path, monkeypatch):
    _processar(staging, MinioFalso(tmp_path, falhar=True))

    def _nao_converte(*_a, **_k):
        raise AssertionError("não devia reconverter")

    monkeypatch.setattr(staging, "_converter_tabular", _nao_converte)
    minio = MinioFalso(tmp_path, falhar=False)
    rec = _processar(staging, minio)

    assert rec["status"] == "converted"
    assert (rec["n_linhas"], rec["n_colunas"]) == (50, 2)
    assert rec["column_map"] == {"CPF": "cpf", "NOME": "nome"}
    tabela = minio.enviados["staging/sftp/fabrica/GEFUS/CadUnico/PESSOA.parquet"]
    assert tabela.num_rows == 50
    assert _pendentes(tmp_path) == []


def test_pendente_de_outro_conteudo_e_ignorado(staging, tmp_path):
    _processar(staging, MinioFalso(tmp_path, falhar=True))
    global CONTEUDO
    original, CONTEUDO = CONTEUDO, CONTEUDO + b"99999999999;novo\n"
    try:
        minio = MinioFalso(tmp_path, falhar=False)
        rec = _processar(staging, minio)
    finally:
        CONTEUDO = original
    # hash novo: converte de novo e o pendente antigo não fica ocupando disco
    assert rec["n_linhas"] == 51
    assert _pendentes(tmp_path) == []


def test_sucesso_nao_deixa_pendente(staging, tmp_path):
    rec = _processar(staging, MinioFalso(tmp_path, falhar=False))
    assert rec["status"] == "converted"
    assert _pendentes(tmp_path) == []
