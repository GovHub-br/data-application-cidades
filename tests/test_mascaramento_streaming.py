"""Mascaramento de CSV/TXT direto do MinIO para o MinIO, sem disco."""

import csv
import hashlib
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import io
import itertools
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import TEST_ENV, fresh_import

_SEGREDO = "segredo-de-teste"


# the package copies the env at import; nothing else reads it, so it can be undone
with pytest.MonkeyPatch.context() as _mp:
    modulo = fresh_import(_mp, "mascarar_minio", "mascaramento", MASKING_HMAC_SECRET=_SEGREDO)

import cliente_minio  # noqa: E402
from mascaramento import paralelo as mod_paralelo, regras, tabular  # noqa: E402


class _Erro(Exception):
    def __init__(self, codigo: str) -> None:
        super().__init__(codigo)
        self.response = {"Error": {"Code": codigo}}


class _ErroDeRede(Exception):
    """Como os erros de stream do botocore, que vêm com `response = None`."""

    response = None


class _Corpo:
    """Body do get_object; `cair_em` simula a conexão caindo depois de N bytes."""

    def __init__(self, dados: bytes, cair_em: int = -1) -> None:
        self._dados, self._pos, self._cair_em = dados, 0, cair_em

    def read(self, n: int = -1) -> bytes:
        if 0 <= self._cair_em <= self._pos:
            raise _ErroDeRede("conexão caiu")
        fim = len(self._dados) if n < 0 else self._pos + n
        if self._cair_em >= 0:
            fim = min(fim, self._cair_em)
        bloco = self._dados[self._pos : fim]
        self._pos += len(bloco)
        return bloco


class S3Falso:
    def __init__(self) -> None:
        self.objetos: dict = {}
        self.tags: dict = {}
        self.uploads: dict = {}
        self.abortados = 0
        self.multipart_abertos = 0
        self._ids = itertools.count()
        self.quedas: list = []  # offsets em que a leitura em streaming cai
        self.falhar_upload_part = False
        self.ao_ler = None  # chamado a cada get_object do streaming

    def _etag(self, key: str) -> str:
        return '"' + hashlib.md5(self.objetos[key]).hexdigest() + '"'

    def head_object(self, Bucket, Key):
        return {"ContentLength": len(self.objetos[Key]), "ETag": self._etag(Key)}

    def get_object(self, Bucket, Key, Range=None, IfMatch=None):
        dados = self.objetos[Key]
        inicio, fim = Range.split("=")[1].split("-") if Range else ("0", "")
        if IfMatch is None:  # amostra do cabeçalho
            return {"Body": _Corpo(dados[int(inicio) : int(fim or len(dados)) + 1])}
        if self.ao_ler:
            self.ao_ler(Key)
            dados = self.objetos[Key]
        if IfMatch != self._etag(Key):
            raise _Erro("PreconditionFailed")
        cair_em = self.quedas.pop(0) - int(inicio) if self.quedas else -1
        return {"Body": _Corpo(dados[int(inicio) : int(fim or len(dados) - 1) + 1], cair_em)}

    def create_multipart_upload(self, Bucket, Key):
        upload_id = f"u{next(self._ids)}"
        self.multipart_abertos += 1
        self.uploads[upload_id] = {"key": Key, "partes": {}}
        return {"UploadId": upload_id}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        if self.falhar_upload_part:
            raise _ErroDeRede("upload caiu")
        self.uploads[UploadId]["partes"][PartNumber] = Body
        return {"ETag": f"e{PartNumber}"}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        upload = self.uploads.pop(UploadId)
        numeros = [p["PartNumber"] for p in MultipartUpload["Parts"]]
        self.objetos[Key] = b"".join(upload["partes"][n] for n in numeros)

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.uploads.pop(UploadId, None)
        self.abortados += 1

    def upload_part_copy(self, Bucket, Key, UploadId, PartNumber, CopySource):
        self.uploads[UploadId]["partes"][PartNumber] = self.objetos[CopySource["Key"]]
        return {"CopyPartResult": {"ETag": f"c{PartNumber}"}}

    def delete_object(self, Bucket, Key):
        self.objetos.pop(Key, None)
        self.tags.pop(Key, None)

    def get_paginator(self, nome):
        objetos = self.objetos

        class _Paginador:
            def paginate(self, Bucket, Prefix):
                chaves = sorted(k for k in objetos if k.startswith(Prefix))
                yield {"Contents": [{"Key": k, "Size": len(objetos[k])} for k in chaves]}

        return _Paginador()

    def get_object_tagging(self, Bucket, Key):
        if Key not in self.objetos:
            raise _Erro("NoSuchKey")
        return {"TagSet": self.tags.get(Key, [])}

    def put_object_tagging(self, Bucket, Key, Tagging):
        # como no MinIO do lake: tag só em raw/
        if not Key.startswith("raw/"):
            raise _Erro("AccessDenied")
        self.tags[Key] = Tagging["TagSet"]

    def put_object(self, Bucket, Key, Body):
        # como no S3: o objeto novo não herda as tags do anterior
        self.objetos[Key] = Body
        self.tags.pop(Key, None)


@pytest.fixture
def minio(monkeypatch):
    monkeypatch.setattr(cliente_minio.time, "sleep", lambda _: None)
    cliente = cliente_minio.ClienteMinio.__new__(cliente_minio.ClienteMinio)
    cliente.s3 = S3Falso()
    cliente.bucket = "teste"
    return cliente


KEY = "raw/sftp/fabrica/GEFUS/CadUnico/PESSOA.TXT"

# cp1252 com acento, campo com delimitador e com quebra de linha, CRLF e CPF formatado
CONTEUDO = (
    "NU_CPF_PESSOA;NO_PESSOA;DT_NASC_PESSOA;NU_IDENTIDADE_PESSOA;DS_MOTIVO\r\n"
    '123.456.789-09;José da Conceição;01/02/1980;12345;"tem ; dentro"\r\n'
    '00012345678;Maria;;;"duas\r\nlinhas"\r\n'
    ";;03/04/1990;;sem identificador\r\n"
).encode("cp1252")


def _via_arquivo(tmp_path: Path, conteudo: bytes) -> bytes:
    src, dst = tmp_path / "src.txt", tmp_path / "dst.txt"
    src.write_bytes(conteudo)
    tabular._mascarar_tabular(str(src), str(dst), ";", "\r\n", False, "cp1252")
    return dst.read_bytes()


def _processar(minio, apply=True, masked_hashes=None):
    return modulo.processar_objeto(minio, "", KEY, "exec-teste", apply, masked_hashes or set())


def test_stream_gera_os_mesmos_bytes_que_o_caminho_em_disco(minio, tmp_path):
    minio.s3.objetos[KEY] = CONTEUDO

    rec = _processar(minio)

    assert rec["status"] == "masked"
    assert minio.s3.objetos[KEY] == _via_arquivo(tmp_path, CONTEUDO)
    assert rec["registros_total"] == 3
    assert rec["registros_alterados"] == 2
    assert {c["column"] for c in rec["masked_columns"]} == {
        "NU_CPF_PESSOA",
        "NO_PESSOA",
        "NU_IDENTIDADE_PESSOA",
    }


def test_stream_mascara_e_preserva_o_resto(minio):
    minio.s3.objetos[KEY] = CONTEUDO

    _processar(minio)

    texto = minio.s3.objetos[KEY].decode("cp1252")
    linhas = list(csv.reader(io.StringIO(texto, newline=""), delimiter=";"))
    assert linhas[0] == [
        "NU_CPF_PESSOA",
        "NO_PESSOA",
        "DT_NASC_PESSOA",
        "NU_IDENTIDADE_PESSOA",
        "DS_MOTIVO",
    ]
    cpf, nome, nasc, rg, motivo = linhas[1]
    assert cpf == regras._hmac_token("12345678909")
    assert (nome, rg) == ("***", "***")
    assert (nasc, motivo) == ("01/02/1980", "tem ; dentro")
    assert linhas[2][4] == "duas\r\nlinhas"
    assert linhas[3] == ["", "", "03/04/1990", "", "sem identificador"]


def test_stream_registra_hashes_e_tag(minio):
    minio.s3.objetos[KEY] = CONTEUDO

    rec = _processar(minio)

    assert rec["hash_before"] == hashlib.md5(CONTEUDO).hexdigest()
    assert rec["hash_after"] == hashlib.md5(minio.s3.objetos[KEY]).hexdigest()
    assert minio.esta_mascarado(KEY)
    assert not minio.s3.uploads


def test_dry_run_grava_previa_e_nao_toca_o_raw(minio):
    minio.s3.objetos[KEY] = CONTEUDO

    rec = _processar(minio, apply=False)

    assert rec["status"] == "dry_run"
    assert minio.s3.objetos[KEY] == CONTEUDO
    previa = "masked_dryrun/" + KEY[len("raw/") :]
    assert hashlib.md5(minio.s3.objetos[previa]).hexdigest() == rec["hash_after"]
    assert not minio.esta_mascarado(KEY)


def test_sem_pii_nao_reescreve_e_hash_cobre_o_arquivo_todo(minio):
    conteudo = b"CO_IBGE;QT_UNIDADES\n" + b"5300108;10\n" * 50000
    minio.s3.objetos[KEY] = conteudo

    rec = _processar(minio)

    assert rec["status"] == "skipped_no_pii"
    assert minio.s3.objetos[KEY] == conteudo
    assert rec["hash_before"] == hashlib.md5(conteudo).hexdigest()
    assert rec["hash_after"] is None
    assert not minio.s3.uploads


def test_hash_ja_mascarado_aborta_sem_reescrever(minio):
    minio.s3.objetos[KEY] = CONTEUDO

    rec = _processar(minio, masked_hashes={hashlib.md5(CONTEUDO).hexdigest()})

    assert rec["status"] == "skipped_already"
    assert rec["hash_before"] == hashlib.md5(CONTEUDO).hexdigest()
    assert minio.s3.objetos[KEY] == CONTEUDO
    # cópia de arquivo já mascarado não pode pagar o mascaramento e o envio inteiros
    assert minio.s3.multipart_abertos == 0


def test_falha_no_upload_mantem_o_original(minio):
    minio.s3.objetos[KEY] = CONTEUDO
    minio.s3.falhar_upload_part = True

    rec = _processar(minio)

    assert rec["status"] == "error"
    assert minio.s3.objetos[KEY] == CONTEUDO
    assert not minio.s3.uploads
    assert minio.s3.abortados == 1
    assert not minio.esta_mascarado(KEY)


def test_leitura_retoma_depois_de_queda(minio, tmp_path):
    minio.s3.objetos[KEY] = CONTEUDO
    minio.s3.quedas = [10, 70, 71]

    rec = _processar(minio)

    assert rec["status"] == "masked"
    assert minio.s3.objetos[KEY] == _via_arquivo(tmp_path, CONTEUDO)
    assert rec["hash_before"] == hashlib.md5(CONTEUDO).hexdigest()


def test_objeto_alterado_durante_a_leitura_falha(minio):
    minio.s3.objetos[KEY] = CONTEUDO
    minio.s3.quedas = [10]
    leituras = []

    def trocar_na_segunda(key):
        leituras.append(key)
        if len(leituras) == 2:
            minio.s3.objetos[KEY] = CONTEUDO + b"x;y;z;w;v\r\n"

    minio.s3.ao_ler = trocar_na_segunda

    rec = _processar(minio)

    assert rec["status"] == "error"
    assert minio.s3.objetos[KEY] == CONTEUDO + b"x;y;z;w;v\r\n"
    assert not minio.s3.uploads


def test_arquivo_sem_cabecalho_mascara_a_primeira_linha(minio, monkeypatch):
    minio.s3.objetos[KEY] = b"A;B;12345678909\nC;D;98765432100\n"
    monkeypatch.setitem(regras.COLUNAS_POR_POSICAO, KEY, {2: "cpf"})

    rec = _processar(minio)

    linhas = minio.s3.objetos[KEY].decode().splitlines()
    assert rec["status"] == "masked"
    assert rec["registros_total"] == 2
    assert linhas == [
        f"A;B;{regras._hmac_token('12345678909')}",
        f"C;D;{regras._hmac_token('98765432100')}",
    ]


def test_escritor_divide_em_partes_e_so_publica_no_fim(minio):
    minio.s3.objetos["k"] = b"antigo"
    escritor = cliente_minio.EscritorMultipart(minio.s3, "teste", "k", tamanho_parte=4)

    escritor.write(b"0123456789")
    assert minio.s3.objetos["k"] == b"antigo"
    escritor.concluir()

    assert minio.s3.objetos["k"] == b"0123456789"
    assert escritor.md5.hexdigest() == hashlib.md5(b"0123456789").hexdigest()


def test_parte_respeita_limite_de_partes(minio):
    pequeno = minio.abrir_escrita("a", tamanho_estimado=10)
    grande = minio.abrir_escrita("b", tamanho_estimado=1000 * 1024**3)

    assert pequeno._tamanho_parte == 64 * 1024 * 1024
    assert 1000 * 1024**3 / grande._tamanho_parte <= 9000


def test_lote_que_nao_rele_igual_derruba(monkeypatch):
    saida = io.StringIO()
    lote = tabular._LoteVerificado(saida, ";", csv.QUOTE_MINIMAL, "\n")
    # writer com outro delimitador: a releitura por ";" não volta igual
    lote._writer = csv.writer(lote._buf, delimiter=",", lineterminator="\n")
    lote.writerow(["a", "b"])

    with pytest.raises(ValueError, match="round-trip"):
        lote.descarregar()
    assert saida.getvalue() == ""


def test_upload_abortado_por_fora_falha_sem_retentar(minio):
    minio.s3.objetos[KEY] = CONTEUDO
    tentativas = []

    def sumir(**kw):
        tentativas.append(kw["PartNumber"])
        raise _Erro("NoSuchUpload")

    minio.s3.upload_part = sumir

    rec = _processar(minio)

    assert rec["status"] == "error"
    assert tentativas == [1]
    assert minio.s3.objetos[KEY] == CONTEUDO


def test_escritor_abortado_recusa_novas_escritas(minio):
    escritor = cliente_minio.EscritorMultipart(minio.s3, "teste", "k", tamanho_parte=4)
    escritor.abortar()

    with pytest.raises(IOError):
        escritor.write(b"0123")


# Modo paralelo. Threads no lugar dos processos: o S3 falso vive na memória do teste.
CADUNICO = b"NU_CPF_PESSOA;NO_PESSOA;DT_NASC_PESSOA;CO_SEXO\n" + b"".join(
    f"{i:011d};NOME {i};01/02/19{i % 100:02d};{i % 2}\n".encode() for i in range(3000)
)


@pytest.fixture
def paralelo(minio, monkeypatch):
    @contextmanager
    def pool_de_threads(n):
        with ThreadPoolExecutor(n) as pool:
            yield pool

    monkeypatch.setattr(mod_paralelo, "_pool", pool_de_threads)
    monkeypatch.setattr(mod_paralelo, "_cliente", lambda: minio)
    monkeypatch.setattr(mod_paralelo, "PROCESSOS", 3)
    monkeypatch.setattr(mod_paralelo, "PARALELO_MIN", 1)
    monkeypatch.setattr(mod_paralelo, "PEDACO", 10_000)
    return minio


def _via_arquivo_lf(tmp_path: Path, conteudo: bytes) -> bytes:
    src, dst = tmp_path / "src.txt", tmp_path / "dst.txt"
    src.write_bytes(conteudo)
    tabular._mascarar_tabular(str(src), str(dst), ";", "\n", False, "utf-8")
    return dst.read_bytes()


def _pedacos(minio):
    return [k for k in minio.s3.objetos if k.startswith(mod_paralelo.PEDACOS_PREFIX)]


def test_paralelo_gera_os_mesmos_bytes_que_o_sequencial(paralelo, tmp_path, caplog):
    paralelo.s3.objetos[KEY] = CADUNICO

    with caplog.at_level("INFO"):
        rec = _processar(paralelo)

    assert "pedaços em 3 processos" in caplog.text
    assert rec["status"] == "masked"
    assert paralelo.s3.objetos[KEY] == _via_arquivo_lf(tmp_path, CADUNICO)
    assert rec["registros_total"] == 3000
    assert rec["registros_alterados"] == 3000
    assert rec["hash_before"] == hashlib.md5(CADUNICO).hexdigest()
    assert rec["hash_after"] == hashlib.md5(paralelo.s3.objetos[KEY]).hexdigest()
    assert paralelo.esta_mascarado(KEY)
    assert not _pedacos(paralelo)
    assert not paralelo.s3.uploads


def test_cortes_caem_logo_apos_fim_de_linha(paralelo):
    paralelo.s3.objetos[KEY] = CADUNICO

    cortes = mod_paralelo._cortes(paralelo, KEY, len(CADUNICO), 7)

    assert cortes[0] == 0 and cortes[-1] == len(CADUNICO)
    assert cortes == sorted(set(cortes))
    assert all(CADUNICO[c - 1 : c] == b"\n" for c in cortes[1:-1])


def test_paralelo_com_aspas_cai_no_sequencial(paralelo, tmp_path):
    # aspas longe do início: a amostra não vê, um dos pedaços sim
    conteudo = CADUNICO + b'99999999999;"NOME; COM ASPAS";01/01/2000;1\n'
    paralelo.s3.objetos[KEY] = conteudo

    rec = _processar(paralelo)

    assert rec["status"] == "masked"
    assert paralelo.s3.objetos[KEY] == _via_arquivo_lf(tmp_path, conteudo)
    assert not _pedacos(paralelo)


def test_paralelo_retoma_so_os_pedacos_que_faltaram(paralelo, tmp_path, monkeypatch):
    paralelo.s3.objetos[KEY] = CADUNICO
    original = paralelo.s3.upload_part
    enviados = []

    def falhar_no_ultimo(**kw):
        if kw["Key"].endswith(f"-{len(CADUNICO)}"):
            raise _Erro("NoSuchUpload")
        enviados.append(kw["Key"])
        return original(**kw)

    monkeypatch.setattr(paralelo.s3, "upload_part", falhar_no_ultimo)
    rec = _processar(paralelo)

    assert rec["status"] == "error"
    assert paralelo.s3.objetos[KEY] == CADUNICO
    prontos = _pedacos(paralelo)
    assert prontos

    enviados.clear()
    monkeypatch.setattr(paralelo.s3, "upload_part", original)
    monkeypatch.setattr(
        paralelo.s3,
        "upload_part",
        lambda **kw: enviados.append(kw["Key"]) or original(**kw),
    )
    rec = _processar(paralelo)

    assert rec["status"] == "masked"
    assert paralelo.s3.objetos[KEY] == _via_arquivo_lf(tmp_path, CADUNICO)
    assert not set(enviados) & set(prontos)
    assert not _pedacos(paralelo)


def test_paralelo_sem_cabecalho_mascara_a_primeira_linha(paralelo, monkeypatch):
    linhas = b"".join(f"A;B;{i:011d}\n".encode() for i in range(2000))
    paralelo.s3.objetos[KEY] = linhas
    monkeypatch.setitem(regras.COLUNAS_POR_POSICAO, KEY, {2: "cpf"})

    rec = _processar(paralelo)

    saida = paralelo.s3.objetos[KEY].decode().splitlines()
    assert rec["status"] == "masked"
    assert rec["registros_total"] == 2000
    assert saida[0] == f"A;B;{regras._hmac_token('0')}"
    assert saida[1999] == f"A;B;{regras._hmac_token('1999')}"


def test_paralelo_ja_mascarado_nao_reescreve(paralelo):
    paralelo.s3.objetos[KEY] = CADUNICO

    rec = _processar(paralelo, masked_hashes={hashlib.md5(CADUNICO).hexdigest()})

    assert rec["status"] == "skipped_already"
    assert paralelo.s3.objetos[KEY] == CADUNICO
    assert not _pedacos(paralelo)
    assert paralelo.s3.multipart_abertos == 0


def test_paralelo_dry_run_nao_toca_o_raw(paralelo):
    paralelo.s3.objetos[KEY] = CADUNICO

    rec = _processar(paralelo, apply=False)

    assert rec["status"] == "dry_run"
    assert paralelo.s3.objetos[KEY] == CADUNICO
    assert "masked_dryrun/" + KEY[len("raw/") :] in paralelo.s3.objetos


def test_arquivo_pequeno_fica_no_sequencial(paralelo, monkeypatch):
    monkeypatch.setattr(mod_paralelo, "PARALELO_MIN", len(CADUNICO) + 1)
    paralelo.s3.objetos[KEY] = CADUNICO
    chamado = []
    monkeypatch.setattr(mod_paralelo, "_pool", lambda n: chamado.append(n))

    rec = _processar(paralelo)

    assert rec["status"] == "masked"
    assert not chamado


def test_leitor_para_no_fim_do_intervalo_mesmo_se_o_servidor_mandar_mais(minio):
    minio.s3.objetos["k"] = b"0123456789"
    minio.s3.get_object = lambda **kw: {"Body": _Corpo(b"3456789")}
    leitor = minio.abrir_leitura("k", inicio=3, fim=6)

    assert io.BufferedReader(leitor).read() == b"345"


# Execução pela DAG


def _rec(status, hash_before=None):
    rec = modulo._novo_registro("exec-teste", KEY)
    rec.update(status=status, hash_before=hash_before)
    return rec


@pytest.mark.parametrize("status", ["skipped_no_pii", "skipped_no_header", "skipped_unsupported"])
def test_triagem_libera_a_staging_sem_marcar_como_mascarado(minio, status):
    minio.s3.objetos[KEY] = CONTEUDO

    modulo._marcar_resultado(minio, KEY, "exec-teste", _rec(status))

    assert minio.pronto_para_staging(KEY)
    assert not minio.esta_mascarado(KEY)


def test_ja_mascarado_sem_tag_ganha_a_tag_de_volta(minio):
    # objeto copiado na reorganização do lake perde a tag, mas o conteúdo já é a máscara
    minio.s3.objetos[KEY] = CONTEUDO

    modulo._marcar_resultado(minio, KEY, "exec-teste", _rec("skipped_already", "abc"))

    assert minio.esta_mascarado(KEY)
    assert {"Key": "masked_hash", "Value": "abc"} in minio.get_tagging(KEY)


@pytest.mark.parametrize("status", ["error", "dry_run", "mdb_mascara_na_staging"])
def test_sem_mascara_nao_libera_a_staging(minio, status):
    minio.s3.objetos[KEY] = CONTEUDO

    modulo._marcar_resultado(minio, KEY, "exec-teste", _rec(status))

    assert not minio.pronto_para_staging(KEY)


def test_regravar_o_objeto_volta_a_bloquear_a_staging(minio):
    minio.s3.objetos[KEY] = CONTEUDO
    modulo._marcar_resultado(minio, KEY, "exec-teste", _rec("skipped_no_pii"))

    minio.s3.put_object(Bucket="teste", Key=KEY, Body=CONTEUDO)

    assert not minio.pronto_para_staging(KEY)


def test_dag_mascara_em_processo_proprio():
    # importado na task, o spawn do modo paralelo reexecuta o binário do Airflow
    dag = (Path(__file__).parents[1] / "dags/data_ingest/minio/minio_transform_dag.py").read_text(
        "utf-8"
    )
    assert "from mascarar_minio import" not in dag
    assert "scripts/mascarar_minio.py" in dag
    assert "check=True" in dag


def test_log_separa_info_e_aviso_sem_artefatos_locais():
    # o Airflow rotula pelo canal: stderr vira ERROR, então só aviso e erro vão para lá
    scripts = Path(__file__).parents[1] / "scripts"
    codigo = (
        f"import sys, logging; sys.path.insert(0, {str(scripts)!r}); "
        "import mascaramento.config; l = logging.getLogger('x'); "
        "l.info('linha info'); l.warning('linha aviso'); l.error('linha erro')"
    )
    env = {**os.environ, **TEST_ENV, "LAKE_LOCAL_ARTIFACTS": "0"}
    saida = subprocess.run(
        [sys.executable, "-c", codigo],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert saida.returncode == 0, saida.stderr
    assert saida.stdout.count("linha info") == 1
    assert "linha aviso" not in saida.stdout and "linha erro" not in saida.stdout
    assert saida.stderr.count("linha aviso") == 1
    assert saida.stderr.count("linha erro") == 1
    assert "linha info" not in saida.stderr


def test_mascara_so_varre_os_canais_do_legado(monkeypatch):
    """B1 (#248): a máscara do legado não lista a raw das APIs."""
    listados = []

    class _Minio:
        def listar_objetos(self, prefix):
            listados.append(prefix)
            return iter(())

    monkeypatch.setattr(modulo, "ClienteMinio", _Minio)
    for nome in ("_conn_str", "_criar_control_table", "_carregar_masked_hashes"):
        if hasattr(modulo, nome):
            monkeypatch.setattr(modulo, nome, lambda *_a: set())
    modulo.run(apply=False)
    assert listados == ["raw/sftp/", "raw/sharepoint/"]
    with pytest.raises(ValueError, match="fora do legado"):
        modulo.run(apply=False, prefix="raw/ibge/")
