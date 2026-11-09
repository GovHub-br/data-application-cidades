"""raw_para_staging dividido em scripts/conversao/: a ligação entre os módulos.

Cobre o que a divisão pode quebrar sem nenhum teste de formato perceber: configuração
copiada no import de cada módulo, caminhos relativos ao pacote, log, a CLI e o `run()`
que a DAG chama, de ponta a ponta.
"""

import hashlib
import importlib
import inspect
import io
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import openpyxl
import pandas as pd
import pyarrow.parquet as pq
import pytest

RAIZ = Path(__file__).parents[1]
SCRIPTS = RAIZ / "scripts"
AMBIENTE = {
    "MINIO_ENDPOINT": "minio:9000",
    "MINIO_ACCESS_KEY": "teste",
    "MINIO_SECRET_KEY": "teste",
    "MINIO_BUCKET": "teste",
    "DB_DW_HOST_MCID": "postgres",
    "DB_DW_USER_MCID": "teste",
    "DB_DW_PASSWORD_MCID": "teste",
    "DB_DW_DBNAME_MCID": "teste",
    "LAKE_LOCAL_ARTIFACTS": "0",
}


@pytest.fixture
def carregar(monkeypatch, tmp_path):
    """Importa o entrypoint do zero, com o ambiente do teste."""

    def _carregar(**extra):
        for key, value in {**AMBIENTE, "LAKE_TMPDIR": str(tmp_path), **extra}.items():
            monkeypatch.setenv(key, value)
        monkeypatch.syspath_prepend(str(SCRIPTS))
        for nome in [
            m for m in sys.modules if m.split(".")[0] in ("conversao", "raw_para_staging")
        ]:
            del sys.modules[nome]
        return importlib.import_module("raw_para_staging")

    return _carregar


def _modulo(nome):
    return sys.modules[f"conversao.{nome}"]


# Configuração


def test_ambiente_chega_a_todos_os_modulos(carregar, tmp_path):
    carregar(MASKING_PREFIX="bruto/", STAGING_PREFIX="stg/")
    # cada módulo copia a constante no import: um que lesse antes do ambiente divergiria
    assert _modulo("processamento").TMPDIR == str(tmp_path)
    assert _modulo("pendente").TMPDIR == str(tmp_path)
    assert _modulo("processamento")._staging_key("bruto/a/b.csv") == "stg/a/b.parquet"
    assert _modulo("processamento")._dryrun_key("stg/a/b.parquet") == (
        "staging_dryrun/a/b.parquet"
    )
    # o prefixo do raw também decide a identidade dos gêmeos
    assert _modulo("gemeos")._stem_sem_extensao("bruto/abecip/2025-10/x.csv") == (
        "abecip/2025-10/x"
    )


def test_artefatos_locais_continuam_ao_lado_do_script(carregar):
    carregar()
    config = _modulo("config")
    assert config.SCRIPTS_DIR == SCRIPTS.resolve()
    assert config._LOG_FILE.parent == SCRIPTS.resolve()
    assert config._LOG_FILE.name.startswith("raw_para_staging_")


def test_sem_artefatos_nao_instala_handler(carregar):
    antes = list(logging.root.handlers)
    carregar()
    assert logging.root.handlers == antes


def test_log_dos_modulos_chega_ao_root(carregar, caplog):
    carregar()
    with caplog.at_level(logging.INFO):
        logging.getLogger("conversao.processamento").info("sinal")
    assert "sinal" in caplog.text


def test_auditoria_vai_ao_minio_e_a_copia_local_ao_lado_do_script(
    carregar, tmp_path, monkeypatch
):
    carregar()
    controle = _modulo("controle")
    monkeypatch.setattr(controle, "_LOCAL_ARTIFACTS", True)
    monkeypatch.setattr(controle, "SCRIPTS_DIR", tmp_path)
    enviados = {}

    class Minio:
        def put_object(self, key, dados):
            enviados[key] = dados

    controle._gravar_auditoria(
        Minio(), "abc", [{"raw_key": "raw/a.csv", "column_map": {"A": "a"}}]
    )

    chave = "audit/staging/execution_id=abc/part-0.parquet"
    assert list(enviados) == [chave]
    df = pd.read_parquet(io.BytesIO(enviados[chave]))
    assert json.loads(df.loc[0, "column_map"]) == {"A": "a"}
    assert (tmp_path / "auditoria_staging_abc.parquet").exists()


# Entrypoint


def test_run_mantem_a_assinatura_que_a_dag_usa(carregar):
    modulo = carregar()
    dag = (RAIZ / "dags/data_ingest/minio/minio_transform_dag.py").read_text("utf-8")
    assert "from raw_para_staging import run" in dag
    assert list(inspect.signature(modulo.run).parameters) == [
        "apply",
        "force",
        "limit",
        "pattern",
        "only_ext",
        "max_size_mb",
        "chunksize",
        "bad_lines",
        "prefix",
    ]


def test_cli_roda_como_script(tmp_path):
    # PYTHONPATH vazio: o script tem que achar o pacote e o plugins/ sozinho
    env = {**os.environ, **AMBIENTE, "PYTHONPATH": "", "LAKE_TMPDIR": str(tmp_path)}
    saida = subprocess.run(
        [sys.executable, str(SCRIPTS / "raw_para_staging.py"), "--help"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert saida.returncode == 0, saida.stderr
    assert "--apply" in saida.stdout and "--bad-lines" in saida.stdout


# run() de ponta a ponta, sem Postgres e sem MinIO


def _xlsx(abas):
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for nome, linhas in abas.items():
        ws = wb.create_sheet(nome)
        for linha in linhas:
            ws.append(linha)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


DADOS_CSV = "ID;NOME\n1;Ana\n2;Bia\n".encode("utf-8")
OBJETOS = {
    "raw/sftp/a/DADOS.csv": DADOS_CSV,
    # gêmeo do DADOS.csv em outra pasta: o texto vence a planilha
    "raw/sharepoint/b/DADOS.xlsx": _xlsx({"Planilha": [["ID"], ["9"]]}),
    "raw/sftp/a/PLAN.xlsx": _xlsx(
        {"Aba 1": [["COD", "VALOR"], ["1", "10"]], "Resumo": [["TOTAL"], ["10"]]}
    ),
    "raw/api/c.json": json.dumps([{"a": 1, "b": {"c": 2}}]).encode("utf-8"),
    "raw/sftp/a/x.zip": b"PK",
    "raw/sftp/a/~$DADOS.csv": b"lixo do Excel",
    "raw/dados_historicos/HIST.csv": DADOS_CSV,
}


class MinioFalso:
    def __init__(self, tmpdir):
        self.tmpdir, self.enviados, self.auditoria = tmpdir, {}, {}

    def listar_objetos(self, prefix):
        for key, dados in OBJETOS.items():
            if key.startswith(prefix):
                yield key, len(dados)

    def sample_bytes(self, key):
        return OBJETOS[key][:65536]

    def baixar_para_tempfile(self, key, ext, tmpdir):
        # o download também tem que ir para o LAKE_TMPDIR configurado
        assert tmpdir == self.tmpdir
        path = Path(tmpdir) / f"download{ext}"
        path.write_bytes(OBJETOS[key])
        return str(path)

    def upload_arquivo(self, path, key):
        self.enviados[key] = pq.read_table(path)

    def put_object(self, key, dados):
        self.auditoria[key] = dados


@pytest.fixture
def executar(carregar, tmp_path, monkeypatch):
    modulo = carregar()
    minio = MinioFalso(str(tmp_path))
    controle = []
    monkeypatch.setattr(modulo, "ClienteMinio", lambda: minio)
    monkeypatch.setattr(modulo, "_conn_str", lambda: "teste")
    monkeypatch.setattr(modulo, "_criar_control_table", lambda _c: None)
    monkeypatch.setattr(modulo, "_carregar_convertidos", lambda _c: set())
    monkeypatch.setattr(modulo, "_registrar_control", lambda _c, r: controle.append(r))

    def _executar(**kw):
        return modulo.run(**kw), minio, controle

    return _executar


def test_run_converte_cada_formato_e_registra(executar, tmp_path):
    contagem, minio, controle = executar(apply=True)

    assert contagem == {
        "converted": 4,
        "skipped_duplicado": 1,
        "skipped_unsupported": 1,
    }
    assert sorted(minio.enviados) == [
        "staging/api/c.parquet",
        "staging/sftp/a/DADOS.parquet",
        "staging/sftp/a/PLAN__aba_1.parquet",
        "staging/sftp/a/PLAN__resumo.parquet",
    ]
    dados = minio.enviados["staging/sftp/a/DADOS.parquet"].to_pylist()
    assert [(r["id"], r["nome"]) for r in dados] == [("1", "Ana"), ("2", "Bia")]
    assert {r["_source_file"] for r in dados} == {"DADOS.csv"}
    assert {r["_source_hash"] for r in dados} == {hashlib.md5(DADOS_CSV).hexdigest()}
    assert minio.enviados["staging/api/c.parquet"].column_names[:2] == ["a", "b_c"]

    # o controle só recebe o que tem hash; a auditoria leva tudo, gêmeo incluso
    assert sorted(r["staging_key"] for r in controle) == sorted(minio.enviados)
    (chave,) = minio.auditoria
    auditoria = pd.read_parquet(io.BytesIO(minio.auditoria[chave]))
    assert sorted(auditoria["status"]) == sorted(
        ["converted"] * 4 + ["skipped_duplicado", "skipped_unsupported"]
    )
    assert "raw/dados_historicos/HIST.csv" not in set(auditoria["raw_key"])
    # nenhum download ou parquet temporário fica para trás
    assert list(tmp_path.iterdir()) == []


def test_run_em_dry_run_grava_so_em_staging_dryrun(executar):
    contagem, minio, _ = executar(apply=False, only_ext="csv,json")
    assert contagem == {"dry_run": 2}
    assert sorted(minio.enviados) == [
        "staging_dryrun/api/c.parquet",
        "staging_dryrun/sftp/a/DADOS.parquet",
    ]


def test_filtro_nao_muda_quem_vence_entre_gemeos(executar):
    # só o xlsx no recorte: ele continua perdendo para o csv que ficou de fora
    contagem, minio, _ = executar(apply=True, only_ext="xlsx")
    assert contagem == {"converted": 2, "skipped_duplicado": 1}
    assert "staging/sharepoint/b/DADOS.parquet" not in minio.enviados
