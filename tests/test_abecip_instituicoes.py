"""abecip_instituicoes_ingest_dag: as competências do raw numa staging só, pelo mesmo
escritor das outras ingestões (texto + linhagem, B12)."""

import importlib
import io
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def list_keys(self, bucket, prefix=""):
        return [k for k in self.objects if k.startswith(prefix)]

    def read_key(self, key, bucket):
        return self.objects[key].decode("utf-8")

    def load_file(self, filename, key, bucket, replace=False):
        with open(filename, "rb") as f:
            self.objects[key] = f.read()


@pytest.fixture
def consolidar(monkeypatch, tmp_path):
    mod = importlib.import_module("dags.data_ingest.abecip.abecip_instituicoes_ingest_dag")
    row = {
        "periodo_rotulo": "2026-09",
        "instituicao_financeira": "CAIXA",
        "volume_mensal_milhoes": 10.5,
    }
    s3 = FakeS3({
        "raw/abecip/2026-09/financiamentos_por_instituicao.json": json.dumps([row]).encode(),
        "raw/abecip/2026-10/financiamentos_por_instituicao.json": json.dumps({"dados": [row]}).encode(),
        "raw/abecip/financiamentos_modalidade/load_id=X/financiamentos_modalidade.xlsx": b"x",
    })  # fmt: skip
    ctx = {"dag_run": SimpleNamespace(run_after=datetime(2026, 11, 24, 7, tzinfo=timezone.utc))}
    monkeypatch.setenv("LAKE_TMPDIR", str(tmp_path))
    monkeypatch.setattr(mod, "S3Hook", lambda aws_conn_id: s3)
    monkeypatch.setattr(mod, "Variable", SimpleNamespace(get=lambda key: "bucket"))
    monkeypatch.setattr(mod, "get_current_context", lambda: ctx)
    task = mod.dag_instance.get_task("consolidar").python_callable
    return SimpleNamespace(run=task, s3=s3)


def test_every_competence_in_one_text_staging(consolidar):
    resumo = consolidar.run()
    assert resumo == {"competencias": 2, "de": "2026-09", "ate": "2026-10", "linhas": 2}
    table = pq.read_table(
        io.BytesIO(consolidar.s3.objects["staging/abecip/financiamentos_por_instituicao.parquet"])
    )
    assert all(str(f.type) == "string" for f in table.schema)
    df = table.to_pandas()
    assert df["competencia_referencia"].tolist() == ["2026-09", "2026-10"]  # a pasta manda
    assert df["volume_mensal_milhoes"].tolist() == ["10.5", "10.5"]
    assert set(df["_load_id"]) == {"20261124T070000Z"}
