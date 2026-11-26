"""Fase 3 da ingestão das APIs (BACEN SGS, Novo CAGED e MRV) no modelo novo.

O Novo CAGED vem dos microdados do PDET (FTP), no lugar do painel Power BI.

Aqui uma carga tem vários arquivos (uma série, um mês), e o contexto que a resposta não
traz vai no nome do arquivo. Os leitores rodam sobre arquivos sintéticos; o extract do
Novo CAGED e do SGS, com HTTP falso.
"""

import importlib
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest
from airflow.sdk.exceptions import AirflowFailException

from plugins.datalake.readers import SourceLayoutChanged, parse


def test_sgs_series_name_comes_from_the_file(tmp_path):
    path = tmp_path / "pf_taxa_juros_aa.json"
    path.write_text(json.dumps([{"data": "01/01/2025", "valor": "11.2"}]))
    assert parse("bacen_sgs_json", str(path)).to_dict("records") == [
        {"tipo": "pf_taxa_juros_aa", "data": "01/01/2025", "valor": "11.2"}
    ]


def test_sgs_html_answer_is_refused(tmp_path):
    path = tmp_path / "x.json"
    path.write_text(json.dumps({"erro": "página"}))
    with pytest.raises(SourceLayoutChanged):
        parse("bacen_sgs_json", str(path))


def _module(monkeypatch, tmp_path, name, http, variable=None, today=datetime(2024, 4, 10)):
    mod = importlib.import_module(name)
    ctx = {
        "run_id": "manual__1",
        "dag": SimpleNamespace(dag_id=name.rsplit(".", 1)[-1]),
        "dag_run": SimpleNamespace(run_after=today.replace(tzinfo=timezone.utc)),
        "params": {"dry_run": False},
    }
    monkeypatch.setenv("LAKE_TMPDIR", str(tmp_path))
    monkeypatch.setattr(mod, "get_current_context", lambda: ctx)
    monkeypatch.setattr(mod, "HttpHook", http)
    if variable is not None:
        monkeypatch.setattr(
            mod, "Variable", SimpleNamespace(get=lambda key, deserialize_json=False: variable)
        )
    return mod.dag_instance.get_task("extract").python_callable


def test_sgs_one_bad_series_lands_nothing(monkeypatch, tmp_path):
    class Http:
        def __init__(self, method, http_conn_id):
            pass

        def run(self, endpoint, data=None, **kw):
            html = "bcdata.sgs.2" in endpoint
            return SimpleNamespace(
                json=(lambda: (_ for _ in ()).throw(ValueError("html")))
                if html
                else (lambda: [{"data": "x", "valor": "1"}]),
                content=b"[]",
            )

    extract = _module(monkeypatch, tmp_path, "dags.snh.data_ingest.bacen.bacen_sgs_ingest_dag", Http,
                      variable=[{"pf": "1", "pj": "2"}])  # fmt: skip
    with pytest.raises(AirflowFailException, match="pj"):
        extract()


def _mrv_sheet(path):
    rows = [["Dados operacionais"], [None, None, "1T25 / 1Q25", "2T25 / 2Q25"],
            ["Lançamentos %MRV"], ["MRV Incorporação"],
            ["VGV", None, "1000", "1100"], ["Unidades", None, "4000", "4200"],
            ["Preço médio", None, "250", "262"]]  # fmt: skip
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(rows).to_excel(
            writer, sheet_name="Dados Oper. MRV&Co | Oper.D", header=False, index=False
        )
    return str(path)


def test_mrv_block_is_one_row_per_quarter(tmp_path):
    df = parse("mrv_lancamentos_xlsx", _mrv_sheet(tmp_path / "planilha_interativa.xlsx"))
    assert df.to_dict("records") == [
        {
            "periodo": "1T25",
            "vgv_lancamentos_milhoes": "1000",
            "unidades": "4000",
            "preco_medio_unidade_mil": "250",
        },
        {
            "periodo": "2T25",
            "vgv_lancamentos_milhoes": "1100",
            "unidades": "4200",
            "preco_medio_unidade_mil": "262",
        },
    ]


def test_mrv_missing_section_is_refused(tmp_path):
    with pytest.raises(SourceLayoutChanged, match="Vendas"):
        parse("mrv_vendas_xlsx", _mrv_sheet(tmp_path / "planilha_interativa.xlsx"))


def _caged_7z(tmp_path, name, rows):
    """A PDET-like 7z: one `;` TXT with the header of the microdata."""
    import subprocess

    txt = tmp_path / f"{name}.txt"
    header = "competênciamov;região;uf;município;seção;subclasse;saldomovimentação"
    txt.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    out = tmp_path / f"{name}.7z"
    subprocess.run(["7z", "a", str(out), str(txt)], check=True, capture_output=True)
    txt.unlink()
    return str(out)


def test_caged_pdet_keeps_only_the_construction_section(tmp_path):
    path = _caged_7z(tmp_path, "CAGEDMOV202606", [
        "202606;3;35;354890;F;4120400;1",
        "202606;3;35;354890;G;4687702;1",
        "202606;3;35;354890;F;4399103;-1",
    ])  # fmt: skip
    df = parse("novo_caged_pdet_7z", path)
    assert df[["seção", "subclasse", "saldomovimentação"]].values.tolist() == [
        ["F", "4120400", "1"],
        ["F", "4399103", "-1"],
    ]


class _FakeFTP:
    TREE = {
        "/pdet/microdados/NOVO CAGED": ["2023", "2026"],
        "/pdet/microdados/NOVO CAGED/2026": ["202609"],
        "/pdet/microdados/NOVO CAGED/2026/202609": [
            "/pdet/microdados/NOVO CAGED/2026/202609/CAGEDMOV202609.7z",
            "/pdet/microdados/NOVO CAGED/2026/202609/CAGEDFOR202609.7z",
            "LEIAME.txt",
        ],
    }  # fmt: skip

    def __init__(self, *a, **kw):
        self.encoding = "utf-8"

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def login(self):
        pass

    def nlst(self, path):
        return self.TREE.get(path, [])

    def retrbinary(self, cmd, write):
        write(b"7z")


def test_caged_pdet_extract_downloads_only_what_is_not_in_raw(monkeypatch, tmp_path):
    mod = importlib.import_module("dags.snh.data_ingest.novo_caged.pdet_ingest_dag")
    ctx = {"run_id": "manual__1", "dag": SimpleNamespace(dag_id="pdet"),
           "params": {"dry_run": False, "limit": 0}}  # fmt: skip
    landed = ["snh/raw/novo_caged/pdet/2026/202609/load_id=20261101T000000Z/CAGEDMOV202609.7z"]
    hook = SimpleNamespace(list_keys=lambda bucket, prefix: landed)
    monkeypatch.setenv("LAKE_TMPDIR", str(tmp_path))
    monkeypatch.setattr(mod, "get_current_context", lambda: ctx)
    monkeypatch.setattr(mod, "S3Hook", lambda aws_conn_id: hook)
    monkeypatch.setattr(mod, "Variable", SimpleNamespace(get=lambda key: "bucket"))
    monkeypatch.setattr(mod.ftplib, "FTP", _FakeFTP)
    paths = mod.dag_instance.get_task("extract").python_callable()
    assert [p.rsplit("/", 1)[-1] for p in paths] == ["CAGEDFOR202609.7z"]  # 2023 fica fora
    assert mod._folder("CAGEDFOR202609.7z") == "novo_caged/pdet/2026/202609"
