"""Fase 2 da ingestão das APIs (ABECIP, FIPE, BACEN Olinda e IBGE) no modelo novo.

Os leitores rodam sobre arquivos sintéticos no formato de cada fonte, e as checagens de
layout precisam recusar coluna trocada. A ibge_ingest_dag é chamada task a task, com
lake, banco, HTTP e contexto falsos.
"""

import importlib
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from plugins.datalake.api import as_text
from plugins.datalake.readers import SourceLayoutChanged, parse
from tests.test_api_ingestion_fgv import FakePg, FakeS3


def _xlsx(path, sheet, rows):
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name=sheet, header=False, index=False)
    return str(path)


def _financing_rows(swap=False):
    rows = [["Unidades financiadas"], [None, "Construção", "Aquisição", "Total"]]
    for month, (c, a) in enumerate([(100, 50), (120, 60), (130, 70)], 1):
        units = [a, c, c + a] if swap else [c, a, c + a]
        rows.append([datetime(2025, month, 1), *units, c * 2.0, a * 2.0, (c + a) * 2.0])
    rows.append([datetime(2025, 4, 1)])  # mês futuro, vazio
    return rows


def test_abecip_financing_reads_by_position(tmp_path):
    df = as_text(
        parse(
            "abecip_financiamentos_xlsx",
            _xlsx(tmp_path / "f.xlsx", "BD_Unidades", _financing_rows()),
        )
    )
    assert df["data_referencia"].tolist() == ["2025-01-01", "2025-02-01", "2025-03-01"]
    assert df["unidades_total"].tolist() == ["150.0", "180.0", "200.0"]  # como na main


def test_abecip_financing_refuses_swapped_columns(tmp_path):
    rows = _financing_rows()
    rows[2][3] = 999  # o total deixa de ser construção + aquisição
    with pytest.raises(SourceLayoutChanged):
        parse("abecip_financiamentos_xlsx", _xlsx(tmp_path / "f.xlsx", "BD_Unidades", rows))


def _savings_rows(break_identity=False):
    rows = [[f"título {i}"] for i in range(6)]
    balance = 1000.0
    for month in range(1, 4):
        dep, ret, rend = 100.0, 60.0, 5.0
        net = dep - ret + (7 if break_identity else 0)
        balance += dep - ret + rend
        rows.append([datetime(2025, month, 1), dep, ret, net, 0.1, rend, balance])
    rows.append(["Total 2025", 300, 180, 120, None, 15, None])
    return rows


def test_abecip_savings_keeps_monthly_rows(tmp_path):
    df = as_text(
        parse("abecip_poupanca_xlsx", _xlsx(tmp_path / "p.xlsx", "SBPE_Mensal", _savings_rows()))
    )
    assert df["data_referencia"].tolist() == ["2025-01-01", "2025-02-01", "2025-03-01"]
    assert df["saldo"].tolist() == ["1045.0", "1090.0", "1135.0"]


def test_abecip_savings_refuses_broken_identity(tmp_path):
    with pytest.raises(SourceLayoutChanged):
        parse(
            "abecip_poupanca_xlsx", _xlsx(tmp_path / "p.xlsx", "SBPE_Mensal", _savings_rows(True))
        )


def test_fipezap_reads_the_four_columns(tmp_path):
    rows = [[None] * 33 for _ in range(4)]
    index = 100.0
    for month in range(1, 4):
        row = [None] * 33
        new = index * 1.01
        row[1], row[22], row[27], row[32] = datetime(2025, month, 1), new, new / index - 1, 0.12
        rows.append(row)
        index = new
    df = as_text(parse("fipezap_xlsx", _xlsx(tmp_path / "z.xlsx", "Índice FipeZAP", rows)))
    assert df.columns.tolist()[0] == "data_referencia"
    assert df["data_referencia"].tolist() == ["2025-01-01", "2025-02-01", "2025-03-01"]
    assert df["imoveis_residenciais_locacao_var_ano_total"].tolist() == ["0.12"] * 3


def test_bacen_olinda_rows(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"value": [{"Data": "2025-01-01", "Info": "x", "Valor": 9.8}]}))
    assert parse("bacen_olinda_json", str(path)).to_dict("records") == [
        {"info": "x", "data": "2025-01-01", "valor": 9.8}
    ]


IBGE_RESPONSE = [
    {
        "id": "6564",
        "variavel": "PIB",
        "unidade": "%",
        "resultados": [
            {
                "classificacoes": [{"id": "11255", "nome": "Setores", "categoria": {"90694": "Construção"}}],
                "series": [{"localidade": {"id": "1", "nome": "Brasil"},
                            "serie": {"202401": "1891.63", "202402": "..."}}],
            }
        ],
    }
]  # fmt: skip


def test_ibge_flattens_and_keeps_value_as_delivered(tmp_path):
    path = tmp_path / "pib_construcao.json"
    path.write_text(json.dumps(IBGE_RESPONSE))
    df = parse("ibge_agregados_json", str(path))
    assert df.to_dict("records")[1] == {
        "variavel_id": "6564", "variavel_nome": "PIB", "localidade_id": "1",
        "localidade_nome": "Brasil", "classificacao_id": "11255", "classificacao_nome": "Setores",
        "categoria_id": "90694", "categoria_nome": "Construção", "unidade": "%",
        "periodo": "202402", "valor": "...",
    }  # fmt: skip
    assert df["valor"].tolist()[0] == "1891.63"  # o ponto é decimal: não sai


@pytest.fixture
def ibge(monkeypatch, tmp_path):
    mod = importlib.import_module("dags.snh.data_ingest.ibge.ibge_ingest_dag")
    s3, pg = FakeS3(), FakePg()
    configs = [{"tabela": "pib_construcao", "agregado": "5932", "variaveis": "6564"},
               {"tabela": "sinapi", "agregado": "2296", "variaveis": "48"}]  # fmt: skip
    calls = []

    class FakeHttp:
        def __init__(self, method, http_conn_id):
            pass

        def run(self, endpoint, data=None, **kw):
            calls.append((endpoint, data))
            if "2296" in endpoint:
                raise RuntimeError("HTTP 500")
            return SimpleNamespace(
                content=json.dumps(IBGE_RESPONSE).encode(), json=lambda: IBGE_RESPONSE
            )

    ctx = {
        "run_id": "scheduled__2026-12-01T06:00:00+00:00",
        "dag": SimpleNamespace(dag_id="ibge_ingest_dag"),
        "dag_run": SimpleNamespace(run_after=datetime(2026, 12, 1, 6, tzinfo=timezone.utc)),
        "params": {"dry_run": False},
    }
    xcom = {}
    ctx["ti"] = SimpleNamespace(xcom_pull=lambda task_ids: xcom.get(task_ids))
    monkeypatch.setenv("LAKE_TMPDIR", str(tmp_path))
    monkeypatch.setattr(mod, "get_current_context", lambda: ctx)
    monkeypatch.setattr(mod, "S3Hook", lambda aws_conn_id: s3)
    monkeypatch.setattr(mod, "PostgresHook", lambda postgres_conn_id: pg)
    monkeypatch.setattr(mod, "HttpHook", FakeHttp)
    monkeypatch.setattr(
        mod, "Variable",
        SimpleNamespace(get=lambda key, deserialize_json=False: configs if key == "IBGE_CONFIGURACOES" else "bucket"),
    )  # fmt: skip

    def run(task_id, *args):
        out = mod.dag_instance.get_task(task_id).python_callable(*args)
        xcom[task_id] = out
        return out

    return SimpleNamespace(run=run, s3=s3, calls=calls)


def test_ibge_one_failing_table_does_not_stop_the_others(ibge):
    extracted = ibge.run("extract")
    assert list(extracted["files"]) == ["pib_construcao"]
    assert extracted["errors"] == ["sinapi: HTTP 500"]
    keys = ibge.run("upload_to_s3", extracted)
    assert keys == ["raw/ibge/pib_construcao/load_id=20261201T060000Z/pib_construcao.json"]
    recs = ibge.run(
        "upload_staging", ibge.run("verify_structural_data_drift", ibge.run("transform", keys))
    )
    assert recs[0]["status"] == "converted" and recs[0]["structural"] == "approved"
    assert (
        "staging/ibge/pib_construcao/load_id=20261201T060000Z/pib_construcao.parquet"
        in ibge.s3.objects
    )
    with pytest.raises(RuntimeError):  # a tabela que falhou na API falha a execução no fim
        ibge.run("report")


def test_ibge_endpoint_uses_comma_between_categories(ibge):
    from dags.snh.data_ingest.ibge.ibge_ingest_dag import _endpoint_and_params

    endpoint, params = _endpoint_and_params(
        {
            "agregado": 6323,
            "variaveis": "4090",
            "periodos": "-8",
            "classificacao_id": 888,
            "categoria": "47946|47949",
        }
    )
    assert endpoint == "api/v3/agregados/6323/periodos/-8/variaveis/4090"
    assert params == {"localidades": "N1[1]", "classificacao": "888[47946,47949]"}
