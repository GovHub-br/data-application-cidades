"""Fase 3 da ingestão das APIs (BACEN SGS, Novo CAGED e MRV) no modelo novo.

Aqui uma carga tem vários arquivos (uma série, um mês), e o contexto que a resposta não
traz vai no nome do arquivo. Os leitores rodam sobre arquivos sintéticos; o extract do
Novo CAGED e do SGS, com HTTP falso.
"""

import importlib
import json
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest
from airflow.sdk.exceptions import AirflowFailException

from plugins.datalake.api import as_text
from plugins.datalake.readers import SourceLayoutChanged, parse
from plugins.novo_caged_query import expected_periods, query_payload


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


def test_caged_periods_end_before_the_current_month():
    periods = expected_periods(date(2027, 1, 10))
    assert (periods[0], periods[-1], len(periods)) == ((2024, 1), (2026, 12), 36)


def _where(payload: dict) -> list:
    return payload["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"][
        "Where"
    ]


def test_caged_query_filters_division_only_when_given():
    where = _where
    assert len(where(query_payload(2025, 3, None))) == 2
    edificios = where(query_payload(2025, 3, "Construção de Edifícios"))
    assert len(edificios) == 3
    assert edificios[-1]["Condition"]["In"]["Values"] == [
        [{"Literal": {"Value": "2025L"}}, {"Literal": {"Value": "'março'"}}]
    ]


def _caged_answer(values):
    return {
        "results": [{"result": {"data": {"dsr": {"DS": [{"PH": [{"DM0": [{"C": values}]}]}]}}}}]
    }


def test_caged_month_comes_from_the_file(tmp_path):
    path = tmp_path / "2025-03.json"
    path.write_text(json.dumps(_caged_answer([10, 4, 6, 1000, 0.6])))
    assert as_text(parse("novo_caged_powerbi_json", str(path))).to_dict("records") == [
        {"ano": "2025", "mes": "03", "admitidos": "10", "desligados": "4", "saldo": "6",
         "estoque": "1000", "variacao": "0.6"}
    ]  # fmt: skip


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


def _caged_http(empty):
    class Http:
        def __init__(self, method, http_conn_id):
            pass

        def run(self, endpoint, json=None):
            where = json["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"][
                "Query"
            ]["Where"]
            year, month = (v["Literal"]["Value"] for v in where[-1]["Condition"]["In"]["Values"][0])
            answer = {} if (year, month) in empty else _caged_answer([1, 1, 0, 10, 0.0])
            return SimpleNamespace(json=lambda: answer, content=b"{}")

    return Http


CAGED = "dags.data_ingest.novo_caged.saldo_estoque_total_construcao_ingest_dag"


def test_caged_latest_month_may_be_missing(monkeypatch, tmp_path):
    extract = _module(monkeypatch, tmp_path, CAGED, _caged_http({("2024L", "'março'")}))
    names = [p.rsplit("/", 1)[-1] for p in extract()]
    assert names == ["2024-01.json", "2024-02.json"]  # março ainda não publicado


def test_caged_older_missing_month_fails(monkeypatch, tmp_path):
    extract = _module(monkeypatch, tmp_path, CAGED, _caged_http({("2024L", "'fevereiro'")}))
    with pytest.raises(AirflowFailException, match="2024, 2"):
        extract()


def test_sgs_one_bad_series_lands_nothing(monkeypatch, tmp_path):
    class Http:
        def __init__(self, method, http_conn_id):
            pass

        def run(self, endpoint, data=None):
            html = "bcdata.sgs.2" in endpoint
            return SimpleNamespace(
                json=(lambda: (_ for _ in ()).throw(ValueError("html")))
                if html
                else (lambda: [{"data": "x", "valor": "1"}]),
                content=b"[]",
            )

    extract = _module(monkeypatch, tmp_path, "dags.data_ingest.bacen.bacen_sgs_ingest_dag", Http,
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
