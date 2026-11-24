"""Fase 4 da ingestão (Infomoney e Tesouro Gerencial) no modelo novo.

Infomoney: cada carga traz só os últimos pregões, então o histórico vem das cargas
anteriores (e de uma exportação única do Postgres). Tesouro: o relatório chega por
e-mail, em ZIP. Os leitores rodam sobre arquivos sintéticos e o extract do Tesouro com
uma caixa de e-mail falsa.
"""

import importlib
import io
import json
import zipfile
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from plugins.datalake.readers import TESOURO_COLUMNS, SourceLayoutChanged, parse


def test_alphavantage_rows_keep_values_as_delivered(tmp_path):
    path = tmp_path / "acoes_imob.json"
    day = {"1. open": "10.5000", "2. high": "11.0000", "3. low": "10.0000",
           "4. close": "10.8000", "5. volume": "1200"}  # fmt: skip
    path.write_text(json.dumps({"Meta Data": {"2. Symbol": "IMOB.SA"},
                                "Time Series (Daily)": {"2026-11-24": day}}))  # fmt: skip
    assert parse("alphavantage_daily_json", str(path)).to_dict("records") == [
        {"symbol": "IMOB.SA", "data_pregao": "2026-11-24", "open": "10.5000", "high": "11.0000",
         "low": "10.0000", "close": "10.8000", "volume": "1200"}
    ]  # fmt: skip


def test_alphavantage_rate_limit_note_is_refused(tmp_path):
    path = tmp_path / "acoes_imob.json"
    path.write_text(json.dumps({"Note": "Thank you for using Alpha Vantage! ..."}))
    with pytest.raises(SourceLayoutChanged):
        parse("alphavantage_daily_json", str(path))


def test_infomoney_prata_keeps_the_newest_load_per_day():
    """O SQL da prata (sem o Jinja) sobre duas cargas que repetem um pregão."""
    import duckdb

    con = duckdb.connect()
    con.execute(
        "create table bronze as select * from (values "
        "('IMOB.SA', '2026-11-20', '10.0', 'A'), ('IMOB.SA', '2026-11-21', '11.0', 'A'), "
        "('IMOB.SA', '2026-11-21', '11.5', 'B'), ('IMOB.SA', '2026-11-22', '12.0', 'B')"
        ") t(symbol, data_pregao, close, _load_id)"
    )
    rows = con.execute(
        "with cargas as (select *, row_number() over (partition by symbol, data_pregao "
        "order by _load_id desc) as ordem from bronze) "
        "select data_pregao, close from cargas where ordem = 1 order by 1"
    ).fetchall()
    assert rows == [("2026-11-20", "10.0"), ("2026-11-21", "11.5"), ("2026-11-22", "12.0")]


def _tesouro_zip(rows, extra_tab=False):
    lines = [f"título {i}" for i in range(12)]
    lines += ["\t".join(r) + ("\t" if extra_tab else "") for r in rows]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("relatorio.csv", "\n".join(lines).encode("utf-16"))
    return buffer.getvalue()


def test_tesouro_zip_keeps_codes_as_text(tmp_path):
    row = ["56101", "MCID", "0E64", "Ação", *["x"] * (len(TESOURO_COLUMNS) - 5), "(1.234,56)"]
    path = tmp_path / "01_relatorio.zip"
    path.write_bytes(_tesouro_zip([row], extra_tab=True))
    df = parse("tesouro_dotacao_zip", str(path))
    assert df.columns.tolist() == TESOURO_COLUMNS
    assert df.loc[0, "acao_governo_codigo"] == "0E64"  # não vira 0.0 em notação científica
    assert df.loc[0, "restos_a_pagar_pagos"] == "(1.234,56)"


def test_tesouro_zip_with_missing_columns_is_refused(tmp_path):
    path = tmp_path / "01_relatorio.zip"
    path.write_bytes(_tesouro_zip([["a", "b", "c"]]))
    with pytest.raises(SourceLayoutChanged):
        parse("tesouro_dotacao_zip", str(path))


@pytest.fixture
def tesouro(monkeypatch, tmp_path):
    mod = importlib.import_module(
        "dags.data_ingest.tesouro_gerencial.dotacao_execucao_outras_fontes_mcid_ingest_dag"
    )
    inbox: list = []
    asked: list = []

    class FakeMailBox:
        def __init__(self, host):
            pass

        def login(self, login, password):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def fetch(self, criteria, bulk=False):
            asked.append(str(criteria))
            return inbox

    ctx = {
        "run_id": "scheduled__2026-11-24T12:00:00+00:00",
        "dag": SimpleNamespace(dag_id="dotacao_execucao_outras_fontes_mcid_ingest_dag"),
        # 01h UTC de 25/11 ainda é 24/11 em Brasília
        "dag_run": SimpleNamespace(run_after=datetime(2026, 11, 25, 1, tzinfo=timezone.utc)),
        "params": {"dry_run": False},
    }
    conn = SimpleNamespace(
        host="imap.x", login="u", password="p", extra_dejson={"sender_email": "s@x"}
    )
    monkeypatch.setenv("LAKE_TMPDIR", str(tmp_path))
    monkeypatch.setattr(mod, "get_current_context", lambda: ctx)
    monkeypatch.setattr(mod, "MailBox", FakeMailBox)
    monkeypatch.setattr(mod, "BaseHook", SimpleNamespace(get_connection=lambda conn_id: conn))
    extract = mod.dag_instance.get_task("extract").python_callable
    return SimpleNamespace(extract=extract, inbox=inbox, asked=asked)


def attachment(name):
    return SimpleNamespace(filename=name, payload=b"zip")


def test_tesouro_lands_each_zip_of_the_day(tesouro):
    tesouro.inbox.append(
        SimpleNamespace(attachments=[attachment("rel 1.zip"), attachment("x.pdf")])
    )
    tesouro.inbox.append(SimpleNamespace(attachments=[attachment("rel 1.zip")]))
    names = [p.rsplit("/", 1)[-1] for p in tesouro.extract()]
    assert names == ["01_rel_1.zip", "02_rel_1.zip"]
    assert "24-Nov-2026" in tesouro.asked[0]  # o dia em Brasília, não em UTC


def test_tesouro_without_email_skips(tesouro):
    with pytest.raises(AirflowSkipException):
        tesouro.extract()
