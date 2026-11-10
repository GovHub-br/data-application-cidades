"""Novo CAGED: period derived from the run date and the missing-month rule (#250)."""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "plugins"))

from cliente_novo_caged import (  # noqa: E402
    ClienteNovoCaged,
    MissingMonthsError,
    expected_periods,
)


def test_year_turn_ends_in_december_of_previous_year():
    periods = expected_periods(date(2027, 1, 10))
    assert periods[0] == (2024, "janeiro")
    assert periods[-1] == (2026, "dezembro")
    assert len(periods) == 36


def test_current_month_is_never_requested():
    assert expected_periods(date(2026, 11, 10))[-1] == (2026, "outubro")


def _client(monkeypatch, empty):
    client = ClienteNovoCaged()

    def fake(ano, mes, cnae_divisao=None):
        return None if (ano, mes) in empty else {"ano": ano, "mes": mes}

    monkeypatch.setattr(client, "obter_dados_mensais", fake)
    return client


def test_missing_old_month_fails(monkeypatch):
    client = _client(monkeypatch, {(2025, "março")})
    with pytest.raises(MissingMonthsError, match="março/2025"):
        client.obter_historico(today=date(2026, 11, 10))


def test_latest_month_may_be_missing(monkeypatch):
    client = _client(monkeypatch, {(2026, "outubro")})
    records = client.obter_historico(today=date(2026, 11, 10))
    assert records[-1] == {"ano": 2026, "mes": "setembro"}
    assert len(records) == len(expected_periods(date(2026, 11, 10))) - 1
