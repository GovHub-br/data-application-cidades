"""Testes do tratamento de casos especiais: vazias e dados sem utilidade.

Cobre tratar_vazia e tratar_dados_sem_utilidade — ambas retornam dict
de descarte sem gerar DataFrame.
"""

from __future__ import annotations

from classificacao.tratamento_especiais import (
    tratar_dados_sem_utilidade,
    tratar_vazia,
)


# ─── tratar_vazia ─────────────────────────────────────────────────────────


def test_tratar_vazia_status_discarded() -> None:
    """tratar_vazia retorna dict com status='discarded'."""
    result = tratar_vazia("bb_2015_vazia")
    assert result["status"] == "discarded"


def test_tratar_vazia_reason_vazia() -> None:
    result = tratar_vazia("bb_2015_vazia")
    assert result["reason"] == "vazia"


def test_tratar_vazia_profile() -> None:
    result = tratar_vazia("bb_2015_vazia")
    assert result["profile"] == "vazia"


def test_tratar_vazia_table_name() -> None:
    result = tratar_vazia("bb_2015_08_agosto_vazia")
    assert result["table_name"] == "bb_2015_08_agosto_vazia"


def test_tratar_vazia_contagem_zero() -> None:
    result = tratar_vazia("qualquer")
    assert result["n_rows"] == 0
    assert result["n_cols"] == 0


def test_tratar_vazia_instituicao_inferida() -> None:
    result = tratar_vazia("bb_2015_vazia")
    assert result["institution"] == "bb"


def test_tratar_vazia_instituicao_unknown() -> None:
    result = tratar_vazia("001_sem_prefixo")
    assert result["institution"] == "unknown"


def test_tratar_vazia_metadados_adicionais() -> None:
    """O dict contém todos os campos esperados de metadados."""
    result = tratar_vazia("teste")
    expected_keys = {
        "table_name",
        "status",
        "reason",
        "n_rows",
        "n_cols",
        "profile",
        "institution",
        "report_date",
        "missing_pct",
        "encoding_issues",
        "date_parse_errors",
        "type_coercion_warnings",
    }
    assert expected_keys.issubset(result.keys())


# ─── tratar_dados_sem_utilidade ────────────────────────────────────────────


def test_tratar_dados_sem_utilidade_status_discarded() -> None:
    result = tratar_dados_sem_utilidade("bb_2015_08_loginfesta")
    assert result["status"] == "discarded"


def test_tratar_dados_sem_utilidade_reason() -> None:
    result = tratar_dados_sem_utilidade("bb_2015_08_loginfesta")
    assert result["reason"] == "dados_sem_utilidade"


def test_tratar_dados_sem_utilidade_profile() -> None:
    result = tratar_dados_sem_utilidade("bb_2015_08_loginfesta")
    assert result["profile"] == "dados_sem_utilidade"


def test_tratar_dados_sem_utilidade_table_name() -> None:
    result = tratar_dados_sem_utilidade("caixa_002_log")
    assert result["table_name"] == "caixa_002_log"


def test_tratar_dados_sem_utilidade_contagem_zero() -> None:
    result = tratar_dados_sem_utilidade("qualquer")
    assert result["n_rows"] == 0
    assert result["n_cols"] == 0


def test_tratar_dados_sem_utilidade_instituicao() -> None:
    result = tratar_dados_sem_utilidade("caixa_002_log")
    assert result["institution"] == "caixa"


def test_tratar_dados_sem_utilidade_metadados_adicionais() -> None:
    """O dict contém todos os campos esperados de metadados."""
    result = tratar_dados_sem_utilidade("teste")
    expected_keys = {
        "table_name",
        "status",
        "reason",
        "n_rows",
        "n_cols",
        "profile",
        "institution",
        "report_date",
        "missing_pct",
        "encoding_issues",
        "date_parse_errors",
        "type_coercion_warnings",
    }
    assert expected_keys.issubset(result.keys())
