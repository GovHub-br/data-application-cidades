"""Testes do módulo ``tratamento_nulos_duplicados`` (nulos/duplicados APF-aware).

Cobre classificação de nulos, conversão de placeholders e deduplicação
semântica ciente de APF (mesmo APF em fases distintas → preservado; duplicata
exata → removida; ``apf`` isolado como chave → erro).
"""

from __future__ import annotations

import pandas as pd
import pytest

from classificacao.tratamento_nulos_duplicados import (
    classificar_nulos,
    converter_placeholders,
    deduplicar_ciente_apf,
)


# ──────────────────────────────────────────────────────────────────────
# classificar_nulos
# ──────────────────────────────────────────────────────────────────────


def test_classificar_nulos_categorias() -> None:
    serie = pd.Series(["valor", "", "-", "NULL", "null", None, "1900-01-01"])
    out = classificar_nulos(serie)
    assert out.tolist() == [
        "preenchido",
        "placeholder",
        "placeholder",
        "placeholder",
        "placeholder",
        "nulo_legitimo",
        "placeholder",
    ]


def test_classificar_nulos_zero_so_em_id_data() -> None:
    # coluna de código/ID: "0" e "0,00" são placeholders
    serie_id = pd.Series(["123", "0", "0,00", "456", None], name="cod_municipio_ibge")
    out_id = classificar_nulos(serie_id)
    assert out_id.tolist() == [
        "preenchido",
        "placeholder",
        "placeholder",
        "preenchido",
        "nulo_a_preencher",
    ]

    # coluna de data: zeros também são placeholders
    serie_dt = pd.Series(["2024-01-01", "0", "0,00"], name="dt_movimento")
    out_dt = classificar_nulos(serie_dt)
    assert out_dt.tolist() == ["preenchido", "placeholder", "placeholder"]

    # coluna não-id/data: "0" é valor legítimo
    serie_valor = pd.Series(["10", "0", "0,00", ""], name="percentual_obra")
    out_valor = classificar_nulos(serie_valor)
    assert out_valor.tolist() == ["preenchido", "preenchido", "preenchido", "placeholder"]


def test_classificar_nulos_nulo_a_preencher_em_id_data() -> None:
    serie = pd.Series([None, "1"], name="nu_apf")
    out = classificar_nulos(serie)
    assert out.tolist() == ["nulo_a_preencher", "preenchido"]


def test_classificar_nulos_zero_numerico_em_id_data() -> None:
    serie = pd.Series([0.0, 123.0, None], name="codigo_ibge")
    out = classificar_nulos(serie)
    assert out.tolist() == ["placeholder", "preenchido", "nulo_a_preencher"]


# ──────────────────────────────────────────────────────────────────────
# converter_placeholders
# ──────────────────────────────────────────────────────────────────────


def test_converter_placeholders_strings() -> None:
    df = pd.DataFrame(
        {
            "cod_municipio_ibge": ["3550308", "", "-", "NULL", None],
            "nome_municipio": ["São Paulo", "", "-", "null", "Brasiléia"],
            "dt_movimento": ["2024-01-01", "1900-01-01", "0", "0,00", "2024-02-01"],
        }
    )
    out = converter_placeholders(df)
    # valores vazios/-/NULL/None viram nulos; apenas o código válido permanece
    assert out["cod_municipio_ibge"].isna().tolist() == [False, True, True, True, True]
    assert out["cod_municipio_ibge"].dropna().tolist() == ["3550308"]
    # vazio/-/null são globais (todas as colunas)
    assert out["nome_municipio"].isna().tolist() == [False, True, True, True, False]
    assert out["nome_municipio"].dropna().tolist() == ["São Paulo", "Brasiléia"]
    # "1900-01-01" é global; "0"/"0,00" valem só em id/data-like (dt_movimento)
    assert out["dt_movimento"].isna().tolist() == [False, True, True, True, False]
    assert out["dt_movimento"].dropna().tolist() == ["2024-01-01", "2024-02-01"]


def test_converter_placeholders_zero_numerico_id_data() -> None:
    df = pd.DataFrame({"codigo_ibge": pd.Series([0, 3550308, 0], dtype="Int64")})
    out = converter_placeholders(df)
    assert out["codigo_ibge"].isna().tolist() == [True, False, True]
    assert out["codigo_ibge"].dropna().tolist() == [3550308]


def test_converter_placeholders_preserva_valores_validos() -> None:
    df = pd.DataFrame({"percentual_obra": ["100,00", "0", "50,50"], "uf": ["SP", "0", "MG"]})
    out = converter_placeholders(df)
    # "0" em coluna não-id/data é valor legítimo e não vira nulo
    assert out["percentual_obra"].tolist() == ["100,00", "0", "50,50"]
    # "0" em coluna de UF não é placeholder (não-id/data)
    assert out["uf"].tolist() == ["SP", "0", "MG"]


# ──────────────────────────────────────────────────────────────────────
# deduplicar_ciente_apf
# ──────────────────────────────────────────────────────────────────────


def _df_apf() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "nu_apf": ["A1", "A1", "A1", "A2", "A2", "A2"],
            "fase": ["PROJETO", "OBRA", "OBRA", "PROJETO", "PROJETO", "CONCLUIDO"],
            "dt_movimento": ["2024-01-31", "2024-03-31", "2024-03-31", "2024-01-31", "2024-03-31", "2024-03-31"],
            "valor": [10, 20, 20, 30, 40, 50],
        }
    )


def test_dedup_preserva_mesmo_apf_em_fases_distintas() -> None:
    df = _df_apf()
    out = deduplicar_ciente_apf(
        df,
        chave_negocio=["nu_apf"],
        colunas_fase=["fase"],
        coluna_snapshot="dt_movimento",
    )
    # A1 tem PROJETO e OBRA (fases distintas) -> preservados;
    # a linha A1/OBRA/2024-03-31 duplicada exata é removida;
    # A2/PROJETO/2024-01-31 e A2/PROJETO/2024-03-31 (snapshots distintos) preservados.
    assert len(out) == 5
    assert out["nu_apf"].tolist() == ["A1", "A1", "A2", "A2", "A2"]
    assert out["fase"].tolist() == ["PROJETO", "OBRA", "PROJETO", "PROJETO", "CONCLUIDO"]


def test_dedup_remove_duplicata_exata() -> None:
    df = _df_apf()
    out = deduplicar_ciente_apf(
        df,
        chave_negocio=["nu_apf"],
        colunas_fase=["fase"],
        coluna_snapshot="dt_movimento",
    )
    duplicada = out[out.duplicated(subset=["nu_apf", "fase", "dt_movimento"])]
    assert duplicada.empty


def test_dedup_sem_coluna_snapshot() -> None:
    df = _df_apf().drop(columns=["dt_movimento"])
    out = deduplicar_ciente_apf(
        df, chave_negocio=["nu_apf"], colunas_fase=["fase"], coluna_snapshot=None
    )
    # A1: PROJETO e OBRA preservados; A2/PROJETO duplicado exato removido
    assert out["nu_apf"].tolist() == ["A1", "A1", "A2", "A2"]
    assert out["fase"].tolist() == ["PROJETO", "OBRA", "PROJETO", "CONCLUIDO"]


def test_dedup_apf_isolado_levanta_erro() -> None:
    df = _df_apf()
    with pytest.raises(ValueError, match="apf"):
        deduplicar_ciente_apf(
            df, chave_negocio=["nu_apf"], colunas_fase=[], coluna_snapshot=None
        )


def test_dedup_chave_vazia_levanta_erro() -> None:
    df = _df_apf()
    with pytest.raises(ValueError, match="chave_negocio"):
        deduplicar_ciente_apf(df, chave_negocio=[], colunas_fase=[], coluna_snapshot=None)


def test_dedup_coluna_ausente_levanta_erro() -> None:
    df = _df_apf()
    with pytest.raises(KeyError, match="nu_apf_fake"):
        deduplicar_ciente_apf(
            df,
            chave_negocio=["nu_apf", "nu_apf_fake"],
            colunas_fase=[],
            coluna_snapshot=None,
        )
    with pytest.raises(KeyError, match="dt_movimento"):
        deduplicar_ciente_apf(
            df.drop(columns=["dt_movimento"]),
            chave_negocio=["nu_apf"],
            colunas_fase=["fase"],
            coluna_snapshot="dt_movimento",
        )


def test_dedup_nao_altera_entrada() -> None:
    df = _df_apf()
    original = df.copy()
    deduplicar_ciente_apf(
        df,
        chave_negocio=["nu_apf"],
        colunas_fase=["fase"],
        coluna_snapshot="dt_movimento",
    )
    pd.testing.assert_frame_equal(df, original)