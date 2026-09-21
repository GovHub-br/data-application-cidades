"""Testes do módulo de deduplicação.

Cobre calcular_hash, agrupar_duplicatas, eleger_canonicas e gerar_mapping.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pytest

from classificacao.deduplicacao import (
    agrupar_duplicatas,
    calcular_hash,
    eleger_canonicas,
    gerar_mapping,
)


# ─── calcular_hash ────────────────────────────────────────────────────────


def test_calcular_hash_mesmo_conteudo_mesmo_hash(tmp_path: Path) -> None:
    """Arquivos com mesmo conteúdo geram o mesmo hash MD5."""
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text("col1\\tcol2\\n1\\t2\\n")
    b.write_text("col1\\tcol2\\n1\\t2\\n")
    assert calcular_hash(a) == calcular_hash(b)


def test_calcular_hash_conteudo_diferente_hash_diferente(tmp_path: Path) -> None:
    """Arquivos com conteúdo diferente geram hashes diferentes."""
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text("col1\\tcol2\\n1\\t2\\n")
    b.write_text("col1\\tcol2\\n3\\t4\\n")
    assert calcular_hash(a) != calcular_hash(b)


def test_calcular_hash_arquivo_inexistente() -> None:
    """Arquivo inexistente levanta FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        calcular_hash(Path("/nao/existe.csv"))


# ─── agrupar_duplicatas ───────────────────────────────────────────────────


def test_agrupar_duplicatas_tres_arquivos(tmp_path: Path) -> None:
    """Três CSVs no diretório geram DataFrame com 3 registros e colunas esperadas."""
    (tmp_path / "alpha.csv").write_text("a\\tb\\n1\\t2\\n")
    (tmp_path / "beta.csv").write_text("c\\td\\n3\\t4\\n")
    (tmp_path / "gamma.csv").write_text("e\\tf\\n5\\t6\\n")
    df = agrupar_duplicatas(tmp_path)
    assert len(df) == 3
    assert list(df.columns) == ["source_table", "content_hash", "file_size"]
    assert list(df["source_table"]) == ["alpha", "beta", "gamma"]


def test_agrupar_duplicatas_diretorio_vazio(tmp_path: Path) -> None:
    """Diretório vazio retorna DataFrame vazio com colunas esperadas."""
    df = agrupar_duplicatas(tmp_path)
    assert df.empty
    assert list(df.columns) == ["source_table", "content_hash", "file_size"]


def test_agrupar_duplicatas_diretorio_inexistente(tmp_path: Path) -> None:
    """Diretório inexistente retorna DataFrame vazio (com warning)."""
    inexistente = tmp_path / "nao_existe"
    df = agrupar_duplicatas(inexistente)
    assert df.empty
    assert list(df.columns) == ["source_table", "content_hash", "file_size"]


def test_agrupar_duplicatas_ignora_nao_csv(tmp_path: Path) -> None:
    """Arquivos não-CSV (.txt, .md) são ignorados."""
    (tmp_path / "nota.txt").write_text("hello")
    (tmp_path / "dados.csv").write_text("a\\tb\\n1\\t2\\n")
    df = agrupar_duplicatas(tmp_path)
    assert len(df) == 1
    assert df.iloc[0]["source_table"] == "dados"


# ─── eleger_canonicas ─────────────────────────────────────────────────────


def test_eleger_canonicas_primeiro_alfabetico() -> None:
    """Primeira tabela em ordem alfabética é a canônica."""
    df_in = pd.DataFrame(
        {
            "source_table": ["zeta", "alpha", "beta"],
            "content_hash": ["abc"] * 3,
            "file_size": [100] * 3,
        }
    )
    df_out = eleger_canonicas(df_in)
    alpha_row = df_out[df_out["source_table"] == "alpha"].iloc[0]
    assert alpha_row["is_duplicate"] == False
    assert alpha_row["canonical_table"] == "alpha"
    beta_row = df_out[df_out["source_table"] == "beta"].iloc[0]
    assert beta_row["is_duplicate"] == True
    assert beta_row["canonical_table"] == "alpha"


def test_eleger_canonicas_tabela_unica() -> None:
    """Uma única tabela é sua própria canônica."""
    df_in = pd.DataFrame(
        {
            "source_table": ["unico"],
            "content_hash": ["xyz"],
            "file_size": [50],
        }
    )
    df_out = eleger_canonicas(df_in)
    assert len(df_out) == 1
    assert df_out.iloc[0]["is_duplicate"] == False
    assert df_out.iloc[0]["canonical_table"] == "unico"


def test_eleger_canonicas_vazio() -> None:
    """DataFrame vazio retorna DataFrame vazio com colunas esperadas."""
    df_in = pd.DataFrame(columns=["source_table", "content_hash", "file_size"])
    df_out = eleger_canonicas(df_in)
    assert df_out.empty
    assert list(df_out.columns) == [
        "source_table",
        "content_hash",
        "file_size",
        "canonical_table",
        "is_duplicate",
    ]


def test_eleger_canonicas_colisao_hash_tamanho_diferente(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Mesmo hash com tamanhos diferentes emite warning e trata como grupos separados."""
    df_in = pd.DataFrame(
        {
            "source_table": ["a", "b", "c"],
            "content_hash": ["hash1", "hash1", "hash2"],
            "file_size": [100, 200, 100],
        }
    )
    with caplog.at_level(logging.WARNING):
        df_out = eleger_canonicas(df_in)
    assert "Colisão de hash MD5" in caplog.text
    # a e b têm mesmo hash mas tamanhos diferentes → grupos separados → ambas canônicas
    a_row = df_out[df_out["source_table"] == "a"].iloc[0]
    b_row = df_out[df_out["source_table"] == "b"].iloc[0]
    assert a_row["is_duplicate"] == False
    assert b_row["is_duplicate"] == False
    # c tem hash diferente, é canônica
    c_row = df_out[df_out["source_table"] == "c"].iloc[0]
    assert c_row["is_duplicate"] == False


def test_eleger_canonicas_grupos_distintos() -> None:
    """Dois grupos com hashes diferentes são tratados independentemente."""
    df_in = pd.DataFrame(
        {
            "source_table": ["d", "c", "b", "a"],
            "content_hash": ["h1", "h1", "h2", "h2"],
            "file_size": [10, 10, 20, 20],
        }
    )
    df_out = eleger_canonicas(df_in)
    # Grupo h1: c é canônica (primeira alfabética), d é duplicata
    c_row = df_out[df_out["source_table"] == "c"].iloc[0]
    d_row = df_out[df_out["source_table"] == "d"].iloc[0]
    assert c_row["is_duplicate"] == False
    assert d_row["is_duplicate"] == True
    assert d_row["canonical_table"] == "c"
    # Grupo h2: a é canônica, b é duplicata
    a_row = df_out[df_out["source_table"] == "a"].iloc[0]
    b_row = df_out[df_out["source_table"] == "b"].iloc[0]
    assert a_row["is_duplicate"] == False
    assert b_row["is_duplicate"] == True
    assert b_row["canonical_table"] == "a"


# ─── gerar_mapping ────────────────────────────────────────────────────────


def test_gerar_mapping_escreve_csv(tmp_path: Path) -> None:
    """gerar_mapping escreve CSV com colunas corretas e retorna DataFrame original."""
    df_in = pd.DataFrame(
        {
            "source_table": ["alpha", "beta"],
            "content_hash": ["abc", "abc"],
            "file_size": [100, 100],
            "canonical_table": ["alpha", "alpha"],
            "is_duplicate": [False, True],
        }
    )
    output = tmp_path / "_dedup_map.csv"
    resultado = gerar_mapping(df_in, output)
    assert output.exists()
    df_lido = pd.read_csv(output)
    assert list(df_lido.columns) == [
        "source_table",
        "content_hash",
        "canonical_table",
        "is_duplicate",
    ]
    assert len(df_lido) == 2
    # Retorna o DataFrame original (encadeamento)
    pd.testing.assert_frame_equal(resultado, df_in)


def test_gerar_mapping_vazio(tmp_path: Path) -> None:
    """DataFrame vazio escreve CSV apenas com cabeçalho."""
    df_in = pd.DataFrame(
        columns=[
            "source_table",
            "content_hash",
            "file_size",
            "canonical_table",
            "is_duplicate",
        ]
    )
    output = tmp_path / "_dedup_map.csv"
    gerar_mapping(df_in, output)
    df_lido = pd.read_csv(output)
    assert df_lido.empty
    assert list(df_lido.columns) == [
        "source_table",
        "content_hash",
        "canonical_table",
        "is_duplicate",
    ]


def test_gerar_mapping_cria_diretorio_pai(tmp_path: Path) -> None:
    """O diretório pai é criado se não existir."""
    df_in = pd.DataFrame(
        {
            "source_table": ["x"],
            "content_hash": ["h"],
            "file_size": [1],
            "canonical_table": ["x"],
            "is_duplicate": [False],
        }
    )
    output = tmp_path / "sub" / "_dedup_map.csv"
    gerar_mapping(df_in, output)
    assert output.exists()
