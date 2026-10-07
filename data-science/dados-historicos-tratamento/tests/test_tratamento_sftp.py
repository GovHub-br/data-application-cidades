"""Testes do módulo de tratamento SFTP: canonicalização GEFUS e Pipe Padrão B.

Cobre normalizar_nome_sftp, extrair_nomes_colunas, detectar_anomalias_pipe,
agrupar_pares_gefus, eleger_canonicas_gefus, e gerar_mapeamento_gefus.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from classificacao.tratamento_sftp import (
    _COLUNAS_ANDAMENTO_OBRA,
    _canonical_tem_pipe,
    agrupar_pares_gefus,
    detectar_anomalias_pipe,
    eleger_canonicas_gefus,
    extrair_nomes_colunas,
    gerar_mapeamento_gefus,
    normalizar_nome_sftp,
)


# ══════════════════════════════════════════════════════════════════════════
# normalizar_nome_sftp
# ══════════════════════════════════════════════════════════════════════════


class TestNormalizarNomeSftp:
    """Testes para normalizar_nome_sftp()."""

    def test_remove_gefus_anteriores(self) -> None:
        """Prefixo gefus_anteriores_ é removido."""
        assert normalizar_nome_sftp("gefus_anteriores_minha_tabela") == "minha_tabela"

    def test_remove_gefus(self) -> None:
        """Prefixo gefus_ é removido."""
        assert normalizar_nome_sftp("gefus_minha_tabela") == "minha_tabela"

    def test_sem_prefixo(self) -> None:
        """Nome sem prefixo retorna inalterado."""
        assert normalizar_nome_sftp("minha_tabela") == "minha_tabela"

    def test_gefus_anteriores_tem_precedencia(self) -> None:
        """gefus_anteriores_ é removido antes de gefus_."""
        result = normalizar_nome_sftp("gefus_anteriores_gefus_tabela")
        assert result == "gefus_tabela"

    def test_vazio(self) -> None:
        """String vazia retorna vazio."""
        assert normalizar_nome_sftp("") == ""

    def test_apenas_prefixo_gefus(self) -> None:
        """Nome igual ao prefixo 'gefus_' retorna vazio."""
        assert normalizar_nome_sftp("gefus_") == ""

    def test_apenas_prefixo_anteriores(self) -> None:
        """Nome igual ao prefixo 'gefus_anteriores_' retorna vazio."""
        assert normalizar_nome_sftp("gefus_anteriores_") == ""


# ══════════════════════════════════════════════════════════════════════════
# extrair_nomes_colunas
# ══════════════════════════════════════════════════════════════════════════


class TestExtrairNomesColunas:
    """Testes para extrair_nomes_colunas()."""

    def test_familia_andamento_obra_mapeamento_fixo(self) -> None:
        """Prefixo anomes_nu_apf_dt_prevista_conclusao retorna 5 campos fixos."""
        nome = (
            "anomes_nu_apf_dt_prevista_conclusao_dt_prevista_inauguracao_situacao_obra"
        )
        resultado = extrair_nomes_colunas(nome)
        assert resultado == _COLUNAS_ANDAMENTO_OBRA
        assert len(resultado) == 5

    def test_familia_andamento_obra_com_sufixo(self) -> None:
        """Prefixo conhecido com conteúdo extra ainda retorna mapeamento fixo."""
        nome = "anomes_nu_apf_dt_prevista_conclusao_extra_stuff"
        resultado = extrair_nomes_colunas(nome)
        assert resultado == _COLUNAS_ANDAMENTO_OBRA

    def test_heuristico_prefixos_conhecidos(self) -> None:
        """Nomes com prefixos conhecidos (nu_, dt_, vr_) são separados."""
        nome = "nu_contrato_dt_vencimento_vr_total"
        resultado = extrair_nomes_colunas(nome)
        assert resultado == ["nu_contrato", "dt_vencimento", "vr_total"]

    def test_nome_sem_prefixo_conhecido(self) -> None:
        """Nome sem prefixo conhecido retorna como campo único."""
        nome = "nome_completo_do_cliente"
        resultado = extrair_nomes_colunas(nome)
        assert resultado == [nome]

    def test_empty_string(self) -> None:
        """String vazia retorna lista vazia."""
        assert extrair_nomes_colunas("") == []

    def test_apenas_prefixos(self) -> None:
        """Sequência de apenas prefixos conhecidos."""
        nome = "nu_dt_co"
        resultado = extrair_nomes_colunas(nome)
        assert resultado == ["nu", "dt", "co"]

    def test_prefixo_no_meio_ignorado(self) -> None:
        """Prefixo conhecido no meio (não no início de um campo) não causa split."""
        nome = "alguma_coisa_nu_contrato"
        # 'coisa' não termina o campo anterior, 'nu' inicia novo
        resultado = extrair_nomes_colunas(nome)
        assert resultado == ["alguma_coisa", "nu_contrato"]

    def test_campos_unicos_com_prefixo(self) -> None:
        """Campo único iniciado por prefixo conhecido."""
        nome = "nu_contrato"
        resultado = extrair_nomes_colunas(nome)
        assert resultado == ["nu_contrato"]


# ══════════════════════════════════════════════════════════════════════════
# detectar_anomalias_pipe
# ══════════════════════════════════════════════════════════════════════════


class TestDetectarAnomaliasPipe:
    """Testes para detectar_anomalias_pipe()."""

    def test_todas_linhas_ok(self) -> None:
        """DataFrame sem nulos nas primeiras n_esperado colunas."""
        df = pd.DataFrame({"a": ["1", "2", "3"], "b": ["x", "y", "z"]})
        resultado = detectar_anomalias_pipe(df, n_esperado=2)
        assert resultado["total_linhas"] == 3
        assert resultado["linhas_ok"] == 3
        assert resultado["linhas_divergentes"] == 0

    def test_com_linhas_divergentes(self) -> None:
        """Linhas com valores nulos contam como divergentes."""
        df = pd.DataFrame({"a": ["1", None, "3"], "b": ["x", "y", None]})
        resultado = detectar_anomalias_pipe(df, n_esperado=2)
        assert resultado["total_linhas"] == 3
        assert resultado["linhas_ok"] == 1
        assert resultado["linhas_divergentes"] == 2

    def test_amostras_divergentes_limitadas_a_5(self) -> None:
        """Máximo de 5 amostras divergentes são retornadas."""
        dados = {f"col{i}": [None] * 10 for i in range(3)}
        df = pd.DataFrame(dados)
        resultado = detectar_anomalias_pipe(df, n_esperado=3)
        assert resultado["total_linhas"] == 10
        assert resultado["linhas_divergentes"] == 10
        assert len(resultado["amostras_divergentes"]) <= 5
        assert "_indice" in resultado["amostras_divergentes"][0]

    def test_empty_dataframe(self) -> None:
        """DataFrame vazio retorna tudo zero."""
        df = pd.DataFrame()
        resultado = detectar_anomalias_pipe(df, n_esperado=2)
        assert resultado["total_linhas"] == 0
        assert resultado["linhas_ok"] == 0
        assert resultado["linhas_divergentes"] == 0
        assert resultado["amostras_divergentes"] == []

    def test_apenas_primeiras_n_colunas_analisadas(self) -> None:
        """Apenas as primeiras n_esperado colunas são verificadas."""
        df = pd.DataFrame(
            {
                "a": [None, "1"],
                "b": [None, "2"],
                "extra": ["x", "y"],
            }
        )
        resultado = detectar_anomalias_pipe(df, n_esperado=2)
        assert resultado["linhas_divergentes"] == 1
        assert resultado["linhas_ok"] == 1

    def test_todas_divergentes(self) -> None:
        """Todas as linhas têm pelo menos um nulo."""
        df = pd.DataFrame({"a": [None, None], "b": ["x", None]})
        resultado = detectar_anomalias_pipe(df, n_esperado=2)
        assert resultado["linhas_divergentes"] == 2
        assert resultado["linhas_ok"] == 0


# ══════════════════════════════════════════════════════════════════════════
# Fixtures compartilhados
# ══════════════════════════════════════════════════════════════════════════


@pytest.fixture
def mock_engine() -> MagicMock:
    """Engine SQLAlchemy mock com suporte a context manager."""
    engine = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    engine.connect.return_value = conn
    return engine


def _configurar_tabelas(engine: MagicMock, tabelas: list[str]) -> MagicMock:
    """Configura engine para retornar lista de tabelas na consulta SQL."""
    conn = engine.connect.return_value.__enter__.return_value
    result = MagicMock()
    result.__iter__.return_value = [(t,) for t in tabelas]
    conn.execute.return_value = result
    return conn


def _configurar_row_counts(engine: MagicMock, counts: list[int]) -> MagicMock:
    """Configura conn.scalar() para retornar contagens sequenciais."""
    conn = engine.connect.return_value.__enter__.return_value
    conn.scalar.side_effect = counts
    return conn


# ══════════════════════════════════════════════════════════════════════════
# agrupar_pares_gefus
# ══════════════════════════════════════════════════════════════════════════


class TestAgruparParesGefus:
    """Testes para agrupar_pares_gefus()."""

    def test_par_completo(self, mock_engine: MagicMock) -> None:
        """Tabela com e sem prefixo GEFUS formam par."""
        _configurar_tabelas(mock_engine, ["minha_tabela", "gefus_minha_tabela"])
        pares = agrupar_pares_gefus(mock_engine)
        assert "minha_tabela" in pares
        assert pares["minha_tabela"]["nao_gefus"] == ["minha_tabela"]
        assert pares["minha_tabela"]["gefus"] == ["gefus_minha_tabela"]

    def test_apenas_gefus(self, mock_engine: MagicMock) -> None:
        """Tabela apenas com prefixo GEFUS (standalone)."""
        _configurar_tabelas(mock_engine, ["gefus_standalone"])
        pares = agrupar_pares_gefus(mock_engine)
        assert "standalone" in pares
        assert pares["standalone"]["nao_gefus"] == []
        assert pares["standalone"]["gefus"] == ["gefus_standalone"]

    def test_apenas_nao_gefus(self, mock_engine: MagicMock) -> None:
        """Tabela sem prefixo GEFUS."""
        _configurar_tabelas(mock_engine, ["sem_par"])
        pares = agrupar_pares_gefus(mock_engine)
        assert "sem_par" in pares
        assert pares["sem_par"]["nao_gefus"] == ["sem_par"]
        assert pares["sem_par"]["gefus"] == []

    def test_gefus_anteriores(self, mock_engine: MagicMock) -> None:
        """gefus_anteriores_ é classificado como GEFUS."""
        _configurar_tabelas(mock_engine, ["tab", "gefus_anteriores_tab"])
        pares = agrupar_pares_gefus(mock_engine)
        assert pares["tab"]["gefus"] == ["gefus_anteriores_tab"]

    def test_multiplos_gefus_mesmo_base(self, mock_engine: MagicMock) -> None:
        """Múltiplas versões GEFUS para o mesmo nome base."""
        _configurar_tabelas(mock_engine, ["tab", "gefus_tab", "gefus_anteriores_tab"])
        pares = agrupar_pares_gefus(mock_engine)
        assert len(pares["tab"]["gefus"]) == 2

    def test_schema_vazio(self, mock_engine: MagicMock) -> None:
        """Schema sem tabelas retorna dict vazio."""
        _configurar_tabelas(mock_engine, [])
        pares = agrupar_pares_gefus(mock_engine)
        assert pares == {}

    def test_varias_tabelas_distintas(self, mock_engine: MagicMock) -> None:
        """Múltiplos grupos são criados corretamente."""
        _configurar_tabelas(
            mock_engine,
            [
                "tab_a",
                "gefus_tab_a",
                "tab_b",
                "gefus_anteriores_tab_b",
                "tab_c",
            ],
        )
        pares = agrupar_pares_gefus(mock_engine)
        assert set(pares.keys()) == {"tab_a", "tab_b", "tab_c"}
        assert pares["tab_a"]["gefus"] == ["gefus_tab_a"]
        assert pares["tab_b"]["gefus"] == ["gefus_anteriores_tab_b"]
        assert pares["tab_c"]["gefus"] == []


# ══════════════════════════════════════════════════════════════════════════
# eleger_canonicas_gefus
# ══════════════════════════════════════════════════════════════════════════


class TestElegerCanonicasGefus:
    """Testes para eleger_canonicas_gefus()."""

    def test_par_com_match(self, mock_engine: MagicMock) -> None:
        """Row counts iguais: GEFUS é eleita canônica."""
        _configurar_tabelas(mock_engine, [])  # não usado, pares é dict
        pares = {"tab": {"nao_gefus": ["tab"], "gefus": ["gefus_tab"]}}
        _configurar_row_counts(mock_engine, [10, 10])
        df = eleger_canonicas_gefus(pares, mock_engine)
        assert len(df) == 1
        assert df.iloc[0]["canonical_table"] == "gefus_tab"
        assert df.iloc[0]["table_name"] == "tab"
        assert df.iloc[0]["row_count_match"] == "True"

    def test_par_sem_match_gefus_maior(self, mock_engine: MagicMock) -> None:
        """GEFUS com mais linhas: GEFUS é eleita mesmo sem match."""
        pares = {"tab": {"nao_gefus": ["tab"], "gefus": ["gefus_tab"]}}
        _configurar_row_counts(mock_engine, [5, 20])
        df = eleger_canonicas_gefus(pares, mock_engine)
        assert len(df) == 1
        assert df.iloc[0]["canonical_table"] == "gefus_tab"
        assert df.iloc[0]["row_count_match"] == "False"

    def test_par_sem_match_nao_gefus_maior(self, mock_engine: MagicMock) -> None:
        """Não-GEFUS com mais linhas: não-GEFUS é eleita."""
        pares = {"tab": {"nao_gefus": ["tab"], "gefus": ["gefus_tab"]}}
        _configurar_row_counts(mock_engine, [30, 10])
        df = eleger_canonicas_gefus(pares, mock_engine)
        assert len(df) == 1
        assert df.iloc[0]["canonical_table"] == "tab"
        assert df.iloc[0]["row_count_match"] == "False"

    def test_gefus_standalone(self, mock_engine: MagicMock) -> None:
        """Apenas GEFUS (sem par): ela mesma é canônica."""
        pares = {"standalone": {"nao_gefus": [], "gefus": ["gefus_standalone"]}}
        _configurar_row_counts(mock_engine, [42])
        df = eleger_canonicas_gefus(pares, mock_engine)
        assert len(df) == 1
        assert df.iloc[0]["canonical_table"] == "gefus_standalone"
        assert df.iloc[0]["table_name"] == "gefus_standalone"
        assert df.iloc[0]["n_rows_canonical"] == 42
        assert pd.isna(df.iloc[0]["row_count_match"])

    def test_apenas_nao_gefus(self, mock_engine: MagicMock) -> None:
        """Sem contraparte GEFUS: não entra no resultado."""
        pares = {"tab": {"nao_gefus": ["tab"], "gefus": []}}
        df = eleger_canonicas_gefus(pares, mock_engine)
        assert df.empty

    def test_varios_pares(self, mock_engine: MagicMock) -> None:
        """Múltiplos pares são processados corretamente."""
        pares = {
            "a": {"nao_gefus": ["a"], "gefus": ["gefus_a"]},
            "b": {"nao_gefus": ["b"], "gefus": ["gefus_b"]},
        }
        _configurar_row_counts(mock_engine, [1, 1, 2, 2])
        df = eleger_canonicas_gefus(pares, mock_engine)
        assert len(df) == 2

    def test_row_counts_sao_inteiros(self, mock_engine: MagicMock) -> None:
        """Colunas n_rows_orig e n_rows_canonical são int."""
        pares = {"tab": {"nao_gefus": ["tab"], "gefus": ["gefus_tab"]}}
        _configurar_row_counts(mock_engine, [7, 7])
        df = eleger_canonicas_gefus(pares, mock_engine)
        assert pd.api.types.is_integer_dtype(df["n_rows_orig"])
        assert pd.api.types.is_integer_dtype(df["n_rows_canonical"])

    def test_parcial_standalone_e_par(self, mock_engine: MagicMock) -> None:
        """Mistura de pares e standalone."""
        pares = {
            "par": {"nao_gefus": ["par"], "gefus": ["gefus_par"]},
            "so_gefus": {"nao_gefus": [], "gefus": ["gefus_so_gefus"]},
        }
        _configurar_row_counts(mock_engine, [5, 5, 100])
        df = eleger_canonicas_gefus(pares, mock_engine)
        assert len(df) == 2


# ══════════════════════════════════════════════════════════════════════════
# gerar_mapeamento_gefus
# ══════════════════════════════════════════════════════════════════════════


class TestGerarMapeamentoGefus:
    """Testes para gerar_mapeamento_gefus()."""

    def test_escreve_csv_com_colunas_corretas(self, tmp_path: Path) -> None:
        """CSV gerado com colunas esperadas."""
        df = pd.DataFrame(
            {
                "table_name": ["tab_a", "tab_b"],
                "canonical_table": ["gefus_tab_a", "gefus_tab_b"],
                "n_rows_orig": [100, 200],
                "n_rows_canonical": [100, 200],
                "is_pipe": [False, False],
                "row_count_match": [True, True],
            }
        )
        output = tmp_path / "mapeamento.csv"
        caminho = gerar_mapeamento_gefus(df, str(output))
        assert Path(caminho).exists()
        df_lido = pd.read_csv(output)
        assert list(df_lido.columns) == [
            "table_name",
            "canonical_table",
            "n_rows_orig",
            "n_rows_canonical",
            "is_pipe",
            "row_count_match",
        ]
        assert len(df_lido) == 2

    def test_dataframe_vazio(self, tmp_path: Path) -> None:
        """DataFrame vazio gera CSV apenas com cabeçalho."""
        df = pd.DataFrame(
            columns=[
                "table_name",
                "canonical_table",
                "n_rows_orig",
                "n_rows_canonical",
                "is_pipe",
                "row_count_match",
            ]
        )
        output = tmp_path / "vazio.csv"
        caminho = gerar_mapeamento_gefus(df, str(output))
        assert Path(caminho).exists()
        df_lido = pd.read_csv(output)
        assert df_lido.empty

    def test_cria_diretorio_pai(self, tmp_path: Path) -> None:
        """Diretório pai é criado automaticamente se não existir."""
        df = pd.DataFrame(
            {
                "table_name": ["x"],
                "canonical_table": ["y"],
                "n_rows_orig": [1],
                "n_rows_canonical": [1],
                "is_pipe": [False],
                "row_count_match": [True],
            }
        )
        output = tmp_path / "subdir" / "mapeamento.csv"
        caminho = gerar_mapeamento_gefus(df, str(output))
        assert Path(caminho).exists()

    def test_retorna_caminho_absoluto(self, tmp_path: Path) -> None:
        """Retorna o caminho absoluto do arquivo escrito."""
        df = pd.DataFrame(
            {
                "table_name": ["x"],
                "canonical_table": ["x"],
                "n_rows_orig": [1],
                "n_rows_canonical": [1],
                "is_pipe": [False],
                "row_count_match": [True],
            }
        )
        output = tmp_path / "out.csv"
        caminho = gerar_mapeamento_gefus(df, str(output))
        assert Path(caminho).is_absolute()
        assert Path(caminho) == Path(output).resolve()

    def test_quoting_non_numeric(self, tmp_path: Path) -> None:
        """Valores não-numéricos são escritos entre aspas."""
        df = pd.DataFrame(
            {
                "table_name": ["tab"],
                "canonical_table": ["gefus_tab"],
                "n_rows_orig": [10],
                "n_rows_canonical": [10],
                "is_pipe": [False],
                "row_count_match": [True],
            }
        )
        output = tmp_path / "quoting.csv"
        gerar_mapeamento_gefus(df, str(output))
        conteudo = Path(output).read_text()
        assert '"tab"' in conteudo
        assert '"gefus_tab"' in conteudo


# ══════════════════════════════════════════════════════════════════════════
# _canonical_tem_pipe
# ══════════════════════════════════════════════════════════════════════════


class TestCanonicalTemPipe:
    """Testes para _canonical_tem_pipe()."""

    def test_col0_longa_com_underscore_retorna_true(self) -> None:
        """Col0 com nome > 40 chars e contendo '_' deve retornar True."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_conn.scalar.return_value = "anomes_nu_apf_dt_prevista_conclusao_dt_prevista_inauguracao_situaca"

        result = _canonical_tem_pipe(mock_engine, "tab_pipe", "sftp")
        assert result is True

    def test_col0_curta_retorna_false(self) -> None:
        """Col0 com nome curto (< 40 chars) deve retornar False."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_conn.scalar.return_value = "anomes"

        result = _canonical_tem_pipe(mock_engine, "tab_normal", "sftp")
        assert result is False

    def test_col0_longa_sem_underscore_retorna_false(self) -> None:
        """Col0 com nome > 40 chars mas sem '_' deve retornar False."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_conn.scalar.return_value = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz12345"

        result = _canonical_tem_pipe(mock_engine, "tab_sem_underscore", "sftp")
        assert result is False

    def test_col0_null_retorna_false(self) -> None:
        """Col0 é NULL (tabela sem colunas) deve retornar False."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_conn.scalar.return_value = None

        result = _canonical_tem_pipe(mock_engine, "tab_vazia", "sftp")
        assert result is False
