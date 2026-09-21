"""Testes do módulo de reconstrução de colunas fragmentadas (Padrão A).

Cobre atribuir_nomes_genericos, split_reconstruido, inferir_tipos_colunas,
estimar_truncamento, e detectar_padrao_a.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from classificacao.reconstrucao_colunas import (
    atribuir_nomes_genericos,
    detectar_padrao_a,
    estimar_truncamento,
    inferir_tipos_colunas,
    split_reconstruido,
)


# ══════════════════════════════════════════════════════════════════════════
# atribuir_nomes_genericos
# ══════════════════════════════════════════════════════════════════════════


class TestAtribuirNomesGenericos:
    """Testes para atribuir_nomes_genericos()."""

    def test_zero_colunas(self) -> None:
        """0 colunas retorna lista vazia."""
        assert atribuir_nomes_genericos(0) == []

    def test_uma_coluna(self) -> None:
        """1 coluna: col_0."""
        assert atribuir_nomes_genericos(1) == ["col_0"]

    def test_cinco_colunas(self) -> None:
        """5 colunas: col_0 … col_4."""
        resultado = atribuir_nomes_genericos(5)
        assert resultado == [
            "col_0",
            "col_1",
            "col_2",
            "col_3",
            "col_4",
        ]

    def test_cem_colunas(self) -> None:
        """100 colunas: col_0 … col_99."""
        resultado = atribuir_nomes_genericos(100)
        assert len(resultado) == 100
        assert resultado[0] == "col_0"
        assert resultado[99] == "col_99"


# ══════════════════════════════════════════════════════════════════════════
# split_reconstruido
# ══════════════════════════════════════════════════════════════════════════


class TestSplitReconstruido:
    """Testes para split_reconstruido()."""

    def test_consistente_n_campos_fixo(self) -> None:
        """Split com n_campos fornecido gera colunas corretas."""
        df_in = pd.DataFrame({"reconstructed": ["a|b|c", "d|e|f", "g|h|i"]})
        df_out = split_reconstruido(df_in, n_campos=3)
        assert df_out.shape == (3, 3)
        assert list(df_out.columns) == ["col_0", "col_1", "col_2"]
        assert df_out["col_0"].tolist() == ["a", "d", "g"]
        assert df_out["col_2"].tolist() == ["c", "f", "i"]

    def test_auto_n_campos(self) -> None:
        """Inferência automática de n_campos a partir do máximo de pipes."""
        df_in = pd.DataFrame({"reconstructed": ["a|b|c", "d|e|f|g"]})
        df_out = split_reconstruido(df_in, n_campos=None)
        # 3 pipes na 2ª linha → 4 campos
        assert df_out.shape[1] == 4

    def test_linhas_com_menos_campos(self) -> None:
        """Linhas com menos campos que n_campos recebem None."""
        df_in = pd.DataFrame({"reconstructed": ["a|b|c", "d|e"]})
        df_out = split_reconstruido(df_in, n_campos=3)
        assert df_out.shape[1] == 3
        assert pd.isna(df_out["col_2"].iloc[1])

    def test_linhas_nulas_removidas(self) -> None:
        """Linhas com reconstructed nulo ou vazio são removidas."""
        df_in = pd.DataFrame({"reconstructed": ["a|b", None, ""]})
        df_out = split_reconstruido(df_in, n_campos=2)
        assert len(df_out) == 1  # apenas "a|b"

    def test_todas_nulas_retorna_vazio(self) -> None:
        """Sem dados válidos, retorna DataFrame vazio."""
        df_in = pd.DataFrame({"reconstructed": [None, None]})
        df_out = split_reconstruido(df_in, n_campos=2)
        assert df_out.empty

    def test_sem_coluna_reconstructed_levanta_erro(self) -> None:
        """DataFrame sem coluna 'reconstructed' levanta ValueError."""
        df_in = pd.DataFrame({"outra": ["x"]})
        with pytest.raises(ValueError, match="reconstructed"):
            split_reconstruido(df_in)

    def test_truncamento_de_linhas_excedentes(self) -> None:
        """Linhas com mais campos que n_campos são truncadas."""
        df_in = pd.DataFrame({"reconstructed": ["a|b|c|d|e", "f|g"]})
        df_out = split_reconstruido(df_in, n_campos=3)
        assert df_out.shape[1] == 3
        # Primeira linha perde os campos excedentes
        assert df_out["col_0"].iloc[0] == "a"
        assert df_out["col_2"].iloc[0] == "c"

    def test_amostra_vazia_inferencia(self) -> None:
        """Se a amostra para inferência for vazia, n_campos=1."""
        df_in = pd.DataFrame({"reconstructed": []}, dtype=str)
        df_out = split_reconstruido(df_in, n_campos=None)
        assert df_out.empty


# ══════════════════════════════════════════════════════════════════════════
# inferir_tipos_colunas
# ══════════════════════════════════════════════════════════════════════════


class TestInferirTiposColunas:
    """Testes para inferir_tipos_colunas()."""

    def test_data_detection(self) -> None:
        """Coluna com valores de data retorna 'date'."""
        df = pd.DataFrame({"col_0": ["2024-01-01", "2024-02-15", "2024-03-30"]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["tipo_inferido"] == "date"

    def test_number_detection(self) -> None:
        """Coluna com números retorna 'number'."""
        df = pd.DataFrame({"col_0": ["123", "456.78", "-9"]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["tipo_inferido"] == "number"

    def test_text_detection(self) -> None:
        """Coluna com texto genérico retorna 'text'."""
        df = pd.DataFrame({"col_0": ["foo", "bar", "baz"]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["tipo_inferido"] == "text"

    def test_majority_date_wins(self) -> None:
        """Maioria de datas → 'date'."""
        df = pd.DataFrame({"col_0": ["2024-01-01", "2024-02-15", "abc"]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["tipo_inferido"] == "date"

    def test_majority_number_wins(self) -> None:
        """Maioria de números → 'number'."""
        df = pd.DataFrame({"col_0": ["123", "456", "abc"]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["tipo_inferido"] == "number"

    def test_coluna_totalmente_vazia(self) -> None:
        """Coluna sem valores não-nulos retorna 'text'."""
        df = pd.DataFrame({"col_0": [None, None, None]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["tipo_inferido"] == "text"

    def test_coluna_com_nulos_misturados(self) -> None:
        """Nulos são ignorados na inferência."""
        df = pd.DataFrame({"col_0": ["2024-01-01", None, "2024-03-30"]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["tipo_inferido"] == "date"

    def test_varias_colunas(self) -> None:
        """Cada coluna gera um registro no DataFrame de saída."""
        df = pd.DataFrame(
            {
                "data": ["2024-01-01"],
                "valor": ["123.45"],
                "nome": ["João"],
            }
        )
        df_tipos = inferir_tipos_colunas(df)
        assert len(df_tipos) == 3
        tipos = dict(zip(df_tipos["coluna"], df_tipos["tipo_inferido"]))
        assert tipos["data"] == "date"
        assert tipos["valor"] == "number"
        assert tipos["nome"] == "text"

    def test_amostras_preenchidas(self) -> None:
        """Até 3 valores de amostra são coletados por coluna."""
        df = pd.DataFrame(
            {
                "col_0": [
                    "2024-01-01",
                    "2024-02-15",
                    "2024-03-30",
                    "2024-04-01",
                ]
            }
        )
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["amostra_1"] == "2024-01-01"
        assert df_tipos.iloc[0]["amostra_2"] == "2024-02-15"
        assert df_tipos.iloc[0]["amostra_3"] == "2024-03-30"

    def test_amostras_vazias_para_coluna_sem_valores(self) -> None:
        """Coluna vazia tem amostras como string vazia."""
        df = pd.DataFrame({"col_0": [None, None]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["amostra_1"] == ""
        assert df_tipos.iloc[0]["amostra_2"] == ""
        assert df_tipos.iloc[0]["amostra_3"] == ""

    def test_numero_com_virgula(self) -> None:
        """Número com vírgula decimal é detectado como number."""
        df = pd.DataFrame({"col_0": ["1,5", "2,3", "3,7"]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["tipo_inferido"] == "number"

    def test_formato_data_br(self) -> None:
        """Data em formato DD/MM/YYYY é detectada."""
        df = pd.DataFrame({"col_0": ["01/01/2024", "15/02/2024"]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["tipo_inferido"] == "date"

    def test_formato_data_iso(self) -> None:
        """Data em formato YYYY/MM/DD é detectada."""
        df = pd.DataFrame({"col_0": ["2024/01/01", "2024/02/15"]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["tipo_inferido"] == "date"

    def test_formato_data_compacto(self) -> None:
        """Data em formato YYYYMMDD é detectada."""
        df = pd.DataFrame({"col_0": ["20240101", "20240215"]})
        df_tipos = inferir_tipos_colunas(df)
        assert df_tipos.iloc[0]["tipo_inferido"] == "date"


# ══════════════════════════════════════════════════════════════════════════
# estimar_truncamento
# ══════════════════════════════════════════════════════════════════════════


class TestEstimarTruncamento:
    """Testes para estimar_truncamento()."""

    def test_sem_colunas_com_dados(self) -> None:
        """n_cols_com_dados <= 0 retorna 1.0."""
        assert estimar_truncamento(0, 100) == 1.0
        assert estimar_truncamento(-1, 100) == 1.0

    def test_sem_caracteres(self) -> None:
        """total_chars_reconstructed <= 0 retorna 1.0."""
        assert estimar_truncamento(5, 0) == 1.0
        assert estimar_truncamento(5, -5) == 1.0

    def test_sem_perda(self) -> None:
        """Chars >= esperado (n_cols * 10) retorna 0.0."""
        assert estimar_truncamento(5, 50) == 0.0
        assert estimar_truncamento(5, 100) == 0.0

    def test_perda_parcial(self) -> None:
        """Chars < esperado retorna fração entre 0 e 1."""
        # 5 cols * 10 = 50 esperado, 25 obtido → perda = 1 - 25/50 = 0.5
        perda = estimar_truncamento(5, 25)
        assert perda == pytest.approx(0.5, abs=0.01)

    def test_perda_75_percent(self) -> None:
        """25 chars de 100 esperados → perda = 0.75."""
        # 10 cols * 10 = 100, 25 obtido → 1 - 0.25 = 0.75
        perda = estimar_truncamento(10, 25)
        assert perda == pytest.approx(0.75, abs=0.01)

    def test_perda_total(self) -> None:
        """Chars = 0 com colunas > 0 retorna 1.0."""
        assert estimar_truncamento(5, 0) == 1.0

    def test_perda_nunca_ultrapassa_1(self) -> None:
        """Perda nunca ultrapassa 1.0."""
        perda = estimar_truncamento(1, 0)
        assert perda == 1.0
        perda2 = estimar_truncamento(1, -10)
        assert perda2 == 1.0

    def test_perda_nunca_negativa(self) -> None:
        """Se chars >> esperado, retorna 0.0 (não valor negativo)."""
        perda = estimar_truncamento(1, 1000)
        assert perda == 0.0


# ══════════════════════════════════════════════════════════════════════════
# detectar_padrao_a
# ══════════════════════════════════════════════════════════════════════════


@pytest.fixture
def mock_engine_padrao_a() -> MagicMock:
    """Engine mock com suporte a context manager."""
    engine = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    engine.connect.return_value = conn
    return engine


def _configurar_candidatas(engine: MagicMock, tabelas: list[tuple[str, int]]) -> None:
    """Configura resultado da consulta de contagem de colunas."""
    conn = engine.connect.return_value.__enter__.return_value
    result = MagicMock()
    result.__iter__.return_value = [(t, n) for t, n in tabelas]
    conn.execute.return_value = result


class TestDetectarPadraoA:
    """Testes para detectar_padrao_a()."""

    def test_sem_candidatas(self, mock_engine_padrao_a: MagicMock) -> None:
        """Nenhuma tabela com >80 colunas retorna lista vazia."""
        _configurar_candidatas(mock_engine_padrao_a, [])
        resultado = detectar_padrao_a(mock_engine_padrao_a)
        assert resultado == []

    @patch("classificacao.reconstrucao_colunas._calcular_pct_preenchimento")
    def test_candidata_preenchimento_baixo(
        self,
        mock_pct: MagicMock,
        mock_engine_padrao_a: MagicMock,
    ) -> None:
        """Tabela com >80 colunas e <25% preenchimento é detectada."""
        _configurar_candidatas(mock_engine_padrao_a, [("tab_a", 85)])
        mock_pct.return_value = 0.10  # 10% < 25%
        resultado = detectar_padrao_a(mock_engine_padrao_a)
        assert resultado == ["tab_a"]

    @patch("classificacao.reconstrucao_colunas._calcular_pct_preenchimento")
    def test_candidata_preenchimento_alto_ignorada(
        self,
        mock_pct: MagicMock,
        mock_engine_padrao_a: MagicMock,
    ) -> None:
        """Tabela com >80 colunas mas >=25% preenchimento NÃO é detectada."""
        _configurar_candidatas(mock_engine_padrao_a, [("tab_b", 90)])
        mock_pct.return_value = 0.50  # 50% >= 25%
        resultado = detectar_padrao_a(mock_engine_padrao_a)
        assert resultado == []

    @patch("classificacao.reconstrucao_colunas._calcular_pct_preenchimento")
    def test_pct_none_ignora_tabela(
        self,
        mock_pct: MagicMock,
        mock_engine_padrao_a: MagicMock,
    ) -> None:
        """Se _calcular_pct_preenchimento retorna None, tabela é ignorada."""
        _configurar_candidatas(mock_engine_padrao_a, [("tab_c", 82)])
        mock_pct.return_value = None
        resultado = detectar_padrao_a(mock_engine_padrao_a)
        assert resultado == []

    @patch("classificacao.reconstrucao_colunas._calcular_pct_preenchimento")
    def test_multiplas_candidatas(
        self,
        mock_pct: MagicMock,
        mock_engine_padrao_a: MagicMock,
    ) -> None:
        """Múltiplas tabelas com baixo preenchimento são detectadas."""
        _configurar_candidatas(
            mock_engine_padrao_a,
            [("tab_x", 85), ("tab_y", 90)],
        )
        mock_pct.side_effect = [0.10, 0.05]
        resultado = detectar_padrao_a(mock_engine_padrao_a)
        assert resultado == ["tab_x", "tab_y"]

    @patch("classificacao.reconstrucao_colunas._calcular_pct_preenchimento")
    def test_mistura_detectadas_e_ignoradas(
        self,
        mock_pct: MagicMock,
        mock_engine_padrao_a: MagicMock,
    ) -> None:
        """Algumas candidatas são detectadas, outras ignoradas."""
        _configurar_candidatas(
            mock_engine_padrao_a,
            [("tab_a", 85), ("tab_b", 90), ("tab_c", 82)],
        )
        mock_pct.side_effect = [0.10, 0.50, 0.20]
        resultado = detectar_padrao_a(mock_engine_padrao_a)
        assert resultado == ["tab_a", "tab_c"]

    @patch("classificacao.reconstrucao_colunas._calcular_pct_preenchimento")
    def test_min_cols_personalizado(
        self,
        mock_pct: MagicMock,
        mock_engine_padrao_a: MagicMock,
    ) -> None:
        """Parâmetro min_cols personalizado filtra tabelas com menos colunas."""
        # Nenhuma tabela tem mais de 60 colunas → sem candidatas
        _configurar_candidatas(mock_engine_padrao_a, [])
        resultado = detectar_padrao_a(mock_engine_padrao_a, min_cols=60)
        assert resultado == []
