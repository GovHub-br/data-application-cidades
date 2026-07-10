"""Tests for data_de_movimento preservation and report_date extraction."""
import tempfile
from pathlib import Path
import pandas as pd
from classificacao.carregamento import _detectar_data_movimento_col0, carregar_csv
from classificacao.tratamento import extrair_periodo_filename


def write_temp_csv(content: str) -> Path:
    """Helper to write a temp CSV file."""
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='')
    tmp.write(content)
    tmp.close()
    return Path(tmp.name)


class TestDetectarDataMovimento:
    """Tests for _detectar_data_movimento_col0."""

    def test_col0_with_br_dates_returns_true(self):
        content = "data\tcol_a\n31/10/2024\tfoo\n31/10/2024\tbar\n31/10/2024\tbaz\n"
        path = write_temp_csv(content)
        assert _detectar_data_movimento_col0(path) is True

    def test_col0_with_iso_dates_returns_true(self):
        content = "data\tcol_a\n2024-10-31\tfoo\n2024-10-31\tbar\n2024-10-31\tbaz\n"
        path = write_temp_csv(content)
        assert _detectar_data_movimento_col0(path) is True

    def test_col0_with_sequential_integers_returns_false(self):
        content = "idx\tcol_a\n0\tfoo\n1\tbar\n2\tbaz\n3\tqux\n"
        path = write_temp_csv(content)
        assert _detectar_data_movimento_col0(path) is False

    def test_col0_with_named_header_returns_false(self):
        content = "cod_contrato\tcol_a\n123\tfoo\n456\tbar\n"
        path = write_temp_csv(content)
        # Should be False because values are not dates
        assert _detectar_data_movimento_col0(path) is False


class TestCarregarCsvWithDM:
    """Tests for carregar_csv with data_de_movimento in col0."""

    def test_preserves_data_movimento_as_column(self):
        content = "data_de_movimento\tagente_financeiro\tapf\n31/10/2024\tBB\t123\n31/10/2024\tCAIXA\t456\n"
        path = write_temp_csv(content)
        df = carregar_csv(path)
        # data_de_movimento should be preserved as a column
        assert "data_de_movimento" in df.columns
        assert df["data_de_movimento"].iloc[0] == "2024-10-31"

    def test_normal_row_index_still_works(self):
        content = "0\tcol_a\tcol_b\n0\tfoo\t10\n1\tbar\t20\n"
        path = write_temp_csv(content)
        df = carregar_csv(path)
        # Row index column should be consumed, not in data columns
        assert "col_a" in df.columns
        assert len(df.columns) == 2  # col_a, col_b
        assert "0" not in df.columns


class TestExtrairPeriodoNovosPadroes:
    """Tests for new report_date extraction patterns."""

    def test_historico_recente_prefix(self):
        result = extrair_periodo_filename(
            "historico_recente_202406_snh_pmcmv_dados_prioritarios_af_caixa"
        )
        assert result == "2024-06-01"

    def test_o_recente_prefix(self):
        result = extrair_periodo_filename(
            "o_recente_202406_snh_pmcmv_dados_prioritarios_af_caixa_entregas"
        )
        assert result == "2024-06-01"

    def test_ecente_truncated_prefix(self):
        result = extrair_periodo_filename(
            "ecente_2024_07_snh_pmcmv_dados_prioritarios_af_bb_vs02_correcao"
        )
        assert result == "2024-07-01"

    def test_storico_truncated_prefix(self):
        result = extrair_periodo_filename(
            "storico_recente_2024_07_snh_pmcmv_dados_prioritarios_af_bb_vs02"
        )
        assert result == "2024-07-01"

    def test_existing_yyyymm_prefix_still_works(self):
        result = extrair_periodo_filename("202402_snh_pmcmv_dados_prioritarios")
        assert result == "2024-02-01"
