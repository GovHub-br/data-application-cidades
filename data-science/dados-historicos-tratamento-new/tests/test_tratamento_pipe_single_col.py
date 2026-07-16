"""Testes do módulo de tratamento de tabelas pipe (coluna única).

Cobre detectar_tabelas_pipe e tratar_tabela_pipe com mocking,
sem necessidade de PostgreSQL.

Testes
------
- detectar_tabelas_pipe: detecção de tabelas com 1 coluna + pipe,
  ignorando tabelas multi-coluna ou sem pipe
- tratar_tabela_pipe: 5 tabelas mapeadas (contratos_pj, proponentes,
  unidades_concluidas, caracterizacoes_entornos, andamento_obras)
- Drop lógico para tab_andamento_obras (índices 4-8 descartados)
- Aviso (WARNING) para tabela não mapeada
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from classificacao.tratamento_pipe_single_col import (
    MAPEAMENTO_PIPE,
    detectar_tabelas_pipe,
    tratar_tabela_pipe,
)

# ── Constantes ──────────────────────────────────────────────────────────

_PREFIXO = "bb_2013_06_junho_pmcmv_18062013_"

# ── Fixtures (synthetic DataFrames) ─────────────────────────────────────


@pytest.fixture
def fixture_contratos_pj() -> pd.DataFrame:
    """Synthetic data for tab_contratos_pj (4 pipes)."""
    return pd.DataFrame(
        {
            "cod_contrato_pjcod_operacaocod_sit_contrato_pjdte_assin_contrat": [
                "389013|403204|0|2012-10-19",
                "224965|2911835|0|2012-01-18",
            ]
        }
    )


@pytest.fixture
def fixture_proponentes() -> pd.DataFrame:
    """Synthetic data for tab_proponentes (4 pipes)."""
    return pd.DataFrame(
        {
            "cod_cnpj_proponentecod_porte_construtorcod_nat_proponentetxt_ra": [
                "07990908000139|0|1|PLANO & PLANO CONSTRUCOES",
                "67010660000124|1|1|RODOBENS NEGOCIOS IMOBILIARIOS",
            ]
        }
    )


@pytest.fixture
def fixture_unidades() -> pd.DataFrame:
    """Synthetic data for tab_unidades_concluidas (7 pipes).
    Pipe positions:
      0: cod_operacao (int)
      1: dte_legalizacao (date) — vazio na fixture
      2: cod_motivo_ociosidade (int)
      3: dte_primeira_entrega (date)
      4: dte_02 (date)
      5: cod_01 (int)
      6: cod_02 (int)
    """
    return pd.DataFrame(
        {
            "cod_operacaodte_legalizacaocod_motivo_ociosidadedte_primeira_en": [
                "23157||0|2010-07-06|2013-04-29|00176|00085",
                "27175||0|2011-01-13|2013-04-15|00122|00115",
            ]
        }
    )


@pytest.fixture
def fixture_entornos() -> pd.DataFrame:
    """Synthetic data for tab_caracterizacoes_entornos (16 pipes)."""
    return pd.DataFrame(
        {
            "cod_operacaobln_empreendimento_hisbln_rede_abast_aguabln_coleta": [
                "201714|F|T|F|T|F|F|T|T|T|T|T|T|T|T|T",
                "205783|F|T|T|T|T|T|T|F|T|T|T|F|T|T|F",
            ]
        }
    )


@pytest.fixture
def fixture_andamento() -> pd.DataFrame:
    """Synthetic data for tab_andamento_obras (17 pipes).
    Pipe positions:
       0: cod_operacao (int)
       1: cod_sit_obra (int)
       2: cod_regime_construcao (int)
       3: cod_pendencia_obra (int)
       4-8: vazia_04..vazia_08 (dropadas)
       9: dte_01 (date)
      10: dte_02 (date)
      11: dte_03 (date)
      12: vlr_01 (decimal)
      13: vlr_02 (decimal)
      14: vlr_03 (decimal)
      15: dte_04 (date)
      16: dte_05 (date)
    """
    return pd.DataFrame(
        {
            "cod_operacaocod_sit_obracod_regime_construcaocod_pendencia_obra": [
                "23157|0|0|0||||||2013-04-30|2010-12-28|2013-05-30|0.00|100.00|531480.52|2011-05-12|2012-05-30",  # noqa: E501
                "27175|6|0|0||||||2011-09-13|2011-08-03|2011-07-15|0.00|100.00|560888.97|2011-09-13|2012-02-29",  # noqa: E501
            ]
        }
    )


@pytest.fixture
def mock_engine() -> MagicMock:
    """Retorna um MagicMock que simula sqlalchemy.engine.Engine."""
    return MagicMock()


# ══════════════════════════════════════════════════════════════════════════
# detectar_tabelas_pipe
# ══════════════════════════════════════════════════════════════════════════


class TestDetectarTabelasPipe:
    """Testes para detectar_tabelas_pipe()."""

    @staticmethod
    def _configurar_context_manager(
        engine: MagicMock,
    ) -> MagicMock:
        """Configura engine.connect() para retornar um connection mock
        que funciona como context manager."""
        conn = MagicMock()
        conn.__enter__.return_value = conn
        engine.connect.return_value = conn
        return conn

    def test_detecta_tabela_com_pipe(self, mock_engine: MagicMock) -> None:
        """Tabela com 1 coluna e dados com pipe deve ser detectada."""
        conn = self._configurar_context_manager(mock_engine)

        # 1ª consulta: information_schema.columns → retorna 1 tabela
        result_tabelas = MagicMock()
        result_tabelas.__iter__.return_value = [("minha_tabela",)]
        conn.execute.return_value = result_tabelas

        # 2ª consulta: nome da coluna → "col_unica"
        # 3ª consulta: COUNT(*) com pipe → 5 linhas
        conn.scalar.side_effect = ["col_unica", 5]

        with patch(
            "classificacao.tratamento_pipe_single_col.get_schema_source",
            return_value="test_schema",
        ):
            tabelas = detectar_tabelas_pipe(mock_engine)

        assert tabelas == ["minha_tabela"]

    def test_ignora_tabela_multiplas_colunas(self, mock_engine: MagicMock) -> None:
        """Tabela com múltiplas colunas (HAVING COUNT(*) = 1 retorna vazio)
        não deve ser detectada."""
        conn = self._configurar_context_manager(mock_engine)

        # Nenhuma tabela tem exatamente 1 coluna
        result_vazio = MagicMock()
        result_vazio.__iter__.return_value = []
        conn.execute.return_value = result_vazio

        with patch(
            "classificacao.tratamento_pipe_single_col.get_schema_source",
            return_value="test_schema",
        ):
            tabelas = detectar_tabelas_pipe(mock_engine)

        assert tabelas == []

    def test_ignora_tabela_uma_coluna_sem_pipe(self, mock_engine: MagicMock) -> None:
        """Tabela com 1 coluna mas sem pipe nos dados não deve ser detectada."""
        conn = self._configurar_context_manager(mock_engine)

        # 1 tabela com 1 coluna
        result_tabelas = MagicMock()
        result_tabelas.__iter__.return_value = [("tab_sem_pipe",)]
        conn.execute.return_value = result_tabelas

        # col_name = "col_unica", COUNT(*) = 0 (sem pipe)
        conn.scalar.side_effect = ["col_unica", 0]

        with patch(
            "classificacao.tratamento_pipe_single_col.get_schema_source",
            return_value="test_schema",
        ):
            tabelas = detectar_tabelas_pipe(mock_engine)

        assert tabelas == []

    def test_schema_vazio_retorna_lista_vazia(self, mock_engine: MagicMock) -> None:
        """Schema sem tabelas deve retornar lista vazia."""
        conn = self._configurar_context_manager(mock_engine)

        result_vazio = MagicMock()
        result_vazio.__iter__.return_value = []
        conn.execute.return_value = result_vazio

        with patch(
            "classificacao.tratamento_pipe_single_col.get_schema_source",
            return_value="schema_vazio",
        ):
            tabelas = detectar_tabelas_pipe(mock_engine)

        assert tabelas == []

    def test_detecta_multiplas_tabelas_pipe(self, mock_engine: MagicMock) -> None:
        """Múltiplas tabelas com 1 coluna e pipe devem ser detectadas."""
        conn = self._configurar_context_manager(mock_engine)

        result_tabelas = MagicMock()
        result_tabelas.__iter__.return_value = [
            ("tab_a",),
            ("tab_b",),
        ]
        conn.execute.return_value = result_tabelas

        # Para tab_a: col="col_a", count=3
        # Para tab_b: col="col_b", count=7
        conn.scalar.side_effect = ["col_a", 3, "col_b", 7]

        with patch(
            "classificacao.tratamento_pipe_single_col.get_schema_source",
            return_value="test_schema",
        ):
            tabelas = detectar_tabelas_pipe(mock_engine)

        assert tabelas == ["tab_a", "tab_b"]

    def test_coluna_none_ignora_tabela(self, mock_engine: MagicMock) -> None:
        """Se a consulta do nome da coluna retorna None, a tabela é ignorada."""
        conn = self._configurar_context_manager(mock_engine)

        result_tabelas = MagicMock()
        result_tabelas.__iter__.return_value = [("tab_sem_coluna",)]
        conn.execute.return_value = result_tabelas

        # col_name retorna None
        conn.scalar.side_effect = [None]

        with patch(
            "classificacao.tratamento_pipe_single_col.get_schema_source",
            return_value="test_schema",
        ):
            tabelas = detectar_tabelas_pipe(mock_engine)

        assert tabelas == []


# ══════════════════════════════════════════════════════════════════════════
# tratar_tabela_pipe — 5 tabelas mapeadas
# ══════════════════════════════════════════════════════════════════════════


class TestTratarTabelaPipe:
    """Testes para tratar_tabela_pipe() com mocking de ler_tabela."""

    # ── tab_contratos_pj ────────────────────────────────────────────────

    @patch("classificacao.tratamento_pipe_single_col.ler_tabela")
    def test_contratos_pj_colunas_e_tipos(
        self,
        mock_ler_tabela: MagicMock,
        fixture_contratos_pj: pd.DataFrame,
    ) -> None:
        """tab_contratos_pj: 4 colunas, tipos int/int/int/date."""
        table_name = f"{_PREFIXO}tab_contratos_pj"
        mock_ler_tabela.return_value = fixture_contratos_pj

        df = tratar_tabela_pipe(MagicMock(), table_name)

        assert df is not None
        assert df.shape[1] == 4
        assert list(df.columns) == [
            "cod_contrato_pj",
            "cod_operacao",
            "cod_sit_contrato_pj",
            "dte_assin_contrato",
        ]

        # Tipos
        assert pd.api.types.is_numeric_dtype(df["cod_contrato_pj"])
        assert pd.api.types.is_numeric_dtype(df["cod_operacao"])
        assert pd.api.types.is_numeric_dtype(df["cod_sit_contrato_pj"])
        assert pd.api.types.is_datetime64_any_dtype(df["dte_assin_contrato"])

        # Valores
        assert df["cod_contrato_pj"].iloc[0] == 389013
        assert df["cod_operacao"].iloc[0] == 403204
        assert df["cod_sit_contrato_pj"].iloc[0] == 0
        assert df["dte_assin_contrato"].iloc[0] == pd.Timestamp("2012-10-19")

    # ── tab_proponentes ─────────────────────────────────────────────────

    @patch("classificacao.tratamento_pipe_single_col.ler_tabela")
    def test_proponentes_colunas_e_tipos(
        self,
        mock_ler_tabela: MagicMock,
        fixture_proponentes: pd.DataFrame,
    ) -> None:
        """tab_proponentes: 4 colunas, tipos text/int/int/text."""
        table_name = f"{_PREFIXO}tab_proponentes"
        mock_ler_tabela.return_value = fixture_proponentes

        df = tratar_tabela_pipe(MagicMock(), table_name)

        assert df is not None
        assert df.shape[1] == 4
        assert list(df.columns) == [
            "cod_cnpj_proponente",
            "cod_porte_construtor",
            "cod_nat_proponente",
            "txt_razao_social",
        ]

        # Valores
        assert df["cod_cnpj_proponente"].iloc[0] == "07990908000139"
        assert df["cod_porte_construtor"].iloc[0] == 0
        assert df["cod_nat_proponente"].iloc[0] == 1
        assert df["txt_razao_social"].iloc[0] == "PLANO & PLANO CONSTRUCOES"

    # ── tab_unidades_concluidas ─────────────────────────────────────────

    @patch("classificacao.tratamento_pipe_single_col.ler_tabela")
    def test_unidades_concluidas_colunas_e_tipos(
        self,
        mock_ler_tabela: MagicMock,
        fixture_unidades: pd.DataFrame,
    ) -> None:
        """tab_unidades_concluidas: 7 colunas, com campos vazios no pipe."""
        table_name = f"{_PREFIXO}tab_unidades_concluidas"
        mock_ler_tabela.return_value = fixture_unidades

        df = tratar_tabela_pipe(MagicMock(), table_name)

        assert df is not None
        assert df.shape[1] == 7
        assert list(df.columns) == [
            "cod_operacao",
            "dte_legalizacao",
            "cod_motivo_ociosidade",
            "dte_primeira_entrega",
            "dte_02",
            "cod_01",
            "cod_02",
        ]

        # Tipos
        assert pd.api.types.is_numeric_dtype(df["cod_operacao"])
        assert pd.api.types.is_datetime64_any_dtype(df["dte_legalizacao"])
        assert pd.api.types.is_numeric_dtype(df["cod_motivo_ociosidade"])
        assert pd.api.types.is_datetime64_any_dtype(df["dte_primeira_entrega"])
        assert pd.api.types.is_datetime64_any_dtype(df["dte_02"])
        assert pd.api.types.is_numeric_dtype(df["cod_01"])
        assert pd.api.types.is_numeric_dtype(df["cod_02"])

        # Campo vazio no pipe → NaT para date (dte_legalizacao)
        # cod_motivo_ociosidade = "0" → 0 (não vazio, ok)
        assert pd.isna(df["dte_legalizacao"].iloc[0])
        assert df["cod_motivo_ociosidade"].iloc[0] == 0

        # Valores preenchidos
        assert df["cod_operacao"].iloc[0] == 23157
        assert df["dte_primeira_entrega"].iloc[0] == pd.Timestamp("2010-07-06")

    # ── tab_caracterizacoes_entornos ────────────────────────────────────

    @patch("classificacao.tratamento_pipe_single_col.ler_tabela")
    def test_caracterizacoes_entornos_colunas_e_tipos(
        self,
        mock_ler_tabela: MagicMock,
        fixture_entornos: pd.DataFrame,
    ) -> None:
        """tab_caracterizacoes_entornos: 16 colunas, 1 int + 15 bool."""
        table_name = f"{_PREFIXO}tab_caracterizacoes_entornos"
        mock_ler_tabela.return_value = fixture_entornos

        df = tratar_tabela_pipe(MagicMock(), table_name)

        assert df is not None
        assert df.shape[1] == 16
        expected_cols = [
            "cod_operacao",
            "bln_empreendimento_his",
            "bln_rede_abast_agua",
            "bln_coleta_esgoto",
            "bln_01",
            "bln_02",
            "bln_03",
            "bln_04",
            "bln_05",
            "bln_06",
            "bln_07",
            "bln_08",
            "bln_09",
            "bln_10",
            "bln_11",
            "bln_12",
        ]
        assert list(df.columns) == expected_cols

        # Tipo int para cod_operacao
        assert pd.api.types.is_numeric_dtype(df["cod_operacao"])

        # Valores booleanos (T → True, F → False)
        assert df["bln_empreendimento_his"].iloc[0] is False
        assert df["bln_rede_abast_agua"].iloc[0] is True
        assert df["bln_coleta_esgoto"].iloc[0] is False
        assert df["bln_01"].iloc[0] is True

        # Segunda linha (posições 0-based): bln_02=T, bln_coleta_esgoto=T, bln_09=F
        assert df["bln_02"].iloc[1] is True
        assert df["bln_coleta_esgoto"].iloc[1] is True
        assert df["bln_09"].iloc[1] is False

    # ── tab_andamento_obras (com drop de colunas) ───────────────────────

    @patch("classificacao.tratamento_pipe_single_col.ler_tabela")
    def test_andamento_obras_colunas_e_tipos(
        self,
        mock_ler_tabela: MagicMock,
        fixture_andamento: pd.DataFrame,
    ) -> None:
        """tab_andamento_obras: 12 colunas (após drop de 5 vazias)."""
        table_name = f"{_PREFIXO}tab_andamento_obras"
        mock_ler_tabela.return_value = fixture_andamento

        df = tratar_tabela_pipe(MagicMock(), table_name)

        assert df is not None
        # 17 colunas originais - 5 dropadas = 12
        assert df.shape[1] == 12
        expected_cols = [
            "cod_operacao",
            "cod_sit_obra",
            "cod_regime_construcao",
            "cod_pendencia_obra",
            "dte_01",
            "dte_02",
            "dte_03",
            "vlr_01",
            "vlr_02",
            "vlr_03",
            "dte_04",
            "dte_05",
        ]
        assert list(df.columns) == expected_cols

        # Tipos
        assert pd.api.types.is_numeric_dtype(df["cod_operacao"])
        assert pd.api.types.is_numeric_dtype(df["cod_sit_obra"])
        assert pd.api.types.is_datetime64_any_dtype(df["dte_01"])
        assert pd.api.types.is_datetime64_any_dtype(df["dte_02"])
        assert pd.api.types.is_datetime64_any_dtype(df["dte_03"])
        assert pd.api.types.is_numeric_dtype(df["vlr_01"])
        assert pd.api.types.is_numeric_dtype(df["vlr_02"])
        assert pd.api.types.is_numeric_dtype(df["vlr_03"])
        assert pd.api.types.is_datetime64_any_dtype(df["dte_04"])
        assert pd.api.types.is_datetime64_any_dtype(df["dte_05"])

        # Valores (primeira linha)
        assert df["cod_operacao"].iloc[0] == 23157
        assert df["dte_01"].iloc[0] == pd.Timestamp("2013-04-30")
        assert df["dte_03"].iloc[0] == pd.Timestamp("2013-05-30")
        assert df["vlr_01"].iloc[0] == 0.0
        assert df["vlr_03"].iloc[0] == 531480.52

        # Valores (segunda linha)
        assert df["cod_operacao"].iloc[1] == 27175
        assert df["dte_05"].iloc[1] == pd.Timestamp("2012-02-29")

    # ── Drop logic para tab_andamento_obras ─────────────────────────────

    @patch("classificacao.tratamento_pipe_single_col.ler_tabela")
    def test_andamento_obras_drop_indices(
        self,
        mock_ler_tabela: MagicMock,
        fixture_andamento: pd.DataFrame,
    ) -> None:
        """Verifica que os índices 4-8 foram dropados em tab_andamento_obras
        e as colunas correspondentes NÃO estão presentes."""
        table_name = f"{_PREFIXO}tab_andamento_obras"
        mock_ler_tabela.return_value = fixture_andamento

        df = tratar_tabela_pipe(MagicMock(), table_name)

        assert df is not None
        assert df.shape[1] == 12

        mapping = MAPEAMENTO_PIPE[table_name]
        kept_indices = [
            i for i in range(mapping["n_cols"]) if i not in mapping["drop_cols"]
        ]
        assert len(kept_indices) == 12

        # Garante que os índices 4,5,6,7,8 foram dropados
        assert 4 not in kept_indices
        assert 5 not in kept_indices
        assert 6 not in kept_indices
        assert 7 not in kept_indices
        assert 8 not in kept_indices

        # Nenhuma coluna "vazia_*" deve existir no resultado
        for dropped_col in ["vazia_04", "vazia_05", "vazia_06", "vazia_07", "vazia_08"]:
            assert dropped_col not in df.columns

    # ── Tabela não mapeada → WARNING + None ─────────────────────────────

    @patch("classificacao.tratamento_pipe_single_col.ler_tabela")
    def test_tabela_nao_mapeada_retorna_none(
        self,
        mock_ler_tabela: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Tabela pipe NÃO presente em MAPEAMENTO_PIPE deve retornar None
        e emitir um log WARNING."""
        # Mock não será chamado (a verificação do mapeamento é anterior),
        # mas definimos para segurança.
        mock_ler_tabela.return_value = pd.DataFrame({"col_unica": ["a|b", "c|d"]})

        with caplog.at_level(logging.WARNING):
            result = tratar_tabela_pipe(MagicMock(), "tabela_sem_mapeamento")

        assert result is None
        assert "não possui mapeamento" in caplog.text

    # ── Engine real (não usado) ─────────────────────────────────────────

    @patch("classificacao.tratamento_pipe_single_col.ler_tabela")
    def test_engine_mock_passado_para_ler_tabela(
        self,
        mock_ler_tabela: MagicMock,
        fixture_contratos_pj: pd.DataFrame,
    ) -> None:
        """Verifica que o engine passado é repassado para ler_tabela."""
        table_name = f"{_PREFIXO}tab_contratos_pj"
        mock_ler_tabela.return_value = fixture_contratos_pj

        engine = MagicMock()
        tratar_tabela_pipe(engine, table_name)

        mock_ler_tabela.assert_called_once_with(engine, table_name)
