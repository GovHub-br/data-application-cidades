from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "helpers"))

from openmetadata.semantic_relationships import _split_column_reference

FORMATO_ESPERADO = "schema.tabela.coluna"


# FASE 1 - Teste Caixa Preta (Especificação)

@pytest.mark.parametrize(
    ("referencia", "esperado"),
    [
        pytest.param(
            "public.municipios.cod_ibge",
            ("public", "municipios", "cod_ibge"),
            id = "CT-01-RF-01-V1-tres-partes",
        ),
        pytest.param(
            "a.b.c",
            ("a", "b", "c"),
            id = "CT-02-RF-01-limite-um-caractere-por-parte",
        ),
        pytest.param(
            "Raw_Data.Tabela_2.Coluna",
            ("Raw_Data", "Tabela_2", "Coluna"),
            id = "CT-03-RF-01-preserva-maiusculas-e-undescore",
        ),
        pytest.param(
            " a . b . c ",
            ("a", "b", "c"),
            id = "CT-04-RF-02-V2-espacos-nas-bordas",    
        ),
        pytest.param(
            "a.\tb\n.c",
            ("a", "b", "c"),
            id = "CT-05-RF-02-tab-e-quebra-de-linha",
        ),
    ],
)
def test_split_column_reference_tres_partes_devolve_schema_tabela_coluna(
    referencia: str, esperado: tuple[str, str, str]    
) -> None:
    """ CT-01 a CT-05: referência válida vai devolver as três partes limpas """
    # Act
    resultado = _split_column_reference(referencia)

    # Assert
    assert resultado == esperado

def test_split_column_reference_tres_partes_devolve_tupla_de_tres_textos() -> None:
    """ CT-06: o tipo de retorno é tupla de três `str` """
    # Act
    resultado = _split_column_reference("a.b.c")

    # Assert
    assert isinstance(resultado, tuple)
    assert len(resultado) == 3
    assert all(isinstance(parte, str) for parte in resultado)

@pytest.mark.parametrize(
    "referencia",
    [
        pytest.param("", id = "CT-07-RF-03-I1-texto-vazio"),
        pytest.param("coluna", id = "CT-08-RF-03-I1-limite-1-parte"),
        pytest.param("tabela.coluna", id = "CT-09-RF-03-I1-limite-2-partes"),
        pytest.param("a.b.c.d", id = "CT-10-RF-03-I2-limite-4-partes"),
        pytest.param("a.b.c.d.e", id = "CT-11-RF-03-I2-limite-5-partes"),
        pytest.param("a.b.c.", id = "CT-12-RF-03-ponto-final-sobrando"),
    ],
)
def test_split_column_reference_partes_diferentes_de_tres_levanta_valueerror(
    referencia: str,
) -> None:
    """CT-07 a CT-12: só exatamente 3 partes são aceitas"""
    # Act / Assert
    with pytest.raises(ValueError):
        _split_column_reference(referencia)

@pytest.mark.parametrize(
    "referencia",
    [
        pytest.param(".b.c", id = "CT-13-RF-04-I3-primeira-parte-vazia"),
        pytest.param("a..c", id = "CT-14-RF-04-I3-parte-do-meio-vazia"),
        pytest.param("a.b.", id = "CT-15-RF-04-I3-ultima-vazia"),
        pytest.param("..", id = "CT-16-RF-04-I3-todas-vazias"),
        pytest.param("a. .c", id = "CT-17-RF-04-I4-limite-um-espaco"),
        pytest.param("a.\t.c", id = "CT-18-RF-04-I4-so-tab"),
        pytest.param(" . . ", id = "CT-19-RF-04-I4-todas-so-espacos"),
    ],
)
def test_split_column_reference_parte_vazia_ou_em_branco_levanta_valueerror(
    referencia: str,
) -> None:
    """CT-13 a CT-19: parte vazia ou só com espaços inválida a referência"""
    # Act / Assert
    with pytest.raises(ValueError):
        _split_column_reference(referencia)

@pytest.mark.parametrize(
    "referencia",
    [
        pytest.param("a.b", id = "CT-20-RF-05-poucas-partes"),
        pytest.param("a..c", id = "CT-21-RF-05-parte-vazia"),
        pytest.param("a.b.c.d", id = "CT-22-RF-05-partes-demais"),
    ],
)
def test_split_column_reference_erro_informa_formato_e_valor_recebido(
    referencia: str,
) -> None:
    """CT-20 a CT-22: a mensagem orienta quem chamou a função"""
    # Act
    with pytest.raises(ValueError) as erro:
        _split_column_reference(referencia)

    # Assert
    mensagem = str(erro.value)
    assert FORMATO_ESPERADO in mensagem
    assert repr(referencia) in mensagem

# FASE 2 - CAIXA BRANCA (ESTRUTURA DO CÓDIGO): MC/DC e branches

def test_split_column_reference_mcdc_a_falso_b_falso_segue_para_o_retornor() -> None:
    """CT-23 (MC/DC T1): D falso, o `if` não dispara e a função retorna"""
    # Act
    resultado = _split_column_reference("a.b.c")

    # Assert
    assert resultado == ("a", "b", "c")


def test_split_column_reference_mcdc_a_verdadeiro_levanta_valueerror() -> None:
    """CT-24 (MC/DC T2): só A muda em relação a CT-23 e D passa verdadeiro"""
    # Act / Assert
    with pytest.raises(ValueError):
        _split_column_reference("a.b")

def test_split_column_reference_mcdc_b_verdadeiro_levanta_valueerror() -> None:
    """CR-25 (MC/DC T3): só B muda em relação a CT-23 e D passa a verdadeiro"""
    # Act / Assert
    with pytest.raises(ValueError):
        _split_column_reference("a..c")

def test_split_column_reference_a_decide_sozinho_quando_b_seria_verdadeiro() -> None:
    """CT-26 (curto-circuito): "." tem 2 partes vazias; o `or` para em A"""
    # Act / Assert
    with pytest.raises(ValueError):
        _split_column_reference(".")

@pytest.mark.parametrize(
    "referencia",
    [
        pytest.param(None, id = "CT-27-STR-none-vira-texto-de-1-parte"),
        pytest.param(123, id = "CT-28-STR-inteiro-vira-texto-de-1-parte"),
        pytest.param(1.5, id = "CT-29-STR-float-vira-texto-de-2-parte"),
    ],
)
def test_split_column_reference_entrada_que_nao_e_texto_levanta_valueerror(
    referencia: Any,
) -> None:
    """CT-27 a CT-29: o `str(reference)` converte antes de dividir"""
    # Act / Assert
    with pytest.raises(ValueError):
        _split_column_reference(referencia)