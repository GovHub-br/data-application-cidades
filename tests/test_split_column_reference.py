import pytest

from openmetadata.semantic_relationships import _split_column_reference


@pytest.mark.parametrize(
    ("referencia", "esperado"),
    [
        ("public.municipios.cod_ibge", ("public", "municipios", "cod_ibge")),
        ("Raw_Data.Tabela_2.Coluna", ("Raw_Data", "Tabela_2", "Coluna")),
        (" a . b . c ", ("a", "b", "c")),
        ("a.\tb\n.c", ("a", "b", "c")),
    ],
)
def test_split_column_reference_devolve_partes_sem_espacos(referencia, esperado) -> None:
    assert _split_column_reference(referencia) == esperado


@pytest.mark.parametrize(
    "referencia",
    ["", "coluna", "a.b", "a.b.c.d", "a.b.c.", ".b.c", "a..c", "..", "a. .c", " . . "],
)
def test_split_column_reference_formato_invalido_levanta_valueerror(referencia) -> None:
    with pytest.raises(ValueError, match=r"schema\.tabela\.coluna"):
        _split_column_reference(referencia)


@pytest.mark.parametrize("referencia", [None, 123, 1.5])
def test_split_column_reference_nao_texto_levanta_valueerror(referencia) -> None:
    # o `str(reference)` vira "None", "123" ou "1.5", que não têm 3 partes
    with pytest.raises(ValueError):
        _split_column_reference(referencia)
