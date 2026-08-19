import pytest

from domain.exceptions import UnsupportedFileFormatError
from parser_factory import CsvParser, ExcelParser, ParserFactory, TxtParser


@pytest.mark.parametrize(
    "formato,esperado",
    [
        ("csv", CsvParser),
        ("txt", TxtParser),
        ("xlsx", ExcelParser),
    ],
)
def test_create_retorna_parser_certo(formato: str, esperado: type) -> None:
    assert isinstance(ParserFactory.create(formato), esperado)


def test_create_formato_desconhecido_levanta_erro() -> None:
    with pytest.raises(UnsupportedFileFormatError):
        ParserFactory.create("txt_desconhecido")


def test_csv_parser_le_e_forca_texto() -> None:
    raw = b"id,valor\n1,007\n2,1891.63\n"
    df = CsvParser().read(raw)
    assert df["valor"].tolist() == ["007", "1891.63"]
    assert df["id"].tolist() == ["1", "2"]


def test_txt_parser_usa_tab_por_padrao() -> None:
    raw = "id\tvalor\n1\t007\n".encode("utf-8")
    df = TxtParser().read(raw)
    assert df["valor"].tolist() == ["007"]
