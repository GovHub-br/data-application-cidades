"""Factory de parsers por formato — usado por BaseFileParser._para_dataframe.

Cada parser sabe só ler um formato (csv/txt/xlsx) pra DataFrame de texto
(`dtype=str`, ver docstring de base_file_parser.py pro motivo). Não tem
conhecimento de dataset nem faz nenhuma tipagem/transformação — isso é sempre
responsabilidade do dbt.

JSON não entra aqui: já tem hook próprio em BaseFileParser
(`_json_para_dataframe`) porque ler JSON aninhado (ex.: IBGE) não é uma
operação genérica igual csv/txt/xlsx.
"""

import io
from typing import Any, Protocol, cast

import pandas as pd

from domain.exceptions import UnsupportedFileFormatError


class Parser(Protocol):
    def read(self, raw: bytes, **kwargs: Any) -> pd.DataFrame: ...


class CsvParser:
    def read(self, raw: bytes, **kwargs: Any) -> pd.DataFrame:
        return cast(pd.DataFrame, pd.read_csv(io.BytesIO(raw), dtype=str, **kwargs))


class TxtParser:
    def read(self, raw: bytes, sep: str = "\t", **kwargs: Any) -> pd.DataFrame:
        return cast(
            pd.DataFrame, pd.read_csv(io.BytesIO(raw), sep=sep, dtype=str, **kwargs)
        )


class ExcelParser:
    def read(self, raw: bytes, **kwargs: Any) -> pd.DataFrame:
        return cast(pd.DataFrame, pd.read_excel(io.BytesIO(raw), dtype=str, **kwargs))


class ParserFactory:
    """Escolhe o parser certo pro `formato` — o resto do sistema não precisa
    saber que `if formato == "csv": ... elif formato == "xlsx": ...` existe."""

    _parsers: dict[str, type[Parser]] = {
        "csv": CsvParser,
        "txt": TxtParser,
        "xlsx": ExcelParser,
    }

    @classmethod
    def create(cls, formato: str) -> Parser:
        parser_cls = cls._parsers.get(formato)
        if parser_cls is None:
            raise UnsupportedFileFormatError(f"Formato não suportado: {formato}")
        return parser_cls()
