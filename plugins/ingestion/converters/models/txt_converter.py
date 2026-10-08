"""Texto delimitado sem delimitador padrão (.txt, .tsv)."""

from pathlib import Path

from ingestion.converters.converter_registry import ConverterFactory
from ingestion.converters.models.csv_converter import CsvConverter


@ConverterFactory.register("txt", extensions=(".txt", ".tsv"))
class TxtConverter(CsvConverter):
    """Igual ao csv, mas sem delimitador padrão: em .txt ele varia (|, ;, tab).

    `.tsv` assume tabulação.
    """

    default_delimiter = None

    def _delimiter(self, path: Path) -> str:
        if self.config.delimiter is None and path.suffix.lower() == ".tsv":
            return "\t"
        return super()._delimiter(path)
