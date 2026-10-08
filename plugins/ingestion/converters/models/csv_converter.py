"""Texto delimitado (csv, txt, tsv), lido em blocos pelo pyarrow."""

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv

from ingestion.converters.base_converter import FileConverter, Source
from ingestion.converters.converter_errors import ConversionError
from ingestion.converters.converter_registry import ConverterFactory

BLOCK_BYTES = 1 << 20


@ConverterFactory.register("csv", extensions=(".csv",))
class CsvConverter(FileConverter):
    """Lê em blocos de 1 MiB, sem inferir tipo: tudo é string.

    O cabeçalho é lido como dado (nomes gerados f0..fN, tudo string), para que
    nomes vazios e repetidos cheguem intactos ao `fix_header` e nenhuma coluna seja
    tipada pela inferência do primeiro bloco. Nulo e string vazia não se confundem:
    `,,` é nulo e `,"",` é vazio. Quebra de linha dentro de aspas fica no valor.
    """

    default_delimiter: str | None = ","

    def _read(self, path: Path) -> Iterator[Source]:
        delimiter = self._delimiter(path)
        width = self._width(path, delimiter)
        reader = self._open(path, delimiter, width)
        try:
            first = reader.read_next_batch()
        except StopIteration:
            raise ConversionError(f"{path.name}: arquivo vazio") from None
        header = first.slice(0, 1).to_pylist()[0]
        names = [header[f"f{i}"] for i in range(width)]
        yield Source(
            suffix=None, header=names, batches=self._rest(first.slice(1), reader)
        )

    def _delimiter(self, path: Path) -> str:
        delimiter = self.config.delimiter or self.default_delimiter
        if delimiter is None:
            raise ConversionError(
                f"{path.name}: informe o delimitador (ConverterConfig.delimiter)"
            )
        return delimiter

    def _width(self, path: Path, delimiter: str) -> int:
        try:
            probe = self._open(path, delimiter, width=None)
        except pa.ArrowInvalid as exc:
            if "Empty CSV file" in str(exc):
                raise ConversionError(f"{path.name}: arquivo vazio") from exc
            raise ConversionError(f"{path.name}: {exc}") from exc
        return len(probe.schema)

    def _open(
        self, path: Path, delimiter: str, width: int | None
    ) -> pacsv.CSVStreamingReader:
        types = {f"f{i}": pa.string() for i in range(width)} if width else None
        return pacsv.open_csv(
            path,
            read_options=pacsv.ReadOptions(
                encoding=self.config.encoding,
                skip_rows=self.config.skip_rows,
                autogenerate_column_names=True,
                block_size=BLOCK_BYTES,
            ),
            parse_options=pacsv.ParseOptions(
                delimiter=delimiter, newlines_in_values=True
            ),
            convert_options=pacsv.ConvertOptions(
                column_types=types,
                strings_can_be_null=True,
                quoted_strings_can_be_null=False,
            ),
            memory_pool=self.memory_pool,
        )

    @staticmethod
    def _rest(
        first: pa.RecordBatch, reader: pacsv.CSVStreamingReader
    ) -> Iterator[pa.RecordBatch]:
        if first.num_rows:
            yield first
        while True:
            try:
                yield reader.read_next_batch()
            except StopIteration:
                return
            except pa.ArrowInvalid as exc:
                raise ConversionError(str(exc)) from exc


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
