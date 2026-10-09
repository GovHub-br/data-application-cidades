"""CSV, lido em blocos pelo pyarrow; base do texto delimitado (o txt herda)."""

import codecs
import io
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import pyarrow as pa
import pyarrow.csv as pacsv

from ingestion.converters.base_converter import FileConverter, Source
from ingestion.converters.converter_errors import ConversionError
from ingestion.converters.converter_registry import ConverterFactory
from ingestion.text import detectar_dialeto, detectar_encoding

BLOCK_BYTES = 1 << 20
SAMPLE_BYTES = 64 * 1024
AUTO = "auto"

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Dialect:
    """Como abrir o arquivo: `transcode` decodifica em Python (com `replace`)."""

    encoding: str
    delimiter: str
    transcode: bool = False


@ConverterFactory.register("csv", extensions=(".csv",))
class CsvConverter(FileConverter):
    """Lê em blocos de 1 MiB, sem inferir tipo: tudo é string.

    O cabeçalho é lido como dado (nomes gerados f0..fN, tudo string), para que
    nomes vazios e repetidos cheguem intactos ao `fix_header` e nenhuma coluna seja
    tipada pela inferência do primeiro bloco. Nulo e string vazia não se confundem:
    `,,` é nulo e `,"",` é vazio. Quebra de linha dentro de aspas fica no valor.

    `encoding="auto"` e `delimiter="auto"` detectam o dialeto numa amostra de 64 KB
    (depois de `skip_rows`), com as regras de `ingestion.text`. No encoding
    detectado, o arquivo é decodificado com `errors="replace"`: um byte inválido
    que a amostra não viu vira U+FFFD (e um aviso no log) em vez de derrubar a
    conversão.
    """

    default_delimiter: str | None = ","

    def _read(self, path: Path) -> Iterator[Source]:
        dialect = self._dialect(path)
        width = self._width(path, dialect)
        reader, stream = self._open(path, dialect, width)
        try:
            first = reader.read_next_batch()
        except StopIteration:
            stream.close()
            raise ConversionError(f"{path.name}: arquivo vazio") from None
        header = first.slice(0, 1).to_pylist()[0]
        names = [header[f"f{i}"] for i in range(width)]
        yield Source(
            suffix=None,
            header=names,
            batches=self._rest(path, first.slice(1), reader, stream),
        )

    def _dialect(self, path: Path) -> _Dialect:
        encoding, delimiter = self.config.encoding, self.config.delimiter
        if AUTO not in (encoding, delimiter):
            return _Dialect(encoding, self._delimiter(path))
        sample = self._sample(path)
        transcode = encoding == AUTO
        if transcode:
            encoding = detectar_encoding(sample)
            # o BOM não é dado: utf-8-sig o descarta, e lê igual o que não tem BOM
            encoding = "utf-8-sig" if encoding == "utf-8" else encoding
        if delimiter == AUTO:
            dialeto = detectar_dialeto(sample, encoding)
            if dialeto is None:
                raise ConversionError(
                    f"{path.name}: nenhum delimitador separa o cabeçalho em colunas"
                )
            delimiter = dialeto[0]
        else:
            delimiter = self._delimiter(path)
        return _Dialect(encoding, delimiter, transcode)

    def _sample(self, path: Path) -> bytes:
        """Os primeiros 64 KB depois das `skip_rows` linhas de preâmbulo."""
        with path.open("rb") as handle:
            for _ in range(self.config.skip_rows):
                handle.readline()
            return handle.read(SAMPLE_BYTES)

    def _delimiter(self, path: Path) -> str:
        delimiter = self.config.delimiter or self.default_delimiter
        if delimiter is None:
            raise ConversionError(
                f"{path.name}: informe o delimitador (ConverterConfig.delimiter)"
            )
        return delimiter

    def _width(self, path: Path, dialect: _Dialect) -> int:
        try:
            probe, stream = self._open(path, dialect, width=None)
        except pa.ArrowInvalid as exc:
            if "Empty CSV file" in str(exc):
                raise ConversionError(f"{path.name}: arquivo vazio") from exc
            raise ConversionError(f"{path.name}: {exc}") from exc
        stream.close()
        return len(probe.schema)

    def _open(
        self, path: Path, dialect: _Dialect, width: int | None
    ) -> tuple[pacsv.CSVStreamingReader, BinaryIO]:
        types = {f"f{i}": pa.string() for i in range(width)} if width else None
        stream: BinaryIO
        if dialect.transcode:
            stream = io.BufferedReader(_Utf8Reader(path, dialect.encoding), BLOCK_BYTES)
            encoding = "utf8"
        else:
            stream, encoding = path.open("rb"), dialect.encoding
        try:
            reader = self._reader(stream, encoding, dialect.delimiter, types)
        except BaseException:
            stream.close()
            raise
        return reader, stream

    def _reader(
        self,
        stream: BinaryIO,
        encoding: str,
        delimiter: str,
        types: dict[str, pa.DataType] | None,
    ) -> pacsv.CSVStreamingReader:
        return pacsv.open_csv(
            stream,
            read_options=pacsv.ReadOptions(
                encoding=encoding,
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
        path: Path,
        first: pa.RecordBatch,
        reader: pacsv.CSVStreamingReader,
        stream: BinaryIO,
    ) -> Iterator[pa.RecordBatch]:
        try:
            if first.num_rows:
                yield first
            while True:
                try:
                    yield reader.read_next_batch()
                except StopIteration:
                    break
                except pa.ArrowInvalid as exc:
                    raise ConversionError(str(exc)) from exc
        finally:
            stream.close()
        raw = getattr(stream, "raw", None)
        if isinstance(raw, _Utf8Reader) and raw.replaced:
            log.warning(
                "%s: bytes inválidos em %s viraram U+FFFD", path.name, raw.encoding
            )


class _Utf8Reader(io.RawIOBase):
    """O arquivo decodificado em `encoding` (com `replace`) e entregue em UTF-8."""

    def __init__(self, path: Path, encoding: str):
        self.encoding = encoding
        self.replaced = False
        self._raw = path.open("rb")
        self._decoder = codecs.getincrementaldecoder(encoding)(errors="replace")
        self._pending = b""
        self._offset = 0
        self._eof = False

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: memoryview) -> int:  # type: ignore[override]
        while self._offset == len(self._pending) and not self._eof:
            chunk = self._raw.read(BLOCK_BYTES)
            self._eof = not chunk
            text = self._decoder.decode(chunk, final=self._eof)
            self.replaced = self.replaced or "\ufffd" in text
            self._pending, self._offset = text.encode("utf-8"), 0
        size = min(len(buffer), len(self._pending) - self._offset)
        buffer[:size] = self._pending[self._offset : self._offset + size]
        self._offset += size
        return size

    def close(self) -> None:
        self._raw.close()
        super().close()
