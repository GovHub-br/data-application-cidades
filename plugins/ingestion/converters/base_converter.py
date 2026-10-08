"""Template da conversão: um arquivo da raw vira Parquet só com texto."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.converters.columns import fix_header
from ingestion.converters.config_converter import ConverterConfig
from ingestion.converters.converter_errors import ConversionError
from ingestion.layout import safe_segment


@dataclass(frozen=True)
class Source:
    """Uma tabela dentro do arquivo (o arquivo, uma aba, uma tabela do mdb).

    `batches` traz as linhas em lotes, com as colunas na ordem do cabeçalho; os
    nomes das colunas dos lotes são ignorados. `schema` só vem preenchido quando a
    fonte já é tipada (parquet): aí o template mantém os tipos em vez de forçar
    string.
    """

    suffix: str | None
    header: Sequence[str | None]
    batches: Iterable[pa.RecordBatch]
    schema: pa.Schema | None = None


@dataclass(frozen=True)
class ConvertedFile:
    name: str
    path: Path
    rows: int
    columns: tuple[str, ...]


class FileConverter(ABC):
    """Template Method: `convert` é fixo, cada formato só implementa `_read`.

    Para cada tabela que `_read` encontra: conserta o cabeçalho (vazio, repetido),
    força todas as colunas para `string`, escreve o Parquet lote a lote (memória
    limitada a um lote) e confere o número de linhas gravadas. O nome de saída é o do
    arquivo, com a tabela como sufixo quando há mais de uma.
    """

    def __init__(self, config: ConverterConfig, memory_pool: pa.MemoryPool | None = None):
        self.config = config
        self.memory_pool = memory_pool

    def convert(self, path: Path, out_dir: Path) -> Iterator[ConvertedFile]:
        names: set[str] = set()
        for source in self._read(path):
            name = safe_segment(path.stem)
            if source.suffix:
                name += f"__{safe_segment(source.suffix)}"
            if name in names:
                raise ConversionError(f"{path.name}: nome de saída repetido ({name})")
            names.add(name)
            converted = self._write(source, out_dir / f"{name}.parquet")
            self._after_write(converted)
            yield converted

    @abstractmethod
    def _read(self, path: Path) -> Iterator[Source]:
        """As tabelas do arquivo, cada uma com cabeçalho e linhas em lotes de texto."""

    def _after_write(self, converted: ConvertedFile) -> None:
        """Gancho depois de cada Parquet (retrato de schema da detecção de drift)."""

    def _write(self, source: Source, target: Path) -> ConvertedFile:
        header = fix_header(source.header)
        schema = source.schema or pa.schema(
            [pa.field(name, pa.string()) for name in header]
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        rows = 0
        with pq.ParquetWriter(target, schema) as writer:
            for batch in source.batches:
                if batch.num_columns != len(header):
                    raise ConversionError(
                        f"{target.name}: lote com {batch.num_columns} colunas, "
                        f"cabeçalho com {len(header)}"
                    )
                columns = batch.columns
                if source.schema is None:
                    columns = [column.cast(pa.string()) for column in columns]
                writer.write_batch(pa.RecordBatch.from_arrays(columns, schema=schema))
                rows += batch.num_rows
        written = pq.ParquetFile(target).metadata.num_rows
        if written != rows:
            raise ConversionError(
                f"{target.name}: {rows} linhas lidas, {written} gravadas"
            )
        return ConvertedFile(
            name=target.name, path=target, rows=rows, columns=tuple(header)
        )


def batches_from_rows(
    rows: Iterable[Sequence[str | None]], width: int, batch_rows: int = 50_000
) -> Iterator[pa.RecordBatch]:
    """Agrupa linhas de texto em lotes; linha curta completa com nulo, longa é erro."""
    buffer: list[Sequence[str | None]] = []
    for row in rows:
        if len(row) > width:
            raise ConversionError(f"linha com {len(row)} colunas, cabeçalho com {width}")
        buffer.append(row)
        if len(buffer) == batch_rows:
            yield _to_batch(buffer, width)
            buffer = []
    if buffer:
        yield _to_batch(buffer, width)


def _to_batch(rows: list[Sequence[str | None]], width: int) -> pa.RecordBatch:
    columns = [
        pa.array([row[i] if i < len(row) else None for row in rows], type=pa.string())
        for i in range(width)
    ]
    return pa.RecordBatch.from_arrays(columns, names=[f"f{i}" for i in range(width)])
