"""Parquet que já chega na raw: passa para a staging com os tipos que tem."""

from collections.abc import Iterator
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.converters.base_converter import FileConverter, Source
from ingestion.converters.converter_registry import ConverterFactory

BATCH_ROWS = 64_000


@ConverterFactory.register("parquet", extensions=(".parquet",))
class ParquetConverter(FileConverter):
    """Lê em lotes e regrava com o schema original: é a exceção ao "tudo string".

    A fonte já entregou tipos (outra pipeline, um dump); trocá-los por texto aqui
    perderia informação sem ganho. Reescrever, em vez de copiar, mantém o arquivo
    passando pelo mesmo template (contagem de linhas, gancho de drift).
    """

    def _read(self, path: Path) -> Iterator[Source]:
        parquet = pq.ParquetFile(path)
        schema = parquet.schema_arrow
        yield Source(
            suffix=None,
            header=schema.names,
            batches=parquet.iter_batches(batch_size=BATCH_ROWS),
            schema=schema,
        )
