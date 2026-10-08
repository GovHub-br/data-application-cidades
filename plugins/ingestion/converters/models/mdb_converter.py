"""Access (.mdb, .accdb), lido pelo mdbtools: uma tabela por vez, por pipe."""

import csv
import io
import re
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv

from ingestion.converters.base_converter import FileConverter, Source
from ingestion.converters.converter_errors import ConversionError
from ingestion.converters.converter_registry import ConverterFactory
from ingestion.converters.models.csv_converter import BLOCK_BYTES


@ConverterFactory.register("mdb", extensions=(".mdb", ".accdb"))
class MdbConverter(FileConverter):
    """Cada tabela do banco Access vira um Parquet `<arquivo>__<tabela>`.

    `mdb-tables` lista as tabelas de usuário; `mdb-export` entrega cada uma como CSV
    UTF-8 com vírgula, lido direto do pipe: o cabeçalho pela primeira linha, o resto
    pelo leitor do pyarrow, tudo string, sem arquivo intermediário. Os valores ficam
    no formato do mdbtools (datas inclusive); interpretar é da prata. O mdbtools é
    binário do sistema, instalado na imagem do Airflow.
    """

    def _read(self, path: Path) -> Iterator[Source]:
        for table in self._tables(path):
            yield self._table(path, table)

    def _tables(self, path: Path) -> list[str]:
        if shutil.which("mdb-tables") is None or shutil.which("mdb-export") is None:
            raise ConversionError(f"{path.name}: mdbtools não instalado (mdb-tables)")
        result = subprocess.run(
            ["mdb-tables", "-1", str(path)], capture_output=True, text=True
        )
        if result.returncode != 0:
            raise ConversionError(
                f"{path.name}: mdb-tables falhou: {result.stderr.strip()}"
            )
        tables = [name.strip() for name in result.stdout.splitlines() if name.strip()]
        if self.config.include:
            pattern = re.compile(self.config.include)
            tables = [name for name in tables if pattern.search(name)]
        return tables

    def _table(self, path: Path, table: str) -> Source:
        process = subprocess.Popen(
            ["mdb-export", str(path), table],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdout is not None
        first_line = process.stdout.readline().decode("utf-8")
        header = next(csv.reader(io.StringIO(first_line)), [])
        if not header:
            _, stderr = process.communicate()
            raise ConversionError(
                f"{path.name}: mdb-export da tabela {table!r} falhou: "
                f"{stderr.decode('utf-8', 'replace').strip()}"
            )
        return Source(
            suffix=table,
            header=header,
            batches=self._batches(path, table, process, width=len(header)),
        )

    def _batches(
        self, path: Path, table: str, process: subprocess.Popen[bytes], width: int
    ) -> Iterator[pa.RecordBatch]:
        names = [f"f{i}" for i in range(width)]
        try:
            reader = pacsv.open_csv(
                process.stdout,
                read_options=pacsv.ReadOptions(
                    column_names=names, block_size=BLOCK_BYTES
                ),
                parse_options=pacsv.ParseOptions(newlines_in_values=True),
                convert_options=pacsv.ConvertOptions(
                    column_types={name: pa.string() for name in names},
                    strings_can_be_null=True,
                    quoted_strings_can_be_null=False,
                ),
                memory_pool=self.memory_pool,
            )
            yield from reader
        except pa.ArrowInvalid as exc:
            if "Empty CSV file" not in str(exc):
                raise ConversionError(f"{path.name} [{table}]: {exc}") from exc
        finally:
            _, stderr = process.communicate()
        if process.returncode != 0:
            raise ConversionError(
                f"{path.name}: mdb-export da tabela {table!r} falhou: "
                f"{stderr.decode('utf-8', 'replace').strip()}"
            )
