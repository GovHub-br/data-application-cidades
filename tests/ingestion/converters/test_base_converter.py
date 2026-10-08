"""Template FileConverter e ConverterFactory, com um conversor fictício."""

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.converters import (
    ConversionError,
    ConvertedFile,
    ConverterConfig,
    ConverterFactory,
    FileConverter,
    Source,
    batches_from_rows,
)

AFTER_WRITE: list[ConvertedFile] = []


@pytest.fixture
def dummy() -> Iterator[str]:
    """Formato `.dummy`: cada linha do arquivo é uma tabela 'aba;cab1,cab2;v1,v2;...'."""

    @ConverterFactory.register("dummy", extensions=(".dummy",))
    class DummyConverter(FileConverter):
        def _read(self, path: Path) -> Iterator[Source]:
            for line in path.read_text().splitlines():
                suffix, header, *rows = line.split(";")
                cells = [[c or None for c in row.split(",")] for row in rows]
                names = [h or None for h in header.split(",")]
                yield Source(
                    suffix=suffix or None,
                    header=names,
                    batches=batches_from_rows(cells, width=len(names), batch_rows=2),
                )

        def _after_write(self, converted: ConvertedFile) -> None:
            AFTER_WRITE.append(converted)

    AFTER_WRITE.clear()
    yield "dummy"
    ConverterFactory._registry.pop("dummy")
    ConverterFactory._extensions.pop(".dummy")


def _convert(
    tmp_path: Path, content: str, name: str = "relatório.dummy"
) -> list[ConvertedFile]:
    source = tmp_path / name
    source.write_text(content)
    converter = ConverterFactory.for_file(source, ConverterConfig())
    return list(converter.convert(source, tmp_path / "out"))


def test_writes_one_string_parquet_per_table_named_after_the_file(
    dummy: str, tmp_path: Path
) -> None:
    [only] = _convert(tmp_path, ";mes,valor;01/2026,007;02/2026,")

    assert only.name == "relat_rio.parquet"
    table = pq.read_table(only.path)
    assert table.schema == pa.schema([("mes", pa.string()), ("valor", pa.string())])
    assert table.to_pylist() == [
        {"mes": "01/2026", "valor": "007"},
        {"mes": "02/2026", "valor": None},
    ]
    assert (only.rows, only.columns) == (2, ("mes", "valor"))


def test_tables_of_one_file_get_a_suffix(dummy: str, tmp_path: Path) -> None:
    converted = _convert(tmp_path, "aba 1;a;1\naba2;b;2")

    assert [c.name for c in converted] == [
        "relat_rio__aba_1.parquet",
        "relat_rio__aba2.parquet",
    ]


def test_header_is_fixed_but_not_renamed(dummy: str, tmp_path: Path) -> None:
    [only] = _convert(tmp_path, ";Valor (R$),,Valor (R$);1,2,3")

    assert only.columns == ("Valor (R$)", "column_2", "Valor (R$)_2")


def test_header_only_file_gives_empty_parquet_with_schema(
    dummy: str, tmp_path: Path
) -> None:
    [only] = _convert(tmp_path, ";a,b")

    table = pq.read_table(only.path)
    assert table.num_rows == 0
    assert table.schema.names == ["a", "b"]


def test_rows_wider_than_header_are_an_error(dummy: str, tmp_path: Path) -> None:
    with pytest.raises(ConversionError, match="colunas"):
        _convert(tmp_path, ";a,b;1,2,3")


def test_short_rows_are_padded_with_nulls(dummy: str, tmp_path: Path) -> None:
    [only] = _convert(tmp_path, ";a,b,c;1")

    assert pq.read_table(only.path).to_pylist() == [{"a": "1", "b": None, "c": None}]


def test_nothing_is_written_before_consuming(dummy: str, tmp_path: Path) -> None:
    source = tmp_path / "f.dummy"
    source.write_text(";a;1")

    ConverterFactory.for_file(source, ConverterConfig()).convert(source, tmp_path / "out")

    assert not (tmp_path / "out").exists()


def test_after_write_hook_sees_every_file(dummy: str, tmp_path: Path) -> None:
    converted = _convert(tmp_path, "x;a;1\ny;b;2")

    assert AFTER_WRITE == converted


def test_factory_prefers_the_configured_format(dummy: str, tmp_path: Path) -> None:
    source = tmp_path / "sem_extensao"
    source.write_text(";a;1")

    converter = ConverterFactory.for_file(source, ConverterConfig(format="dummy"))

    assert type(converter).__name__ == "DummyConverter"


def test_unknown_format_lists_registered_ones(dummy: str, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"dummy.*\.dummy"):
        ConverterFactory.for_file(tmp_path / "arquivo.xyz", ConverterConfig())


def test_config_defaults() -> None:
    config = ConverterConfig()

    assert (config.encoding, config.skip_rows, config.header_row) == ("utf-8", 0, 1)
    assert config.format is None and config.delimiter is None
