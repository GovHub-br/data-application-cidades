"""Contrato de todo FileConverter.

A partir de um arquivo de texto, todo formato gera Parquet só com colunas string,
sem tipar nem renomear, com o nome derivado do arquivo e sem gravar nada antes de
ser consumido. Formato que já chega tipado (parquet) é a exceção da regra de string.

Cada implementação registrada entra em IMPLEMENTATIONS (e tem amostra em
`conftest.CASES`) e herda estes testes.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.converters import ConverterFactory
from tests.ingestion.converters.conftest import ContractCase

IMPLEMENTATIONS = ["csv", "txt"]

pytestmark = pytest.mark.parametrize("contract_case", IMPLEMENTATIONS, indirect=True)


def _convert(case: ContractCase, out: Path) -> list[Path]:
    converter = ConverterFactory.for_file(case.path, case.config)
    return [converted.path for converted in converter.convert(case.path, out)]


def test_outputs_are_named_after_the_source_file(
    contract_case: ContractCase, tmp_path: Path
) -> None:
    paths = _convert(contract_case, tmp_path / "out")

    assert [p.name for p in paths] == contract_case.outputs


def test_text_sources_become_string_only_parquet(
    contract_case: ContractCase, tmp_path: Path
) -> None:
    for path in _convert(contract_case, tmp_path / "out"):
        schema = pq.read_schema(path)
        if contract_case.text_source:
            assert all(field.type == pa.string() for field in schema), schema


def test_every_source_row_is_written(contract_case: ContractCase, tmp_path: Path) -> None:
    paths = _convert(contract_case, tmp_path / "out")

    assert sum(pq.ParquetFile(p).metadata.num_rows for p in paths) == contract_case.rows


def test_nothing_is_written_before_consuming(
    contract_case: ContractCase, tmp_path: Path
) -> None:
    converter = ConverterFactory.for_file(contract_case.path, contract_case.config)
    converter.convert(contract_case.path, tmp_path / "out")

    assert not (tmp_path / "out").exists()
