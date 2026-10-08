"""Contrato de todo Extractor.

Toda estratégia grava o dado da fonte em disco, no formato original, e devolve um
RawFile por parte, sem carregar o dataset em memória e sem gravar nada antes de ser
consumida.

Cada implementação registrada entra em IMPLEMENTATIONS (e tem um caso em
`conftest.CASES`) e herda estes testes.
"""

import hashlib
from pathlib import Path

import pytest

from ingestion.extractors import ExtractorFactory, SourceNotFoundError
from ingestion.layout import safe_segment
from tests.ingestion.extractors.conftest import ContractCase

IMPLEMENTATIONS = ["api", "http_file"]

pytestmark = pytest.mark.parametrize("contract_case", IMPLEMENTATIONS, indirect=True)


def test_strategy_is_registered(contract_case: ContractCase) -> None:
    assert type(contract_case.build()) in ExtractorFactory._registry.values()


def test_writes_every_part_byte_for_byte_inside_work_dir(
    contract_case: ContractCase, tmp_path: Path
) -> None:
    parts = list(contract_case.build().extract(tmp_path))

    assert {part.name: part.path.read_bytes() for part in parts} == contract_case.expected
    for part in parts:
        assert tmp_path in part.path.parents
        assert part.size == part.path.stat().st_size
        assert part.sha256 == hashlib.sha256(part.path.read_bytes()).hexdigest()


def test_names_are_safe_and_unique(contract_case: ContractCase, tmp_path: Path) -> None:
    names = [part.name for part in contract_case.build().extract(tmp_path)]

    assert names == [safe_segment(name) for name in names]
    assert len(names) == len(set(names))


def test_nothing_is_written_before_the_first_part_is_requested(
    contract_case: ContractCase, tmp_path: Path
) -> None:
    contract_case.build().extract(tmp_path)

    assert not any(tmp_path.iterdir())


def test_missing_source_raises_source_not_found(
    contract_case: ContractCase, tmp_path: Path
) -> None:
    with pytest.raises(SourceNotFoundError):
        list(contract_case.build_missing().extract(tmp_path))
