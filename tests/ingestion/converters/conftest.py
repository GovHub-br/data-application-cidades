"""Amostras de cada formato, que alimentam o contrato dos conversores."""

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from ingestion.converters import ConverterConfig


@dataclass
class ContractCase:
    """Um arquivo de amostra, a configuração para lê-lo e quantas linhas tem.

    `text_source` é falso só para formatos que já chegam tipados (parquet).
    """

    path: Path
    config: ConverterConfig
    rows: int
    text_source: bool = True
    outputs: list[str] = field(default_factory=list)


def _csv_case(tmp_path: Path) -> ContractCase:
    path = tmp_path / "serie.csv"
    path.write_text("mes,valor\n01/2026,1.5\n02/2026,007\n", encoding="utf-8")
    return ContractCase(path, ConverterConfig(), rows=2, outputs=["serie.parquet"])


def _txt_case(tmp_path: Path) -> ContractCase:
    path = tmp_path / "base pf.txt"
    path.write_bytes("mês|valor\nago|1\n".encode("latin-1"))
    config = ConverterConfig(delimiter="|", encoding="latin-1")
    return ContractCase(path, config, rows=1, outputs=["base_pf.parquet"])


def _json_case(tmp_path: Path) -> ContractCase:
    path = tmp_path / "ipca.json"
    path.write_text(
        '[{"data":"01/07/2026","valor":"0.26"},{"data":"01/08/2026","valor":"-0.32"}]'
    )
    return ContractCase(path, ConverterConfig(), rows=2, outputs=["ipca.parquet"])


CASES: dict[str, Callable[[Path], ContractCase]] = {
    "csv": _csv_case,
    "txt": _txt_case,
    "json": _json_case,
}


@pytest.fixture
def contract_case(request: pytest.FixtureRequest, tmp_path: Path) -> ContractCase:
    return CASES[request.param](tmp_path)
