"""O que é próprio do txt: delimitador obrigatório, .tsv com tabulação."""

from pathlib import Path

import pytest

from ingestion.converters import ConversionError
from tests.ingestion.converters.test_csv_converter import _rows


def test_txt_requires_an_explicit_delimiter(tmp_path: Path) -> None:
    with pytest.raises(ConversionError, match="delimitador"):
        _rows(tmp_path, b"a|b\n1|2\n", name="f.txt")


def test_tsv_defaults_to_tab(tmp_path: Path) -> None:
    assert _rows(tmp_path, b"a\tb\n1\t2\n", name="f.tsv") == [{"a": "1", "b": "2"}]
