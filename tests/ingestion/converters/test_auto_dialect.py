"""`encoding="auto"` e `delimiter="auto"`: o texto que chega sem dialeto declarado.

A detecção é a do `raw_para_staging` (amostra de 64 KB, `ingestion.text`): o que
a amostra não vê (um byte inválido no meio de um arquivo grande) vira U+FFFD em vez
de derrubar a conversão, como o `errors="replace"` do script.
"""

from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from ingestion.converters import ConversionError, ConverterConfig, ConverterFactory

AUTO = {"encoding": "auto", "delimiter": "auto"}


def _rows(
    tmp_path: Path, data: bytes, name: str = "f.csv", **config: Any
) -> list[dict[str, Any]]:
    path = tmp_path / name
    path.write_bytes(data)
    converter = ConverterFactory.for_file(path, ConverterConfig(**{**AUTO, **config}))
    [converted] = converter.convert(path, tmp_path / "out")
    rows: list[dict[str, Any]] = pq.read_table(converted.path).to_pylist()
    return rows


def test_cp1252_with_semicolon(tmp_path: Path) -> None:
    data = "município;valor\nSão Paulo;1,5\n".encode("cp1252")

    assert _rows(tmp_path, data) == [{"município": "São Paulo", "valor": "1,5"}]


def test_utf8_pipe_in_txt(tmp_path: Path) -> None:
    data = "mês|situação\nago|concluída\n".encode()

    assert _rows(tmp_path, data, name="base.txt") == [
        {"mês": "ago", "situação": "concluída"}
    ]


def test_utf8_bom_is_dropped(tmp_path: Path) -> None:
    data = "﻿apf;valor\n1;2\n".encode()

    assert _rows(tmp_path, data) == [{"apf": "1", "valor": "2"}]


def test_crlf_and_quoted_header_with_newline(tmp_path: Path) -> None:
    data = b'"Data de\r\nMovimento";"Valor"\r\n"01/2026";"1"\r\n'

    assert _rows(tmp_path, data) == [{"Data de\r\nMovimento": "01/2026", "Valor": "1"}]


def test_invalid_byte_after_the_sample_is_replaced(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    filler = "".join(f"linha {i};ação\n" for i in range(8_000)).encode()
    assert len(filler) > 64 * 1024
    data = b"a;b\n" + filler + b"fim;S\xe3o\n"

    rows = _rows(tmp_path, data)

    assert rows[0] == {"a": "linha 0", "b": "ação"}
    assert rows[-1] == {"a": "fim", "b": "S\ufffdo"}
    assert len(rows) == 8_001
    assert "U+FFFD" in caplog.text


def test_byte_undefined_in_cp1252_after_the_sample_is_replaced(tmp_path: Path) -> None:
    filler = b"".join(b"linha %d;a\xe7\xe3o\n" % i for i in range(8_000))
    data = b"a;b\n" + filler + b"fim;x\x81y\n"

    rows = _rows(tmp_path, data)

    assert rows[0] == {"a": "linha 0", "b": "ação"}
    assert rows[-1] == {"a": "fim", "b": "x\ufffdy"}


def test_detection_starts_after_the_preamble(tmp_path: Path) -> None:
    data = b"Relatorio, gerado em 01/10/2026, pagina 1\na|b\n1|2\n"

    assert _rows(tmp_path, data, skip_rows=1) == [{"a": "1", "b": "2"}]


def test_only_one_of_them_can_be_auto(tmp_path: Path) -> None:
    data = "a;b\nç;1\n".encode("latin-1")

    assert _rows(tmp_path, data, encoding="latin-1") == [{"a": "ç", "b": "1"}]
    assert _rows(tmp_path, b"a|b\n1|2\n", delimiter="|") == [{"a": "1", "b": "2"}]


def test_single_column_text_is_a_conversion_error(tmp_path: Path) -> None:
    with pytest.raises(ConversionError, match="delimitador"):
        _rows(tmp_path, b"so uma coluna\nvalor\n")
