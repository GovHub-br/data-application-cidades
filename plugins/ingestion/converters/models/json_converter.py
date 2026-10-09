"""JSON (lista de registros), lido em streaming pelo ijson em duas passadas."""

import json
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import ijson

from ingestion.converters.base_converter import FileConverter, Source, batches_from_rows
from ingestion.converters.converter_errors import ConversionError
from ingestion.converters.converter_registry import ConverterFactory

BATCH_ROWS = 10_000
SCALAR_COLUMN = "value"


@ConverterFactory.register("json", extensions=(".json",))
class JsonConverter(FileConverter):
    """Cada registro em `config.record_path` vira uma linha; cada chave, uma coluna.

    Duas passadas no arquivo local: a primeira junta as chaves de todos os registros
    na ordem em que aparecem (registro heterogêneo não perde coluna); a segunda
    escreve. Nenhuma guarda o arquivo em memória.

    Texto, sem tipo: número sai com o texto da fonte (`5.10` continua `5.10`),
    `true`/`false` em minúsculas, nulo como nulo, objeto ou lista aninhada como
    texto JSON (desempacotar é da prata; ali dentro o número passa por float).
    Registro que não é objeto vai para a coluna `value`.

    Com `config.key_column`, o item em `record_path` é um objeto cujas chaves são
    dado (`{"2026-10-08": {...}, ...}`): cada chave vira uma linha, com a chave
    nessa coluna e os campos do valor ao lado.
    """

    def _read(self, path: Path) -> Iterator[Source]:
        columns: dict[str, None] = {}
        for record in self._records(path):
            columns.update(dict.fromkeys(record))
        if not columns:
            raise ConversionError(
                f"{path.name}: nenhum registro em {self.config.record_path!r}"
            )
        names = list(columns)
        rows = (
            [_text(record.get(name)) for name in names] for record in self._records(path)
        )
        yield Source(
            suffix=None,
            header=names,
            batches=batches_from_rows(rows, width=len(names), batch_rows=BATCH_ROWS),
        )

    def _records(self, path: Path) -> Iterator[dict[str, Any]]:
        with path.open("rb") as source:
            try:
                for item in ijson.items(source, self.config.record_path, use_float=False):
                    for record in self._explode(item):
                        yield (
                            record
                            if isinstance(record, dict)
                            else {SCALAR_COLUMN: record}
                        )
            except ijson.JSONError as exc:
                raise ConversionError(f"{path.name}: json inválido ({exc})") from exc

    def _explode(self, item: Any) -> Iterator[Any]:
        key_column = self.config.key_column
        if key_column is None or not isinstance(item, dict):
            yield item
            return
        for key, value in item.items():
            fields = value if isinstance(value, dict) else {SCALAR_COLUMN: value}
            yield {key_column: key, **fields}


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(_plain(value), ensure_ascii=False)
    return str(value)


def _plain(value: Any) -> Any:
    """Decimal do ijson vira int/float para caber no json.dumps."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value
