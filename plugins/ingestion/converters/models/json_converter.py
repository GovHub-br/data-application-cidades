"""JSON, de lista plana a resposta aninhada, lido em streaming pelo ijson."""

import json
import re
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import ijson

from ingestion.converters.base_converter import FileConverter, Source, batches_from_rows
from ingestion.converters.config_converter import Field
from ingestion.converters.converter_errors import ConversionError
from ingestion.converters.converter_registry import ConverterFactory

BATCH_ROWS = 10_000
SCALAR_COLUMN = "value"
ROOT = "item"
KEY, VALUE = "key", "value"
SEGMENT = re.compile(r"^([^\[\{]*)(\[\*\]|\{keys\}|\{values\})?$")


@ConverterFactory.register("json", extensions=(".json",))
class JsonConverter(FileConverter):
    """Registros em `config.record_path` viram linhas; o desenho vem da configuração.

    - Lista plana (padrão): cada registro é uma linha e cada chave uma coluna. Duas
      passadas no arquivo: a primeira junta as chaves de todos os registros na
      ordem em que aparecem (registro heterogêneo não perde coluna), a segunda
      escreve. Registro que não é objeto vai para a coluna `value`.
    - `key_column`: o item em `record_path` é um objeto cujas chaves são dado
      (`{"2026-10-08": {...}}`); cada chave vira uma linha, nessa coluna.
    - `nested`: listas dentro de cada registro, percorridas em ordem, como o
      `record_path` em lista do `pandas.json_normalize`; uma linha por item do
      último nível. `explode_keys`: no último nível, o objeto cujas chaves viram
      linhas (`key`, `value`).
    - `columns`: as colunas de saída, cada uma um `Field` com o caminho até o
      valor a partir de qualquer nível (`item`, um nome de `nested`, `key`,
      `value`); uma passada só. Sem `columns`, as colunas são as chaves do último
      nível.

    Texto, sem tipo: número sai com o texto da fonte (`5.10` continua `5.10`),
    `true`/`false` em minúsculas, nulo como nulo, objeto ou lista como texto JSON.
    Nenhuma passada guarda o arquivo em memória; o que fica é um registro por vez.
    """

    def _read(self, path: Path) -> Iterator[Source]:
        if self.config.columns:
            names = list(self.config.columns)
            fields = list(self.config.columns.values())
            yield Source(
                suffix=None,
                header=names,
                batches=batches_from_rows(
                    self._declared_rows(path, fields),
                    width=len(names),
                    batch_rows=BATCH_ROWS,
                ),
            )
            return
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

    def _declared_rows(self, path: Path, fields: list[Field]) -> Iterator[list[Any]]:
        count = 0
        for context in self._contexts(path):
            count += 1
            yield [_resolve(field, context) for field in fields]
        if not count:
            raise ConversionError(
                f"{path.name}: nenhum registro em {self.config.record_path!r}"
            )

    def _records(self, path: Path) -> Iterator[dict[str, Any]]:
        """O objeto do último nível de cada contexto, como linha plana."""
        last = self.config.nested[-1] if self.config.nested else ROOT
        for context in self._contexts(path):
            record = context[last]
            if not isinstance(record, dict):
                record = {SCALAR_COLUMN: record}
            if KEY in context and self.config.explode_keys:
                record = {
                    **{k: v for k, v in record.items() if k != self.config.explode_keys},
                    KEY: context[KEY],
                    VALUE: context[VALUE],
                }
            yield record

    def _contexts(self, path: Path) -> Iterator[dict[str, Any]]:
        """Um contexto por linha: o objeto de cada nível, pelo nome do nível."""
        with path.open("rb") as source:
            try:
                for item in ijson.items(source, self.config.record_path, use_float=False):
                    for record in self._explode(item):
                        yield from self._descend({ROOT: record})
            except ijson.JSONError as exc:
                raise ConversionError(f"{path.name}: json inválido ({exc})") from exc

    def _descend(self, context: dict[str, Any]) -> Iterator[dict[str, Any]]:
        contexts = [context]
        current = ROOT
        for name in self.config.nested:
            contexts = [
                {**ctx, name: child}
                for ctx in contexts
                if isinstance(ctx[current], dict)
                for child in ctx[current].get(name) or []
            ]
            current = name
        for ctx in contexts:
            target = ctx[current]
            if self.config.explode_keys is None:
                yield ctx
                continue
            keyed = (
                target.get(self.config.explode_keys) if isinstance(target, dict) else None
            )
            for key, value in (keyed or {}).items():
                yield {**ctx, KEY: key, VALUE: value}

    def _explode(self, item: Any) -> Iterator[Any]:
        key_column = self.config.key_column
        if key_column is None or not isinstance(item, dict):
            yield item
            return
        for key, value in item.items():
            fields = value if isinstance(value, dict) else {SCALAR_COLUMN: value}
            yield {key_column: key, **fields}


def _resolve(field: Field, context: dict[str, Any]) -> str | None:
    values: list[Any] = [context]
    for segment in field.path.split("."):
        match = SEGMENT.match(segment)
        if not match:
            raise ConversionError(f"caminho inválido em Field: {field.path!r}")
        name, mode = match.groups()
        if name:
            values = [v[name] for v in values if isinstance(v, dict) and name in v]
        if mode == "[*]":
            values = [item for v in values if isinstance(v, list) for item in v]
        elif mode == "{keys}":
            values = [key for v in values if isinstance(v, dict) for key in v]
        elif mode == "{values}":
            values = [item for v in values if isinstance(v, dict) for item in v.values()]
    values = [v for v in values if v is not None]
    if not values:
        return field.default
    if field.join is not None:
        return field.join.join(str(_text(v)) for v in values)
    if len(values) > 1:
        raise ConversionError(
            f"{field.path!r} deu {len(values)} valores; declare join para juntá-los"
        )
    return _text(values[0])


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
