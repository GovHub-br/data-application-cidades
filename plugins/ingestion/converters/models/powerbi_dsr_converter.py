"""Power BI público (`querydata`): a resposta em DSR vira uma linha por linha."""

from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import ijson

from ingestion.converters.base_converter import FileConverter, Source, batches_from_rows
from ingestion.converters.converter_errors import ConversionError
from ingestion.converters.converter_registry import ConverterFactory

BATCH_ROWS = 10_000


@ConverterFactory.register("powerbi_dsr")
class PowerBiDsrConverter(FileConverter):
    """Decodifica o DSR (Data Shape Result) do `querydata` de um relatório público.

    O DSR é compactado: cada linha (`DM0`) traz em `C` só os valores que não são
    nulos nem repetidos. A máscara `Ø` marca as posições nulas, a `R` as que
    repetem a linha anterior, e `S` (na primeira linha) dá a ordem das colunas
    (`M0`, `M1`…); colunas com `DN` guardam índices de `ValueDicts`. Sem
    decodificar, ler `C` por posição troca os valores de coluna sempre que algum
    vem nulo (o mês do Novo CAGED ainda não publicado, por exemplo). Formato
    estrutural, como o mdb.

    As colunas recebem o nome do `descriptor` da consulta (`M0` → `Admitidos`).
    Tudo texto; consulta sem linhas vira tabela vazia com as colunas. Lê um
    resultado por vez; uma resposta do `querydata` é pequena.
    """

    def _read(self, path: Path) -> Iterator[Source]:
        results = list(_results(path))
        if not results:
            raise ConversionError(f"{path.name}: fora do formato do Power BI")
        header = _header(results[0])
        for result in results[1:]:
            if _header(result) != header:
                raise ConversionError(f"{path.name}: consultas com colunas diferentes")
        yield Source(
            suffix=None,
            header=header,
            batches=batches_from_rows(
                (row for result in results for row in _decode(result)),
                width=len(header),
                batch_rows=BATCH_ROWS,
            ),
        )


def _results(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("rb") as source:
        try:
            for result in ijson.items(
                source, "results.item.result.data", use_float=False
            ):
                if "dsr" not in result or "descriptor" not in result:
                    raise ConversionError(f"{path.name}: fora do formato do Power BI")
                yield result
        except ijson.JSONError as exc:
            raise ConversionError(f"{path.name}: json inválido ({exc})") from exc


def _header(result: dict[str, Any]) -> list[str]:
    return [str(item["Name"]) for item in result["descriptor"]["Select"]]


def _decode(result: dict[str, Any]) -> Iterator[list[str | None]]:
    names = {item["Value"]: item["Name"] for item in result["descriptor"]["Select"]}
    order = [item["Value"] for item in result["descriptor"]["Select"]]
    for dataset in result["dsr"].get("DS", []):
        value_dicts = dataset.get("ValueDicts", {})
        for group in dataset.get("PH", []):
            schema: list[dict[str, Any]] = []
            previous: list[Any] = []
            for row in group.get("DM0", []):
                schema = row.get("S", schema)
                values = iter(row.get("C", []))
                nulls, repeats = row.get("Ø", 0), row.get("R", 0)
                decoded = []
                for i, column in enumerate(schema):
                    if repeats & (1 << i):
                        value = previous[i]
                    elif nulls & (1 << i):
                        value = None
                    else:
                        value = next(values)
                        if "DN" in column and value is not None:
                            value = value_dicts[column["DN"]][value]
                    decoded.append(value)
                previous = decoded
                by_name = {names.get(c["N"], c["N"]): v for c, v in zip(schema, decoded)}
                yield [_text(by_name.get(names[key])) for key in order]


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(int(value)) if value == value.to_integral_value() else str(value)
    return str(value)
