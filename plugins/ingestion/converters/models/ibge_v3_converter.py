"""IBGE v3 (`/agregados/.../variaveis/...`): JSON aninhado → uma linha por valor."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import ijson

from ingestion.converters.base_converter import FileConverter, Source, batches_from_rows
from ingestion.converters.converter_errors import ConversionError
from ingestion.converters.converter_registry import ConverterFactory

BATCH_ROWS = 10_000
COLUMNS = (
    "variavel_id",
    "variavel_nome",
    "localidade_id",
    "localidade_nome",
    "classificacao_id",
    "classificacao_nome",
    "categoria_id",
    "categoria_nome",
    "unidade",
    "periodo",
    "valor",
)


@ConverterFactory.register("ibge_v3")
class IbgeV3Converter(FileConverter):
    """Achata a resposta da API de agregados do IBGE, como o `ClienteIBGE` fazia.

    A resposta é uma lista de variáveis; cada uma traz `resultados` (uma combinação
    de categorias das classificações pedidas) e, dentro deles, uma `serie` por
    localidade, com o período como CHAVE (`{"202501": "1.3"}`). Formato estrutural,
    como o mdb: desempacotar em SQL obrigaria o merge a acontecer depois da prata.

    Uma linha por (variável, localidade, classificação, categoria, período), tudo
    texto. Sem classificação, `classificacao_id`/`categoria_id` valem `"0"` e os
    nomes ficam vazios; com várias, ids juntados por `|` e nomes por ` | ` (a
    mesma regra do achatamento antigo, que as pratas e as chaves do merge supõem).
    O valor fica como a API mandou (`...` e `-` incluídos): tipar é da prata.

    Lê uma variável por vez (ijson); a memória acompanha o tamanho de uma variável,
    não o do arquivo.
    """

    def _read(self, path: Path) -> Iterator[Source]:
        yield Source(
            suffix=None,
            header=COLUMNS,
            batches=batches_from_rows(
                _rows(path), width=len(COLUMNS), batch_rows=BATCH_ROWS
            ),
        )


def _rows(path: Path) -> Iterator[list[str | None]]:
    count = 0
    with path.open("rb") as source:
        try:
            for variable in ijson.items(source, "item", use_float=False):
                for row in _flatten(variable):
                    count += 1
                    yield row
        except ijson.JSONError as exc:
            raise ConversionError(f"{path.name}: json inválido ({exc})") from exc
        except (KeyError, TypeError) as exc:
            raise ConversionError(
                f"{path.name}: fora do formato da API v3 do IBGE ({exc!r})"
            ) from exc
    if not count:
        raise ConversionError(f"{path.name}: nenhum valor na resposta do IBGE")


def _flatten(variable: dict[str, Any]) -> Iterator[list[str | None]]:
    for result in variable["resultados"]:
        classifications = result.get("classificacoes") or []
        category_ids: list[str] = []
        category_names: list[str] = []
        for classification in classifications:
            for key, name in (classification.get("categoria") or {}).items():
                category_ids.append(str(key))
                category_names.append(str(name))
        context = [
            "|".join(str(c["id"]) for c in classifications) or "0",
            " | ".join(str(c["nome"]) for c in classifications),
            "|".join(category_ids) or "0",
            " | ".join(category_names),
        ]
        for series in result["series"]:
            location = series["localidade"]
            for period, value in series["serie"].items():
                yield [
                    str(variable["id"]),
                    str(variable["variavel"]),
                    str(location["id"]),
                    str(location["nome"]),
                    *context,
                    str(variable["unidade"]),
                    str(period),
                    None if value is None else str(value),
                ]
