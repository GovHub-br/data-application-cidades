"""Configuração da conversão: só literais, declarados no topo da DAG.

Tudo aqui descreve a estrutura do arquivo (onde está o cabeçalho, qual o
delimitador, qual aba), nunca tipo ou nome de coluna: isso é da prata.
"""

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Field:
    """Uma coluna de saída do JSON, pelo caminho até o valor (estilo `json_normalize`).

    O caminho começa pelo nível: `item` (o registro), o nome de cada lista de
    `nested`, ou `key`/`value` (a chave e o valor de `explode_keys`). Depois,
    segmentos separados por ponto; `[*]` no fim de um segmento percorre a lista,
    e `{keys}`/`{values}` percorrem as chaves ou os valores de um objeto:

        Field("item.id")
        Field("resultados.classificacoes[*].categoria{keys}", join="|", default="0")

    Vários valores exigem `join`; nenhum valor dá `default` (ou nulo).
    """

    path: str
    join: str | None = None
    default: str | None = None


@dataclass(frozen=True)
class ConverterConfig:
    """Como ler os arquivos de uma partição da raw.

    - `format`: força o conversor; sem ele, vale a extensão do arquivo.
    - `encoding`, `delimiter`, `skip_rows`: texto delimitado (csv, txt). `skip_rows`
      pula linhas de preâmbulo antes do cabeçalho.
    - `sheet`, `header_row`: xlsx (`sheet=None` lê todas as abas; `header_row` é
      1-based).
    - `record_path`: json, prefixo do ijson onde estão os registros (`item` para uma
      lista no topo).
    - `key_column`: json, quando o que está em `record_path` é um objeto cujas
      chaves são dado (a data de cada pregão, por exemplo): cada chave vira uma
      linha, com a chave nessa coluna.
    - `nested`, `explode_keys`, `columns`: json aninhado, como o
      `pandas.json_normalize`. `nested` são listas dentro de cada registro,
      percorridas em ordem (uma linha por item do último nível); `explode_keys` é,
      no último nível, o objeto cujas chaves viram linhas; `columns` declara as
      colunas de saída (`Field`). Sem `columns`, as colunas são as chaves do
      último nível.
    - `include`: regex de aba, tabela (mdb) ou membro (zip) a converter.
    """

    format: str | None = None
    encoding: str = "utf-8"
    delimiter: str | None = None
    skip_rows: int = 0
    sheet: str | None = None
    header_row: int = 1
    record_path: str = "item"
    key_column: str | None = None
    nested: tuple[str, ...] = ()
    explode_keys: str | None = None
    columns: Mapping[str, Field] | None = None
    include: str | None = None
