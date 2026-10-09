"""Configuração da conversão: só literais, declarados no topo da DAG.

Tudo aqui descreve a estrutura do arquivo (onde está o cabeçalho, qual o
delimitador, qual aba), nunca tipo ou nome de coluna: isso é da prata.
"""

from dataclasses import dataclass


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
    include: str | None = None
