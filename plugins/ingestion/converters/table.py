"""Conversão por uma função da DAG: o arquivo da raw vira uma tabela (pandas).

É o caminho das fontes cuja staging, na main, saía dos registros que o cliente
montava (colunas escolhidas e renomeadas, rodapé fora, `dt_ingest`). A regra fica
na DAG, numa função pequena e nomeada; aqui só o que é igual para todas: gravar
só texto, como o `registros_para_staging_parquet` da main.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

#: `(arquivo da raw) -> tabela`, ou `(arquivo da raw, staging atual) -> tabela`.
TableFunction = Callable[..., pd.DataFrame]


@dataclass(frozen=True)
class Table:
    """A conversão de uma fonte pela função `function` da DAG.

    - `fallback_json`: grava também os registros em `raw/<fonte>/fallback_json/`,
      como a main fazia nas fontes de planilha.
    - `accumulate`: a função recebe também a staging atual (ou `None`, na
      primeira vez), para fontes que só devolvem a janela recente e cujo
      histórico vive na própria staging.
    """

    function: TableFunction
    fallback_json: bool = False
    accumulate: bool = False


def convert_table(
    table: Table, raw: Path, out_dir: Path, current: Path | None = None
) -> list[Path]:
    """Grava `<out_dir>/<raw sem extensão>.parquet` (e o fallback, se pedido).

    `current` é o Parquet atual da staging, para `accumulate`.
    """
    if table.accumulate:
        previous = pd.read_parquet(current) if current is not None else None
        frame = table.function(raw, previous)
    else:
        frame = table.function(raw)
    records = frame.to_dict(orient="records")
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet = out_dir / f"{raw.stem}.parquet"
    _all_text(pd.DataFrame(records)).to_parquet(parquet, engine="pyarrow", index=False)
    written = [parquet]
    if table.fallback_json:
        fallback = out_dir / "fallback_json" / f"{raw.stem}.json"
        fallback.parent.mkdir(parents=True, exist_ok=True)
        fallback.write_text(json.dumps(records, ensure_ascii=False, default=str))
        written.append(fallback)
    return written


def _all_text(frame: pd.DataFrame) -> pd.DataFrame:
    """O `_todas_colunas_texto` da main: sem inferência de tipo no Parquet."""

    def text(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, default=str)
        if pd.isna(value):
            return None
        if hasattr(value, "isoformat"):
            return str(value.isoformat())
        return str(value)

    return cast(
        pd.DataFrame, frame.apply(lambda column: column.map(text).astype("string"))
    )
