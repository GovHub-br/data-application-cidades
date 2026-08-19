"""Validação estrutural de schema — nunca de tipo/valor.

Usado (opt-in) por BaseFileParser/registros_para_staging_parquet quando um
DatasetConfig com `expected_columns` é informado. Detecta cedo uma fonte que
perdeu/renomeou coluna, sem entrar no mérito do conteúdo — isso é dbt.
"""

import pandas as pd

from domain.exceptions import SchemaValidationError


class SchemaValidator:
    def __init__(self, expected_columns: list[str]) -> None:
        self.expected_columns = expected_columns

    def validate(self, df: pd.DataFrame) -> None:
        missing = set(self.expected_columns) - set(df.columns)
        if missing:
            raise SchemaValidationError(f"Colunas ausentes: {sorted(missing)}")
