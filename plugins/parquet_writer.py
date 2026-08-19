"""Writer da staging: DataFrame -> texto -> parquet -> MinIO.

Extraído de base_file_parser.py, onde a mesma sequência (stringificar +
serializar + subir) estava duplicada entre `BaseFileParser._salvar_parquet` e
`registros_para_staging_parquet`.
"""

import io
import logging

import pandas as pd

from cliente_minio import upload_staging_parquet
from domain.models import ParseResult, SourceFile


def _stringificar(df: pd.DataFrame) -> pd.DataFrame:
    """Cast de toda coluna pra texto, preservando nulo real (nunca a string 'None').

    Também normaliza nome de coluna (strip + lower) — cabeçalho de planilha/CSV
    vem com casing/espaço inconsistente, e isso é estrutural, não tipagem.
    """
    df = df.rename(columns=lambda c: str(c).strip().lower())
    for col in df.columns:
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else str(v)).astype("string")
    return df


def salvar_staging_parquet(source: SourceFile, df: pd.DataFrame) -> ParseResult:
    """Stringifica, serializa e sobe o parquet (texto) de staging no MinIO."""
    df = _stringificar(df)
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    upload_staging_parquet(source.fonte, source.dado, buffer.getvalue())
    logging.info(
        f"[parquet_writer] staging/{source.fonte}/{source.dado}.parquet "
        f"({len(df)} linhas, {len(df.columns)} colunas)"
    )
    return ParseResult(source=source, rows=len(df), columns=len(df.columns))
