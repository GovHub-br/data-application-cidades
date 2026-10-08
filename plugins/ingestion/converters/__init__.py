"""Conversão (Template Method + Factory).

Arquivo da raw -> Parquet só com texto na staging.
"""

from ingestion.converters.columns import fix_header

__all__ = ["fix_header"]
