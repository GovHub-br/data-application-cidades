"""Conversão (Template Method + Factory).

Arquivo da raw -> Parquet só com texto na staging.
"""

from ingestion.converters.base_converter import (
    ConvertedFile,
    FileConverter,
    Source,
    batches_from_rows,
)
from ingestion.converters.columns import fix_header
from ingestion.converters.config_converter import ConverterConfig
from ingestion.converters.converter_errors import ConversionError
from ingestion.converters.converter_registry import ConverterFactory

__all__ = [
    "ConversionError",
    "ConvertedFile",
    "ConverterConfig",
    "ConverterFactory",
    "FileConverter",
    "Source",
    "batches_from_rows",
    "fix_header",
]
