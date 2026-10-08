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

# Registra os formatos no ConverterFactory (import pelo efeito do decorator).
from ingestion.converters.models import csv_converter  # noqa: E402, F401
from ingestion.converters.models import txt_converter  # noqa: E402, F401
from ingestion.converters.models import json_converter  # noqa: E402, F401
from ingestion.converters.models import xlsx_converter  # noqa: E402, F401
from ingestion.converters.models import mdb_converter  # noqa: E402, F401
