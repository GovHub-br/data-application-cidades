"""Extração (Strategy + Factory).

Copia o dado da fonte para disco, no formato original.
"""

from ingestion.extractors.base_extractor import Extractor, RawFile, write_stream
from ingestion.extractors.config_extractor import ExtractorConfig, HttpRequest, MailQuery
from ingestion.extractors.extractor_errors import ExtractionError, SourceNotFoundError
from ingestion.extractors.extractor_registry import ExtractorFactory

__all__ = [
    "ExtractionError",
    "Extractor",
    "ExtractorConfig",
    "ExtractorFactory",
    "HttpRequest",
    "MailQuery",
    "RawFile",
    "SourceNotFoundError",
    "write_stream",
]
