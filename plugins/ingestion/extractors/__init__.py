"""Extração (Strategy + Factory).

Copia o dado da fonte para disco, no formato original.
"""

from ingestion.extractors.base_extractor import (
    Extractor,
    RawFile,
    describe_file,
    write_stream,
)
from ingestion.extractors.config_extractor import (
    ExtractorConfig,
    FgvDadosQuery,
    HttpRequest,
    MailQuery,
    ObjectQuery,
)
from ingestion.extractors.extractor_errors import ExtractionError, SourceNotFoundError
from ingestion.extractors.extractor_registry import ExtractorFactory

__all__ = [
    "ExtractionError",
    "Extractor",
    "ExtractorConfig",
    "ExtractorFactory",
    "FgvDadosQuery",
    "HttpRequest",
    "MailQuery",
    "ObjectQuery",
    "RawFile",
    "SourceNotFoundError",
    "describe_file",
    "write_stream",
]

# Registra as estratégias no ExtractorFactory (import pelo efeito do decorator).
from ingestion.extractors.models import api_extractor  # noqa: E402, F401
from ingestion.extractors.models import http_file_extractor  # noqa: E402, F401
from ingestion.extractors.models import email_extractor  # noqa: E402, F401
from ingestion.extractors.models import fgvdados_extractor  # noqa: E402, F401
from ingestion.extractors.models import object_storage_extractor  # noqa: E402, F401
