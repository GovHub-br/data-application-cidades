"""Extração (Strategy + Factory).

Copia o dado da fonte para disco, no formato original.
"""

from ingestion.extractors.base_extractor import RawFile, write_stream

__all__ = ["RawFile", "write_stream"]
