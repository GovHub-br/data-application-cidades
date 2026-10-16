"""Passos que as DAGs encadeiam: extract_to_raw -> convert_to_staging."""

from ingestion.pipeline.steps import NothingNew

__all__ = ["NothingNew"]
