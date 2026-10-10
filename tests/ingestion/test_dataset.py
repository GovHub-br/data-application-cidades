"""DatasetSpec: a configuração literal de um dataset, declarada no topo da DAG."""

import dataclasses

import pytest

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.loaders import LoadMode

API = ExtractorConfig(
    source="api", conn_id="http_bacen", requests=(HttpRequest(name="a", endpoint="/a"),)
)


def test_defaults_to_overwrite_without_keys() -> None:
    spec = DatasetSpec(domain="bacen", dataset="sgs", extractor=API)

    assert (spec.load_mode, spec.keys) == (LoadMode.OVERWRITE, ())
    assert spec.converter == ConverterConfig()


def test_load_mode_and_keys_are_validated() -> None:
    spec = DatasetSpec(
        domain="bacen", dataset="sgs", extractor=API, load_mode="merge", keys=["data"]
    )

    assert (spec.load_mode, spec.keys) == (LoadMode.MERGE, ("data",))
    with pytest.raises(ValueError, match="merge exige keys"):
        DatasetSpec(domain="bacen", dataset="sgs", extractor=API, load_mode="merge")


@pytest.mark.parametrize("name", ["", "fgv/incc", "série"])
def test_domain_and_dataset_must_already_be_safe_path_segments(name: str) -> None:
    with pytest.raises(ValueError, match="segmento"):
        DatasetSpec(domain=name, dataset="sgs", extractor=API)


def test_lazy_extractor_runs_only_when_resolved() -> None:
    calls: list[str] = []

    def from_variable() -> ExtractorConfig:
        calls.append("Variable.get")  # na DAG real, lê a Variable dentro da task
        return API

    spec = DatasetSpec(domain="bacen", dataset="sgs", extractor=from_variable)

    assert calls == []  # parse da DAG: nada lido
    assert spec.extractor_config() is API
    assert calls == ["Variable.get"]


def test_literal_extractor_is_returned_as_is() -> None:
    assert (
        DatasetSpec(domain="bacen", dataset="sgs", extractor=API).extractor_config()
        is API
    )


def test_spec_is_frozen() -> None:
    spec = DatasetSpec(domain="bacen", dataset="sgs", extractor=API)

    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.domain = "outro"  # type: ignore[misc]
