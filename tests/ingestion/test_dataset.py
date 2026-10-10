"""DatasetSpec: a configuração literal de um dataset, declarada no topo da DAG."""

import dataclasses
from collections.abc import Mapping
from typing import Any

import pytest

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest, api
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
    calls: list[object] = []

    def from_task(entrada: object) -> ExtractorConfig:
        calls.append(entrada)  # na DAG real, a task passa o que leu da Variable
        return API

    spec = DatasetSpec(domain="bacen", dataset="sgs", extractor=from_task)

    assert calls == []  # parse da DAG: nada lido
    assert spec.resolve().extractor_config() is API
    assert calls == [{}]


def test_a_template_resolves_one_spec_per_entry() -> None:
    # Com Variable, os campos que dependem da entrada são funções do perfil.
    template = DatasetSpec(
        domain="ibge",
        dataset=lambda entrada: entrada["dataset"],
        extractor=api(
            base_url="https://servicodados.ibge.gov.br",
            requests=lambda entrada: (
                HttpRequest(
                    name=entrada["dataset"], endpoint=f"/agregados/{entrada['agregado']}"
                ),
            ),
        ),
        load_mode="merge",
        keys=("periodo",),
    )

    spec = template.resolve({"dataset": "sinapi", "agregado": 2296})

    assert spec.dataset == "sinapi"
    assert spec.extractor_config() == ExtractorConfig(
        source="api",
        base_url="https://servicodados.ibge.gov.br",
        requests=(HttpRequest(name="sinapi", endpoint="/agregados/2296"),),
    )
    assert (spec.load_mode, spec.keys) == (LoadMode.MERGE, ("periodo",))


def test_a_template_needs_an_entry_and_a_safe_name() -> None:
    template = DatasetSpec(domain="ibge", dataset=lambda e: e["dataset"], extractor=API)

    with pytest.raises(ValueError, match="entrada"):
        template.resolve()
    with pytest.raises(ValueError, match="segmento"):
        template.resolve({"dataset": "pib/construcao"})


def test_prepare_can_depend_on_the_entry() -> None:
    template = DatasetSpec(
        domain="sftp",
        dataset=lambda e: e["dataset"],
        extractor=API,
        prepare=lambda e: (e["dataset"],),
    )

    assert template.resolve({"dataset": "int040"}).prepare == ("int040",)


def test_literal_extractor_is_returned_as_is() -> None:
    assert (
        DatasetSpec(domain="bacen", dataset="sgs", extractor=API).extractor_config()
        is API
    )


def test_spec_is_frozen() -> None:
    spec = DatasetSpec(domain="bacen", dataset="sgs", extractor=API)

    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.domain = "outro"  # type: ignore[misc]


def test_the_extractor_of_a_resolved_spec_is_only_built_when_asked() -> None:
    # A conversão resolve o spec mas não precisa do extrator (nem da Variable).
    calls: list[object] = []

    def series(entrada: Mapping[str, Any]) -> ExtractorConfig:
        calls.append(entrada)
        return API

    spec = DatasetSpec(domain="bacen", dataset="sgs", extractor=series).resolve(
        {"ipca": 433}
    )

    assert calls == []
    assert spec.extractor_config() is API
    assert calls == [{"ipca": 433}]
