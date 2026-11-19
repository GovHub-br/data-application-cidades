"""ODCS contracts of the sources (ADR-0006) and the dbt sources generated from them."""

from pathlib import Path

import pytest
import yaml

from contratos import gerar_sources

CONTRACTS = sorted(gerar_sources.CONTRACTS.glob("*.odcs.yaml"))


def test_generated_sources_are_up_to_date():
    stale = [
        str(path.relative_to(gerar_sources.ROOT))
        for path, content in gerar_sources.generate().items()
        if not path.exists() or path.read_text(encoding="utf-8") != content
    ]
    assert not stale, "Run `uv run python scripts/contratos/gerar_sources.py`: " + ", ".join(stale)


def test_no_generated_source_without_contract():
    generated = {p.name for p in gerar_sources.TARGET.glob("*.yml")}
    assert generated == {f"{yaml.safe_load(c.read_text())['id']}.yml" for c in CONTRACTS}


@pytest.mark.parametrize("path", CONTRACTS, ids=lambda p: Path(p).name)
def test_each_dataset_is_matched_by_one_object(path):
    """A new raw file must match one object: two objects with the same pattern would
    split the same delivery (ADR-0006: one object per dataset)."""
    contract = yaml.safe_load(path.read_text())
    keys = [
        (gerar_sources.custom(o)["file_pattern"], gerar_sources.custom(o)["path"])
        for o in contract["schema"]
    ]
    assert len(keys) == len(set(keys))
    for obj in contract["schema"]:
        props = gerar_sources.custom(obj)
        assert props["path"].startswith("staging/") and props["file_pattern"]
        assert props["load_type"] in ("full", "incremental")


@pytest.mark.parametrize("path", CONTRACTS, ids=lambda p: Path(p).name)
def test_contract_is_valid_odcs(path):
    """Lint against the ODCS JSON Schema (datacontract-cli, dev group)."""
    from datacontract.data_contract import DataContract

    result = DataContract(data_contract_file=str(path)).lint()
    assert result.result == "passed", [c.reason for c in result.checks if c.result != "passed"]
