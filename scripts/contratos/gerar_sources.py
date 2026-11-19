"""Generate the dbt sources of each contract (ADR-0006: the contract is the source of truth).

`contracts/<source>.odcs.yaml` -> `dbt/mcid/models/fontes/<source>.yml`. The generated
file is never edited by hand; `tests/test_contratos.py` fails when it is out of date.

    uv run python scripts/contratos/gerar_sources.py
"""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "contracts"
TARGET = ROOT / "dbt" / "mcid" / "models" / "fontes"
HEADER = (
    "# GERADO por scripts/contratos/gerar_sources.py a partir de contracts/{id}.odcs.yaml.\n"
    "# Não edite: mude o contrato e rode o gerador.\n"
)


def custom(item: dict) -> dict:
    return {c["property"]: c["value"] for c in item.get("customProperties") or []}


def source_yaml(contract: dict) -> str:
    bucket = contract["servers"][0]["location"].removeprefix("s3://").split("/")[0]
    owners = [t["username"] for t in contract.get("team") or [] if t.get("role") == "owner"]
    tables = []
    for obj in contract["schema"]:
        props = custom(obj)
        columns = []
        for prop in obj.get("properties") or []:
            column = {"name": prop["name"]}
            if prop.get("classification"):
                column["meta"] = {"classification": prop["classification"]}
            columns.append(column)
        tables.append(
            {
                "name": obj["name"],
                "description": obj.get("description") or "",
                "meta": {  # `caminho` is what the fonte_lake macro reads
                    "caminho": props["path"],
                    "file_pattern": props["file_pattern"],
                    "load_type": props["load_type"],
                    "dag": props.get("dag"),
                },
                "columns": columns,
            }
        )
    doc = {
        "version": 2,
        "sources": [
            {
                "name": contract["id"],
                "description": (contract.get("description") or {}).get("purpose", ""),
                "schema": "staging",
                "meta": {
                    "bucket": bucket,
                    "camada": "staging",
                    "contract": f"contracts/{contract['id']}.odcs.yaml",
                    "owner": owners[0] if owners else None,
                },
                "tables": tables,
            }
        ],
    }
    body = yaml.safe_dump(doc, allow_unicode=True, sort_keys=False, width=100)
    return HEADER.format(id=contract["id"]) + body


def generate() -> dict[Path, str]:
    """Generated file -> content, for every contract."""
    out = {}
    for path in sorted(CONTRACTS.glob("*.odcs.yaml")):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        out[TARGET / f"{contract['id']}.yml"] = source_yaml(contract)
    return out


def main() -> None:
    TARGET.mkdir(parents=True, exist_ok=True)
    for path, content in generate().items():
        path.write_text(content, encoding="utf-8")
        print(f"wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
