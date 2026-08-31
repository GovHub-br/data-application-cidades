#!/usr/bin/env python3
"""Produz inventário seguro de materialização e estratégia de carga dbt."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = ROOT / "dbt" / "mcid" / "target" / "manifest.json"
DEFAULT_OUTPUT = ROOT / "docs-conjuntura" / "quality" / "load_strategies.json"


def strategy(materialized: str) -> str:
    return {
        "incremental": "incremental_com_full_refresh_opcional",
        "table": "reconstrucao_integral_por_execucao",
        "view": "avaliacao_sob_demanda",
        "materialized_view": "materializacao_banco",
        "ephemeral": "nao_persistido",
    }.get(materialized, "materializacao_nao_classificada")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    rows = []
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        governance = node.get("meta", {}).get("governance", {})
        materialized = node.get("config", {}).get("materialized", "unknown")
        rows.append(
            {
                "model": node["name"],
                "product": governance.get("product"),
                "layer": governance.get("layer"),
                "materialized": materialized,
                "load_strategy": strategy(materialized),
            }
        )
    rows.sort(key=lambda row: (str(row["product"]), str(row["layer"]), row["model"]))
    payload = {"version": 1, "summary": Counter(row["materialized"] for row in rows), "models": rows}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=dict), encoding="utf-8")
    unknown = sum(row["load_strategy"] == "materializacao_nao_classificada" for row in rows)
    print(f"Estratégias de carga: {len(rows)} modelos, {unknown} não classificadas.")
    return 1 if args.strict and unknown else 0


if __name__ == "__main__":
    raise SystemExit(main())
