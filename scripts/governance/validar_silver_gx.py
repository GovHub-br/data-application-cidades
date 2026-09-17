#!/usr/bin/env python3
"""Executa contratos Silver no Great Expectations sem expor dados.

Os contratos continuam declarados no YAML do dbt, no teste
``silver_contract``. Este adaptador traduz as regras compatíveis para GX e as
executa diretamente nas tabelas PostgreSQL. O relatório persistido contém só
o modelo, a regra e o resultado; não contém linhas inesperadas, chaves ou
valores de negócio.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
from pathlib import Path
from urllib.parse import quote_plus

# O progresso interno do GX é ruidoso em execução de DAG e não agrega
# diagnóstico seguro. O relatório resumido abaixo é a única saída persistida.
os.environ.setdefault("TQDM_DISABLE", "1")

import great_expectations as gx
import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT = ROOT / "dbt" / "mcid"
MODELS_ROOT = DBT_PROJECT / "models"
DEFAULT_MANIFEST = DBT_PROJECT / "target" / "manifest.json"
DEFAULT_REPORT = ROOT / "build" / "quality" / "silver_gx.json"
RESULT_FORMAT = {
    "result_format": "BASIC",
    "partial_unexpected_count": 0,
    "include_unexpected_rows": False,
}


def load_yaml(path: Path) -> dict:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else {}


def contract_arguments(model: dict) -> dict | None:
    for test in model.get("data_tests", []) or []:
        if not isinstance(test, dict) or "silver_contract" not in test:
            continue
        config = test["silver_contract"] or {}
        return config.get("arguments", {}) or {}
    return None


def load_contracts() -> dict[str, dict]:
    contracts: dict[str, dict] = {}
    for path in MODELS_ROOT.rglob("*.yml"):
        document = load_yaml(path)
        for model in document.get("models", []) or []:
            if arguments := contract_arguments(model):
                contracts[str(model["name"])] = arguments
    return contracts


def connection_string() -> str:
    required = (
        "DB_DW_USER_MCID",
        "DB_DW_PASSWORD_MCID",
        "DB_DW_HOST_MCID",
        "DB_DW_PORT_MCID",
        "DB_DW_DBNAME_MCID",
    )
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError("Variáveis de banco ausentes para GX: " + ", ".join(missing))
    return (
        "postgresql+psycopg2://"
        f"{quote_plus(os.environ['DB_DW_USER_MCID'])}:"
        f"{quote_plus(os.environ['DB_DW_PASSWORD_MCID'])}@"
        f"{os.environ['DB_DW_HOST_MCID']}:{os.environ['DB_DW_PORT_MCID']}/"
        f"{os.environ['DB_DW_DBNAME_MCID']}"
    )


def expectations(arguments: dict) -> list[object]:
    # Todo modelo Silver deve ser uma relação materializada e não vazia. As
    # regras seguintes vêm do contrato específico quando ele já foi declarado.
    rules: list[object] = [
        gx.expectations.ExpectTableColumnCountToBeBetween(
            min_value=1, result_format=RESULT_FORMAT
        ),
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=1, result_format=RESULT_FORMAT
        ),
    ]
    columns = arguments.get("expected_columns", [])
    if columns:
        rules.append(
            gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=columns,
                exact_match=not arguments.get("allow_additional_columns", True),
                result_format=RESULT_FORMAT,
            )
        )
    for column in arguments.get("not_null_columns", []):
        rules.append(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column, result_format=RESULT_FORMAT
            )
        )
    unique_key = arguments.get("unique_key", [])
    if len(unique_key) == 1:
        rules.append(
            gx.expectations.ExpectColumnValuesToBeUnique(
                column=unique_key[0], result_format=RESULT_FORMAT
            )
        )
    elif len(unique_key) > 1:
        rules.append(
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=unique_key, result_format=RESULT_FORMAT
            )
        )
    for column, threshold in (arguments.get("min_completeness", {}) or {}).items():
        rules.append(
            gx.expectations.ExpectColumnProportionOfNonNullValuesToBeBetween(
                column=column,
                min_value=float(threshold),
                max_value=1.0,
                result_format=RESULT_FORMAT,
            )
        )
    for column, values in (arguments.get("accepted_values", {}) or {}).items():
        rules.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column=column, value_set=values, result_format=RESULT_FORMAT
            )
        )
    for column, pattern in (arguments.get("value_patterns", {}) or {}).items():
        rules.append(
            gx.expectations.ExpectColumnValuesToMatchRegex(
                column=column, regex=pattern, result_format=RESULT_FORMAT
            )
        )
    for column, bounds in (arguments.get("numeric_ranges", {}) or {}).items():
        rules.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=column,
                min_value=bounds.get("min"),
                max_value=bounds.get("max"),
                result_format=RESULT_FORMAT,
            )
        )
    return rules


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--select", help="Nomes de modelos separados por vírgula.")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env", override=False)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    selected = set(args.select.split(",")) if args.select else None
    contracts = load_contracts()
    nodes = {
        node["name"]: node
        for node in manifest.get("nodes", {}).values()
        if node.get("resource_type") == "model"
        and node.get("meta", {}).get("governance", {}).get("layer") == "silver"
        and (selected is None or node.get("name") in selected)
    }
    if not nodes:
        raise RuntimeError("Nenhum contrato Silver selecionado no manifest dbt.")

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_postgres(
        name="mcid_silver_ephemeral", connection_string=connection_string()
    )
    checks: list[dict[str, object]] = []
    for model_name, node in sorted(nodes.items()):
        asset = datasource.add_table_asset(
            name=model_name,
            table_name=node.get("alias") or node["name"],
            schema_name=node["schema"],
        )
        batch = asset.add_batch_definition_whole_table("whole_table").get_batch()
        for expectation in expectations(contracts.get(model_name, {})):
            # GX escreve barras de progresso no terminal. Elas não são
            # diagnóstico persistível e podem confundir o log do Airflow.
            with (
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                result = batch.validate(expectation)
            checks.append(
                {
                    "model": model_name,
                    "expectation": type(expectation).__name__,
                    "success": bool(result.success),
                }
            )

    payload = {
        "version": 1,
        "models": sorted(nodes),
        "checks": checks,
        "passed": all(check["success"] for check in checks),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    failures = sum(not check["success"] for check in checks)
    print(
        f"GX Silver: {len(checks)} verificações, {failures} falhas. Relatório: {args.output}"
    )
    return 1 if args.strict and failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
