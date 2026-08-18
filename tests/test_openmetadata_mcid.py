import ast
import importlib.util
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
MCID_MODELS_DIR = REPO_ROOT / "airflow_lappis/dags/dbt/mcid/models"
OPENMETADATA_DIR = REPO_ROOT / "airflow_lappis/dags/openmetadata"
DOCUMENTED_MODEL_DIRS = (
    MCID_MODELS_DIR / "conjuntura_dbt",
    MCID_MODELS_DIR / "empreendimento_far_dbt",
    MCID_MODELS_DIR / "entidades_dbt",
)
RECIPE_NAMES = (
    "postgres_metadata.yaml",
    "postgres_profiler.yaml",
    "postgres_classifier.yaml",
)
METADATA_RECIPE_NAMES = (
    "airflow_metadata.yaml",
    "dbt_metadata.yaml",
    *RECIPE_NAMES,
    "superset_metadata.yaml",
)


def _load_glossary_module():
    module_path = OPENMETADATA_DIR / "glossary.py"
    spec = importlib.util.spec_from_file_location("mcid_glossary_sync", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _documented_models() -> list[dict]:
    models = []
    for model_dir in DOCUMENTED_MODEL_DIRS:
        for schema_path in sorted(model_dir.glob("*/schema.yml")):
            schema = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
            assert schema["version"] == 2
            models.extend(schema["models"])
    return models


def _recipe_source_configs() -> list[dict]:
    configs = []
    for recipe_name in RECIPE_NAMES:
        recipe_path = OPENMETADATA_DIR / "recipes" / recipe_name
        recipe = yaml.safe_load(recipe_path.read_text(encoding="utf-8"))
        configs.append(recipe["source"]["sourceConfig"]["config"])
    return configs


def _unanchor(pattern: str) -> str:
    assert pattern.startswith("^") and pattern.endswith("$")
    return pattern[1:-1]


def test_mcid_glossary_definition_is_valid() -> None:
    glossary = _load_glossary_module()
    definition_path = OPENMETADATA_DIR / "glossaries/mcid.yaml"

    definition, terms = glossary.load_glossary(str(definition_path))
    summary = glossary.sync_glossary(
        str(definition_path),
        "http://openmetadata:8585/api",
        "",
        dry_run=True,
    )

    assert definition["name"] == "MCID"
    assert summary["terms"] == len(terms)
    assert len(terms) == 62


def test_documented_models_reference_existing_glossary_terms() -> None:
    glossary = _load_glossary_module()
    _, terms = glossary.load_glossary(
        str(OPENMETADATA_DIR / "glossaries/mcid.yaml")
    )
    valid_fqns = {term["fullyQualifiedName"] for term in terms}

    models = _documented_models()
    assert len(models) == 74

    column_count = 0
    for model in models:
        assert len(" ".join(model["description"].split())) >= 80
        assert "mcid" in model["config"]["tags"]

        openmetadata = model["meta"]["openmetadata"]
        assert openmetadata["tier"].startswith("Tier.Tier")
        assert openmetadata["tags"]
        assert openmetadata["glossary"]
        assert set(openmetadata["glossary"]) <= valid_fqns

        for column in model["columns"]:
            column_count += 1
            assert column["description"].strip()
            column_terms = (
                column.get("meta", {})
                .get("openmetadata", {})
                .get("glossary", [])
            )
            assert set(column_terms) <= valid_fqns

    assert column_count == 1215


def test_postgres_recipes_share_the_documented_model_allowlist() -> None:
    configs = _recipe_source_configs()
    expected_schemas = {
        "conjuntura_bronze",
        "conjuntura_gold",
        "conjuntura_silver",
        "empreendimento_far",
        "entidades_fds",
    }
    expected_tables = {model["name"] for model in _documented_models()}

    reference_schema_patterns = configs[0]["schemaFilterPattern"]["includes"]
    reference_table_patterns = configs[0]["tableFilterPattern"]["includes"]

    assert {_unanchor(item) for item in reference_schema_patterns} == expected_schemas
    assert {_unanchor(item) for item in reference_table_patterns} == expected_tables
    assert "models_metadata" not in expected_tables

    for config in configs[1:]:
        assert config["schemaFilterPattern"]["includes"] == reference_schema_patterns
        assert config["tableFilterPattern"]["includes"] == reference_table_patterns


def test_metadata_rest_sinks_use_bounded_batches() -> None:
    for recipe_name in METADATA_RECIPE_NAMES:
        recipe_path = OPENMETADATA_DIR / "recipes" / recipe_name
        recipe = yaml.safe_load(recipe_path.read_text(encoding="utf-8"))
        sink = recipe["sink"]

        assert sink["type"] == "metadata-rest"
        assert sink["config"]["bulk_sink_batch_size"] == 10


def test_classifier_does_not_persist_raw_sample_rows() -> None:
    recipe_path = OPENMETADATA_DIR / "recipes/postgres_classifier.yaml"
    recipe = yaml.safe_load(recipe_path.read_text(encoding="utf-8"))
    config = recipe["source"]["sourceConfig"]["config"]

    assert config["enableAutoClassification"] is True
    assert config["storeSampleData"] is False
    assert config["sampleDataCount"] <= 50


def test_virtualenv_tasks_do_not_load_worker_plugins() -> None:
    dag_path = OPENMETADATA_DIR / "openmetadata_ingestion_dag.py"
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))
    virtualenv_decorators = [
        decorator
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr == "virtualenv"
    ]

    assert len(virtualenv_decorators) == 4
    for decorator in virtualenv_decorators:
        expect_airflow = next(
            (
                keyword.value
                for keyword in decorator.keywords
                if keyword.arg == "expect_airflow"
            ),
            None,
        )
        assert isinstance(expect_airflow, ast.Constant)
        assert expect_airflow.value is False
