import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
OPENMETADATA_DIR = REPO_ROOT / "airflow_lappis/dags/openmetadata"
CATALOG_PATH = OPENMETADATA_DIR / "semantic_relationships/mcid.yaml"
MODELS_DIR = REPO_ROOT / "airflow_lappis/dags/dbt/mcid/models"


def _load_module():
    module_path = OPENMETADATA_DIR / "semantic_relationships.py"
    spec = importlib.util.spec_from_file_location(
        "mcid_semantic_relationships",
        module_path,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_catalog_matches_all_documented_dbt_models_and_columns() -> None:
    semantic = _load_module()

    summary = semantic.validate_catalog_against_dbt(
        str(CATALOG_PATH),
        str(MODELS_DIR),
    )

    assert summary == {
        "tables": 74,
        "columns": 1215,
        "relationships": 27,
        "searchGroups": 14,
        "joinModels": 19,
        "joinClauses": 53,
    }


def test_catalog_dry_run_does_not_require_openmetadata_sdk_or_token() -> None:
    semantic = _load_module()

    summary = semantic.sync_semantic_relationships(
        catalog_path=str(CATALOG_PATH),
        host_port="",
        jwt_token="",
        dry_run=True,
    )

    assert summary["tables"] == 74
    assert summary["relationships"] == 27
    assert summary["searchGroups"] == 14
    assert summary["joinClauses"] == 53
    assert summary["patched"] == 0
    assert summary["unchanged"] == 0


def test_markdown_keeps_lineage_validated_links_and_candidates_separate() -> None:
    semantic = _load_module()
    catalog = semantic.load_semantic_catalog(str(CATALOG_PATH))

    fds_markdown = semantic.render_table_markdown(
        catalog,
        "entidades_fds.fds_obra_mensal",
    )
    cross_program_markdown = semantic.render_table_markdown(
        catalog,
        "empreendimento_far.dados_prioritarios_caixa",
    )
    conjuncture_markdown = semantic.render_table_markdown(
        catalog,
        "conjuntura_gold.gold_indices_mercado_imobiliario",
    )

    assert "não são constraints FK" in fds_markdown
    assert "Contrato validado por teste dbt" in fds_markdown
    assert "fds_cadastro_pj.apf" in fds_markdown
    assert "Relação candidata" in cross_program_markdown
    assert "fds_dados_prioritarios_entregas.apf" in cross_program_markdown
    assert "cd.data_referencia = DATE '2023-12-01'" in conjuncture_markdown


def test_every_scoped_table_has_renderable_openmetadata_content() -> None:
    semantic = _load_module()
    catalog = semantic.load_semantic_catalog(str(CATALOG_PATH))

    table_keys = semantic._scope_table_keys(catalog)
    rendered = {
        table_key: semantic.render_table_markdown(catalog, table_key)
        for table_key in table_keys
    }

    assert len(rendered) == 74
    assert all("Relações semânticas MCID" in text for text in rendered.values())
    assert all("Privacidade e interpretação" in text for text in rendered.values())


def test_patch_is_granular_and_preserves_unrelated_custom_properties() -> None:
    semantic = _load_module()

    class FakeClient:
        def __init__(self) -> None:
            self.path = ""
            self.operations = []

        def patch(self, path: str, data: str):
            self.path = path
            self.operations = json.loads(data)
            return {"id": "table-id"}

    client = FakeClient()
    metadata = SimpleNamespace(
        client=client,
        get_suffix=lambda entity: "tables",
    )
    table = SimpleNamespace(
        id="table-id",
        extension={
            "thirdPartyProperty": "não pode ser removida",
            "mcidSemanticRelationships": "versão anterior",
        },
    )

    semantic._patch_custom_properties_preserving_extension(
        metadata=metadata,
        entity=object,
        table=table,
        custom_properties={
            "mcidSemanticRelationships": "versão nova",
            "mcidRelatedTables": [{"id": "related-id"}],
        },
    )

    assert client.path == "tables/table-id"
    assert client.operations == [
        {
            "op": "replace",
            "path": "/extension/mcidSemanticRelationships",
            "value": "versão nova",
        },
        {
            "op": "add",
            "path": "/extension/mcidRelatedTables",
            "value": [{"id": "related-id"}],
        },
    ]
    assert all(operation["path"] != "/extension" for operation in client.operations)


def test_patch_adds_extension_object_when_table_has_none() -> None:
    semantic = _load_module()

    class FakeClient:
        def patch(self, path: str, data: str):
            self.path = path
            self.operations = json.loads(data)
            return {"id": "table-id"}

    client = FakeClient()
    metadata = SimpleNamespace(
        client=client,
        get_suffix=lambda entity: "tables",
    )
    table = SimpleNamespace(id="table-id", extension=None)

    semantic._patch_custom_properties_preserving_extension(
        metadata=metadata,
        entity=object,
        table=table,
        custom_properties={"mcidSemanticRelationships": "documentação"},
    )

    assert client.operations == [
        {
            "op": "add",
            "path": "/extension",
            "value": {"mcidSemanticRelationships": "documentação"},
        }
    ]
