import importlib.util
import sys
from types import ModuleType
from pathlib import Path

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
EXECUTION_PATH = REPO_ROOT / "airflow_lappis/dags/openmetadata/execution.py"


def _load_execution_module():
    spec = importlib.util.spec_from_file_location(
        "openmetadata_execution_under_test", EXECUTION_PATH
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeTable:
    pass


class FakeDatabase:
    pass


class FakeMetadataClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def list_all_entities(
        self,
        entity: type,
        fields: list[str] | None = None,
        limit: int = 100,
        params: dict | None = None,
    ) -> list:
        self.calls.append(
            {
                "entity": entity,
                "fields": fields,
                "limit": limit,
                "params": params,
            }
        )
        return []


def test_table_list_calls_use_bounded_default_page_size() -> None:
    execution = _load_execution_module()
    client = FakeMetadataClient()

    execution.set_entity_list_page_size(client, FakeTable, page_size=20)
    client.list_all_entities(entity=FakeTable, fields=["columns"])

    assert client.calls == [
        {
            "entity": FakeTable,
            "fields": ["columns"],
            "limit": 20,
            "params": None,
        }
    ]


def test_pagination_preserves_other_entities_and_explicit_limits() -> None:
    execution = _load_execution_module()
    client = FakeMetadataClient()

    execution.set_entity_list_page_size(client, FakeTable, page_size=20)
    client.list_all_entities(entity=FakeDatabase)
    client.list_all_entities(entity=FakeTable, limit=5)

    assert [call["limit"] for call in client.calls] == [100, 5]


def test_pagination_rejects_invalid_page_size() -> None:
    execution = _load_execution_module()

    with pytest.raises(ValueError, match="maior que zero"):
        execution.set_entity_list_page_size(
            FakeMetadataClient(), FakeTable, page_size=0
        )


def test_profile_command_runs_in_process(monkeypatch, tmp_path: Path) -> None:
    execution = _load_execution_module()
    recipe = {
        "source": {"type": "postgres"},
        "workflowConfig": {"raiseOnError": True},
    }
    recipe_path = tmp_path / "profiler.yaml"
    recipe_path.write_text(yaml.safe_dump(recipe), encoding="utf-8")
    received_configs: list[dict] = []

    monkeypatch.setattr(
        execution,
        "execute_profiler_workflow_in_process",
        received_configs.append,
    )

    execution.execute_metadata("profile", recipe_path)

    assert received_configs == [recipe]


def test_classifier_command_runs_in_process(monkeypatch, tmp_path: Path) -> None:
    execution = _load_execution_module()
    recipe = {
        "source": {"type": "postgres"},
        "workflowConfig": {"raiseOnError": True},
    }
    recipe_path = tmp_path / "classifier.yaml"
    recipe_path.write_text(yaml.safe_dump(recipe), encoding="utf-8")
    received_configs: list[dict] = []

    monkeypatch.setattr(
        execution,
        "execute_classifier_workflow_in_process",
        received_configs.append,
    )

    execution.execute_metadata("classify", recipe_path)

    assert received_configs == [recipe]


def test_table_workflow_installs_pagination_before_execution(monkeypatch) -> None:
    execution = _load_execution_module()
    client = FakeMetadataClient()
    workflow_config = type("FakeWorkflowConfig", (), {"successThreshold": 80})()
    workflow = type(
        "FakeWorkflowInstance",
        (),
        {"metadata": client, "workflow_config": workflow_config},
    )()

    class FakeWorkflowClass:
        @classmethod
        def create(cls, workflow_config: dict):
            assert workflow_config == {"source": {"type": "postgres"}}
            return workflow

    class FakeSdkTable:
        pass

    execute_calls: list[tuple[object, dict]] = []

    def fake_execute_workflow(*, workflow: object, config_dict: dict) -> None:
        workflow.metadata.list_all_entities(entity=FakeSdkTable, fields=["columns"])
        execute_calls.append((workflow, config_dict))

    fake_modules = {
        "metadata": ModuleType("metadata"),
        "metadata.cli": ModuleType("metadata.cli"),
        "metadata.cli.common": ModuleType("metadata.cli.common"),
        "metadata.generated": ModuleType("metadata.generated"),
        "metadata.generated.schema": ModuleType("metadata.generated.schema"),
        "metadata.generated.schema.entity": ModuleType("metadata.generated.schema.entity"),
        "metadata.generated.schema.entity.data": ModuleType(
            "metadata.generated.schema.entity.data"
        ),
        "metadata.generated.schema.entity.data.table": ModuleType(
            "metadata.generated.schema.entity.data.table"
        ),
    }
    fake_modules["metadata.cli.common"].execute_workflow = fake_execute_workflow
    fake_modules["metadata.generated.schema.entity.data.table"].Table = FakeSdkTable

    for module_name, module in fake_modules.items():
        monkeypatch.setitem(sys.modules, module_name, module)

    config = {"source": {"type": "postgres"}}
    execution.execute_table_workflow_in_process(
        workflow_config=config,
        workflow_class=FakeWorkflowClass,
        workflow_name="AutoClassificationWorkflow",
    )

    assert execute_calls == [(workflow, config)]
    assert client.calls[0]["entity"] is FakeSdkTable
    assert client.calls[0]["limit"] == 20
    assert workflow.workflow_config.successThreshold == 100


def test_regular_ingestion_keeps_cli_execution(monkeypatch, tmp_path: Path) -> None:
    execution = _load_execution_module()
    recipe = {"source": {"type": "postgres"}}
    recipe_path = tmp_path / "metadata.yaml"
    recipe_path.write_text(yaml.safe_dump(recipe), encoding="utf-8")
    subprocess_calls: list[tuple[list[str], dict[str, object]]] = []

    def record_subprocess_call(command: list[str], **kwargs: object) -> None:
        subprocess_calls.append((command, kwargs))

    monkeypatch.setattr(execution.subprocess, "run", record_subprocess_call)

    execution.execute_metadata("ingest", recipe_path)

    assert len(subprocess_calls) == 1
    command, kwargs = subprocess_calls[0]
    assert command[1:] == ["ingest", "-c", str(recipe_path)]
    assert kwargs["check"] is True
