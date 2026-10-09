"""Projeto dbt temporário para testar o `fonte_lake` real, sem o DW.

Cada teste monta um projeto mínimo com cópias dos macros do lake do repositório
(`fonte_lake`, `lake_dt_ingest`), fontes e modelos próprios, e roda o dbt-duckdb
em processo. A variável `lake_root` aponta o macro para um diretório local no
lugar do MinIO.
"""

import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml
from dbt.cli.main import dbtRunner, dbtRunnerResult

from ingestion.converters import ConverterConfig, convert_partition
from ingestion.extractors import write_stream
from ingestion.layout import ingestion_partition, raw_prefix, staging_prefix
from ingestion.storage import StorageFactory, land

REPO = Path(__file__).resolve().parents[3]
MACROS = [
    REPO / "dbt" / "mcid" / "macros" / name
    for name in (
        "fonte_lake.sql",
        "lake_dt_ingest.sql",
        "normalizar_colunas.sql",
        "arquivo_mais_recente.sql",
    )
]


@dataclass
class DbtProject:
    path: Path
    lake: Path

    @property
    def database(self) -> Path:
        return self.path / "lake_test.duckdb"

    def source(self, name: str, **meta: Any) -> None:
        """Declara uma fonte em `lake_staging` com o `meta` dado."""
        sources_file = self.path / "models" / "sources.yml"
        document = yaml.safe_load(sources_file.read_text())
        document["sources"][0]["tables"].append({"name": name, "meta": meta})
        sources_file.write_text(yaml.safe_dump(document, allow_unicode=True))

    def model(self, name: str, sql: str) -> None:
        (self.path / "models" / f"{name}.sql").write_text(sql)

    def dbt(self, *args: str, lake_root: str | None = None) -> dbtRunnerResult:
        variables = {} if lake_root is None else {"lake_root": lake_root}
        return dbtRunner().invoke(
            [
                *args,
                "--project-dir",
                str(self.path),
                "--profiles-dir",
                str(self.path),
                "--vars",
                json.dumps(variables),
            ]
        )

    def build(self, *select: str) -> dbtRunnerResult:
        args = ["build", *(["--select", *select] if select else [])]
        result = self.dbt(*args, lake_root=str(self.lake))
        assert result.success, result.exception or result.result
        return result

    def compiled(self, model: str) -> str:
        result = self.dbt("compile", "--select", model)
        assert result.success, result.exception
        return next(self.path.rglob(f"compiled/**/{model}.sql")).read_text()

    def ingest(
        self, domain: str, dataset: str, when: datetime, files: dict[str, bytes]
    ) -> None:
        """Uma ingestão de verdade: land na raw, conversão, publicação do latest/."""
        storage = StorageFactory.create("local", root=self.lake)
        partition = ingestion_partition(when)
        raw = raw_prefix(domain, dataset, partition)
        work = self.path.parent / "work"
        land(
            storage, (write_stream([d], work / "raw" / n) for n, d in files.items()), raw
        )
        convert_partition(
            storage,
            raw_prefix=raw,
            staging_prefix=staging_prefix(domain, dataset, partition),
            latest_prefix=f"staging/{domain}/{dataset}/latest/",
            config=ConverterConfig(),
            work_dir=work,
        )

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        # Mesma configuração da conexão que o dbt-duckdb mantém aberta no processo.
        with duckdb.connect(str(self.database)) as connection:
            return connection.sql(sql).fetchall()

    def types(self, table: str) -> dict[str, str]:
        rows = self.query(
            f"select column_name, data_type from information_schema.columns "
            f"where table_name = '{table}' order by ordinal_position"
        )
        return dict(rows)


@pytest.fixture
def dbt_project(tmp_path: Path) -> DbtProject:
    project = tmp_path / "projeto"
    (project / "models").mkdir(parents=True)
    (project / "macros").mkdir()
    for macro in MACROS:
        shutil.copy(macro, project / "macros" / macro.name)
    (project / "dbt_project.yml").write_text(
        yaml.safe_dump(
            {
                "name": "lake_test",
                "version": "1.0.0",
                "config-version": 2,
                "profile": "lake_test",
                "model-paths": ["models"],
                "macro-paths": ["macros"],
                "models": {"lake_test": {"+materialized": "table"}},
            }
        )
    )
    (project / "profiles.yml").write_text(
        yaml.safe_dump(
            {
                "lake_test": {
                    "target": "dev",
                    "outputs": {
                        "dev": {
                            "type": "duckdb",
                            "path": str(project / "lake_test.duckdb"),
                            "threads": 1,
                        }
                    },
                }
            }
        )
    )
    (project / "models" / "sources.yml").write_text(
        yaml.safe_dump(
            {"version": 2, "sources": [{"name": "lake_staging", "tables": []}]}
        )
    )
    lake = tmp_path / "lake"
    lake.mkdir()
    return DbtProject(path=project, lake=lake)
