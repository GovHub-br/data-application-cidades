"""O `fonte_lake` real: fonte legada e modos de carga sobre a staging particionada."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tests.ingestion.dbt.conftest import DbtProject

FIRST = datetime(2026, 10, 1, 9, tzinfo=timezone.utc)
SECOND = datetime(2026, 10, 8, 9, tzinfo=timezone.utc)

IPCA_1 = b'[{"data":"01/07/2026","valor":"0.26"},{"data":"01/08/2026","valor":"-0.32"}]'
SELIC_1 = b'[{"data":"01/08/2026","valor":"13.75"}]'
# Segunda ingestão: agosto revisado, julho sumiu da fonte, setembro novo, sem Selic.
IPCA_2 = b'[{"data":"01/08/2026","valor":"-0.30"},{"data":"01/09/2026","valor":"0.48"}]'


def _bacen(dbt_project: DbtProject, **meta: object) -> None:
    dbt_project.source("bacen_sgs", caminho="staging/bacen/sgs", **meta)
    dbt_project.model("bronze_bacen_sgs", "select * from {{ fonte_lake('bacen_sgs') }}")
    dbt_project.ingest(
        "bacen", "sgs", FIRST, {"ipca.json": IPCA_1, "selic.json": SELIC_1}
    )
    dbt_project.ingest("bacen", "sgs", SECOND, {"ipca.json": IPCA_2})
    dbt_project.build()


def _rows(dbt_project: DbtProject) -> list[tuple[str, str, str, str]]:
    """(pasta da ingestão, série, data, valor) de cada linha do bronze."""
    rows = dbt_project.query("select data, valor, filename from bronze_bacen_sgs")
    return sorted((Path(f).parts[-2], Path(f).stem, d, v) for d, v, f in rows)


def _compile_error(dbt_project: DbtProject, **meta: object) -> str:
    dbt_project.source("bacen_sgs", caminho="staging/bacen/sgs", **meta)
    dbt_project.model("bronze_bacen_sgs", "select * from {{ fonte_lake('bacen_sgs') }}")
    result = dbt_project.dbt("compile", "--select", "bronze_bacen_sgs")
    assert not result.success
    return str(result.exception or result.result)


def test_legacy_source_compiles_to_the_s3_lake_as_before(dbt_project: DbtProject) -> None:
    dbt_project.source("fgv_incc_m", caminho="staging/fgv/incc_m.parquet")
    dbt_project.model("bronze_fgv_incc_m", "select * from {{ fonte_lake('fgv_incc_m') }}")

    sql = dbt_project.compiled("bronze_fgv_incc_m")

    assert (
        "read_parquet(\n            's3://data-lake-mcid/staging/fgv/incc_m.parquet'"
        in sql
    )


def test_lake_root_points_the_legacy_source_to_a_local_lake(
    dbt_project: DbtProject,
) -> None:
    target = dbt_project.lake / "staging" / "fgv" / "incc_m.parquet"
    target.parent.mkdir(parents=True)
    pq.write_table(pa.table({"mes": ["jan/26"], "indice": ["1234.5"]}), target)
    dbt_project.source("fgv_incc_m", caminho="staging/fgv/incc_m.parquet")
    dbt_project.model("bronze_fgv_incc_m", "select * from {{ fonte_lake('fgv_incc_m') }}")

    dbt_project.build()

    assert dbt_project.query("select * from bronze_fgv_incc_m") == [("jan/26", "1234.5")]


def test_overwrite_reads_only_the_last_ingestion(dbt_project: DbtProject) -> None:
    _bacen(dbt_project, load_mode="overwrite")

    assert _rows(dbt_project) == [
        ("latest", "ipca", "01/08/2026", "-0.30"),
        ("latest", "ipca", "01/09/2026", "0.48"),
    ]
    assert dbt_project.types("bronze_bacen_sgs") == {
        "data": "VARCHAR",
        "valor": "VARCHAR",
        "filename": "VARCHAR",
    }


def test_unknown_load_mode_fails_at_compile_time(dbt_project: DbtProject) -> None:
    assert "load_mode desconhecido: 'upsert'" in _compile_error(
        dbt_project, load_mode="upsert"
    )


def test_append_stacks_every_ingestion_and_ignores_latest(dbt_project: DbtProject) -> None:
    _bacen(dbt_project, load_mode="append")

    assert _rows(dbt_project) == [
        ("060000", "ipca", "01/07/2026", "0.26"),
        ("060000", "ipca", "01/08/2026", "-0.30"),
        ("060000", "ipca", "01/08/2026", "-0.32"),
        ("060000", "ipca", "01/09/2026", "0.48"),
        ("060000", "selic", "01/08/2026", "13.75"),
    ]
