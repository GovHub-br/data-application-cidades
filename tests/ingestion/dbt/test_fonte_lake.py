"""O `fonte_lake` real: fonte legada e modos de carga sobre a staging particionada."""

import pyarrow as pa
import pyarrow.parquet as pq

from tests.ingestion.dbt.conftest import DbtProject


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
