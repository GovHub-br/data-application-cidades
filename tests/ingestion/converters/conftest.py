"""Amostras de cada formato, que alimentam o contrato dos conversores."""

import io
import json
import sys
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from ingestion.converters import ConverterConfig


@dataclass
class ContractCase:
    """Um arquivo de amostra, a configuração para lê-lo e quantas linhas tem.

    `text_source` é falso só para formatos que já chegam tipados (parquet).
    """

    path: Path
    config: ConverterConfig
    rows: int
    text_source: bool = True
    outputs: list[str] = field(default_factory=list)


def _csv_case(tmp_path: Path, request: pytest.FixtureRequest) -> ContractCase:
    path = tmp_path / "serie.csv"
    path.write_text("mes,valor\n01/2026,1.5\n02/2026,007\n", encoding="utf-8")
    return ContractCase(path, ConverterConfig(), rows=2, outputs=["serie.parquet"])


def _txt_case(tmp_path: Path, request: pytest.FixtureRequest) -> ContractCase:
    path = tmp_path / "base pf.txt"
    path.write_bytes("mês|valor\nago|1\n".encode("latin-1"))
    config = ConverterConfig(delimiter="|", encoding="latin-1")
    return ContractCase(path, config, rows=1, outputs=["base_pf.parquet"])


def _json_case(tmp_path: Path, request: pytest.FixtureRequest) -> ContractCase:
    path = tmp_path / "ipca.json"
    path.write_text(
        '[{"data":"01/07/2026","valor":"0.26"},{"data":"01/08/2026","valor":"-0.32"}]'
    )
    return ContractCase(path, ConverterConfig(), rows=2, outputs=["ipca.parquet"])


def _xlsx_case(tmp_path: Path, request: pytest.FixtureRequest) -> ContractCase:
    from openpyxl import Workbook

    workbook = Workbook()
    workbook.remove(workbook.worksheets[0])
    sheet = workbook.create_sheet("INCC-M")
    sheet.append(["Mês", "Índice", "Variação"])
    sheet.append(["jan/26", 1234.5, 0.42])
    sheet.append(["fev/26", 1240, None])
    path = tmp_path / "incc_m.xlsx"
    workbook.save(path)
    return ContractCase(path, ConverterConfig(), rows=2, outputs=["incc_m.parquet"])


FAKE_MDBTOOLS = {
    "mdb-tables": """
import json, sys
print("\\n".join(json.load(open(sys.argv[-1]))))
""",
    "mdb-export": """
import json, sys
tables = json.load(open(sys.argv[-2]))
if sys.argv[-1] not in tables:
    sys.exit(f"tabela inexistente: {sys.argv[-1]}")
sys.stdout.write(tables[sys.argv[-1]])
""",
}


@pytest.fixture
def fake_mdbtools(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """mdb-tables/mdb-export de mentira: o ".mdb" é um json {tabela: csv}."""
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir()
    for name, body in FAKE_MDBTOOLS.items():
        script = bin_dir / name
        script.write_text(f"#!{sys.executable}\n{body}")
        script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{__import__('os').environ['PATH']}")
    return bin_dir


def fake_mdb(path: Path, tables: dict[str, str]) -> Path:
    path.write_text(json.dumps(tables))
    return path


def _mdb_case(tmp_path: Path, request: pytest.FixtureRequest) -> ContractCase:
    request.getfixturevalue("fake_mdbtools")
    path = fake_mdb(
        tmp_path / "MCidades_AO_1.mdb",
        {
            "contratos": 'apf,valor\n"0001","1500.00"\n"0002",\n',
            "obras": "apf,situacao\n0001,CONCLUIDA\n",
        },
    )
    return ContractCase(
        path,
        ConverterConfig(),
        rows=3,
        outputs=["MCidades_AO_1__contratos.parquet", "MCidades_AO_1__obras.parquet"],
    )


def _parquet_case(tmp_path: Path, request: pytest.FixtureRequest) -> ContractCase:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "historico.parquet"
    pq.write_table(pa.table({"ano": [2025, 2026], "valor": [1.5, None]}), path)
    return ContractCase(
        path, ConverterConfig(), rows=2, text_source=False, outputs=["historico.parquet"]
    )


def zip_of(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def _zip_case(tmp_path: Path, request: pytest.FixtureRequest) -> ContractCase:
    path = tmp_path / "dotacao_execucao.zip"
    path.write_bytes(
        zip_of({"dotacao.csv": b"a,b\n1,2\n3,4\n", "leia-me.csv": b"x\ny\n"})
    )
    return ContractCase(
        path,
        ConverterConfig(),
        rows=3,
        outputs=[
            "dotacao_execucao__dotacao.parquet",
            "dotacao_execucao__leia-me.parquet",
        ],
    )


CASES: dict[str, Callable[[Path, pytest.FixtureRequest], ContractCase]] = {
    "csv": _csv_case,
    "txt": _txt_case,
    "json": _json_case,
    "xlsx": _xlsx_case,
    "mdb": _mdb_case,
    "parquet": _parquet_case,
    "zip": _zip_case,
}


@pytest.fixture
def contract_case(request: pytest.FixtureRequest, tmp_path: Path) -> ContractCase:
    return CASES[request.param](tmp_path, request)
