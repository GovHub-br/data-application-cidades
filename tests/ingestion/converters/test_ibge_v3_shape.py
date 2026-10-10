"""O formato da API de agregados do IBGE, declarado na DAG, sobre o JsonConverter.

A resposta real (`fixtures/ibge_v3_pib_construcao.json`, agregado 5932) tem de
dar as mesmas linhas que o conversor dedicado dava (`.expected.json`), que é o
achatamento que o macro `ibge_v3_tipado` e as pratas do IBGE supõem.
"""

import json
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.converters import ConverterFactory
from tests.ingestion.dags.conftest import load_dag_module

FIXTURES = Path(__file__).parent / "fixtures"


def test_dag_shape_reproduces_the_flattening_the_pratas_expect(tmp_path: Path) -> None:
    [spec] = [
        s
        for s in load_dag_module("data_ingest/ibge/ibge_ingest_dag.py").DATASETS
        if s.dataset == "pib_construcao"
    ]
    source = FIXTURES / "ibge_v3_pib_construcao.json"
    expected = json.loads((FIXTURES / "ibge_v3_pib_construcao.expected.json").read_text())

    [converted] = ConverterFactory.for_file(source, spec.converter).convert(
        source, tmp_path
    )
    table = pq.read_table(converted.path)

    assert table.column_names == expected["columns"]
    assert [list(row.values()) for row in table.to_pylist()] == expected["rows"]
