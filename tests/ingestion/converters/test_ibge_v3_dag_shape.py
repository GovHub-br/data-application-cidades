"""IBGE v3 (agregados): o formato declarado na DAG sobre o JsonConverter."""

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.converters import ConversionError, ConverterFactory
from tests.ingestion.dags.conftest import load_dag_module

IBGE_V3 = load_dag_module("data_ingest/ibge/ibge_ingest_dag.py").IBGE_V3

PAYLOAD = [
    {
        "id": "6564",
        "variavel": "Taxa trimestral",
        "unidade": "%",
        "resultados": [
            {
                "classificacoes": [
                    {
                        "id": "11255",
                        "nome": "Setores e subsetores",
                        "categoria": {"90694": "Construção"},
                    }
                ],
                "series": [
                    {
                        "localidade": {"id": "1", "nome": "Brasil"},
                        "serie": {"202501": "1.3", "202502": "..."},
                    }
                ],
            }
        ],
    },
    {
        "id": "48",
        "variavel": "Custo médio m²",
        "unidade": "Moeda corrente",
        "resultados": [
            {
                "classificacoes": [],
                "series": [
                    {
                        "localidade": {"id": "1", "nome": "Brasil"},
                        "serie": {"202608": "1891.63"},
                    }
                ],
            }
        ],
    },
]


def _rows(tmp_path: Path, payload: Any) -> list[dict[str, Any]]:
    path = tmp_path / "pib_construcao.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    converter = ConverterFactory.for_file(path, IBGE_V3)
    [converted] = converter.convert(path, tmp_path / "out")
    table = pq.read_table(converted.path)
    assert all(field.type == pa.string() for field in table.schema)
    rows: list[dict[str, Any]] = table.to_pylist()
    return rows


def test_one_row_per_value_with_the_context_of_variable_and_category(
    tmp_path: Path,
) -> None:
    rows = _rows(tmp_path, PAYLOAD)

    assert rows[0] == {
        "variavel_id": "6564",
        "variavel_nome": "Taxa trimestral",
        "localidade_id": "1",
        "localidade_nome": "Brasil",
        "classificacao_id": "11255",
        "classificacao_nome": "Setores e subsetores",
        "categoria_id": "90694",
        "categoria_nome": "Construção",
        "unidade": "%",
        "periodo": "202501",
        "valor": "1.3",
    }
    # Marcador de ausente fica como veio: tipar é da prata.
    assert rows[1]["periodo"] == "202502" and rows[1]["valor"] == "..."


def test_without_classification_ids_are_zero_as_in_the_old_flattening(
    tmp_path: Path,
) -> None:
    rows = _rows(tmp_path, PAYLOAD)

    assert rows[2] == {
        "variavel_id": "48",
        "variavel_nome": "Custo médio m²",
        "localidade_id": "1",
        "localidade_nome": "Brasil",
        "classificacao_id": "0",
        "classificacao_nome": "",
        "categoria_id": "0",
        "categoria_nome": "",
        "unidade": "Moeda corrente",
        "periodo": "202608",
        "valor": "1891.63",
    }


def test_two_classifications_are_joined_like_the_old_flattening(
    tmp_path: Path,
) -> None:
    payload = [dict(PAYLOAD[0])]
    payload[0]["resultados"] = [
        {
            "classificacoes": [
                {"id": "1", "nome": "A", "categoria": {"10": "a"}},
                {"id": "2", "nome": "B", "categoria": {"20": "b"}},
            ],
            "series": [
                {"localidade": {"id": "1", "nome": "Brasil"}, "serie": {"2024": "7"}}
            ],
        }
    ]

    [row] = _rows(tmp_path, payload)

    assert (row["classificacao_id"], row["classificacao_nome"]) == ("1|2", "A | B")
    assert (row["categoria_id"], row["categoria_nome"]) == ("10|20", "a | b")


def test_empty_response_is_a_conversion_error(tmp_path: Path) -> None:
    with pytest.raises(ConversionError, match="nenhum registro"):
        _rows(tmp_path, [])
