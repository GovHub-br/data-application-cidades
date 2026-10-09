"""Novo CAGED, construção: admitidos, desligados, saldo, estoque e variação.

Fonte: o painel público do Novo CAGED no Power BI. Não há API: cada mês é uma
consulta POST ao `querydata` do relatório público (a mesma que o navegador faz),
filtrada pelo grande grupamento Construção e, em dois recortes, pela divisão
CNAE 2.0. A resposta vem em DSR (compactada), gravada como veio; o conversor
`powerbi_dsr` a decodifica.

Três DAGs, um recorte cada, com os `dag_id`s que a `conjuntura_dag` dispara:
construção de edifícios, serviços especializados e o total da construção.

LoadMode: overwrite. Cada execução consulta todos os meses desde 2024-01 até o
mês corrente, então a última ingestão é a verdade. Mês ainda não publicado vem
com as medidas nulas e sai na prata. Os meses são montados dentro da task (o mês
corrente muda), nunca no parse.
"""

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.layout import TIMEZONE
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps

FIRST_YEAR = 2024
MESES = (
    "janeiro",
    "fevereiro",
    "março",
    "abril",
    "maio",
    "junho",
    "julho",
    "agosto",
    "setembro",
    "outubro",
    "novembro",
    "dezembro",
)
HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "X-PowerBI-ResourceKey": "5b95b481-bfbc-4287-935e-ce2b20015ab6",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://app.powerbi.com/",
}
DATASET_ID = "4859b5fd-e3ad-4a7c-95fe-aa62fc046d96"
MODEL_ID = 3021080
DATE_TABLE = "LocalDateTable_9b82530a-b08e-43fc-8e3a-39c225627f7d"
MEDIDAS = (
    ("Admitidos", "Admitidos"),
    ("Desligados", "Desligados"),
    ("Saldo", "Saldo"),
    ("Estoque Mensal", "Estoque"),
    ("Vr. Relativa", "Variacao"),
)
# dag_id → (dataset na staging, divisão CNAE 2.0; None = total da construção)
RECORTES = {
    "novo_caged_construcao_edificios": (
        "saldo_estoque_construcao_edificios",
        "Construção de Edifícios",
    ),
    "novo_caged_servicos_especializados_construcao": (
        "saldo_estoque_servicos_especializados_construcao",
        "Serviços Especializados para Construção",
    ),
    "novo_caged_total_construcao": ("saldo_estoque_total_construcao", None),
}


def _in(source: str, prop: str, *values: str) -> dict[str, Any]:
    return {
        "Condition": {
            "In": {
                "Expressions": [
                    {
                        "Column": {
                            "Expression": {"SourceRef": {"Source": source}},
                            "Property": prop,
                        }
                    }
                ],
                "Values": [[{"Literal": {"Value": value}} for value in values]],
            }
        }
    }


def consulta(ano: int, mes: str, cnae_divisao: str | None) -> dict[str, Any]:
    """Corpo do `querydata` para um mês: as cinco medidas, com os filtros."""
    where = [_in("e", "Grande Grupamento", "'Construção'")]
    if cnae_divisao:
        where.append(_in("e", "CNAE 2.0 Divisão", f"'{cnae_divisao}'"))
    period = _in("l", "Ano", f"{ano}L")
    period["Condition"]["In"]["Expressions"].append(
        {"Column": {"Expression": {"SourceRef": {"Source": "l"}}, "Property": "Mês"}}
    )
    period["Condition"]["In"]["Values"][0].append({"Literal": {"Value": f"'{mes}'"}})
    where.append(period)
    return {
        "version": "1.0.0",
        "queries": [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": {
                                    "Version": 2,
                                    "From": [
                                        {"Name": "e", "Entity": "Econômico", "Type": 0},
                                        {"Name": "m", "Entity": "Medidas", "Type": 0},
                                        {"Name": "l", "Entity": DATE_TABLE, "Type": 0},
                                    ],
                                    "Select": [
                                        {
                                            "Measure": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "m"}
                                                },
                                                "Property": prop,
                                            },
                                            "Name": name,
                                        }
                                        for prop, name in MEDIDAS
                                    ],
                                    "Where": where,
                                },
                                "Binding": {
                                    "Primary": {
                                        "Groupings": [{"Projections": [0, 1, 2, 3, 4]}]
                                    },
                                    "DataReduction": {
                                        "DataVolume": 3,
                                        "Primary": {"Window": {"Count": 10}},
                                    },
                                },
                            }
                        }
                    ]
                },
                "DatasetId": DATASET_ID,
            }
        ],
        "modelId": MODEL_ID,
    }


def _months() -> list[tuple[int, int]]:
    now = datetime.now(TIMEZONE)
    return [
        (year, month)
        for year in range(FIRST_YEAR, now.year + 1)
        for month in range(1, 13)
        if (year, month) <= (now.year, now.month)
    ]


def _extractor(cnae_divisao: str | None) -> Callable[[], ExtractorConfig]:
    def config() -> ExtractorConfig:
        return ExtractorConfig(
            source="api",
            base_url="https://wabi-brazil-south-api.analysis.windows.net",
            requests=tuple(
                HttpRequest(
                    name=f"{year}-{month:02d}",
                    endpoint="/public/reports/querydata?synchronous=true",
                    method="POST",
                    json=consulta(year, MESES[month - 1], cnae_divisao),
                    headers=HEADERS,
                )
                for year, month in _months()
            ),
        )

    return config


DATASETS = {
    dag_id: DatasetSpec(
        domain="novo_caged",
        dataset=dataset,
        extractor=_extractor(cnae_divisao),
        converter=ConverterConfig(format="powerbi_dsr"),
        load_mode=LoadMode.OVERWRITE,
    )
    for dag_id, (dataset, cnae_divisao) in RECORTES.items()
}


def _build(dag_id: str, spec: DatasetSpec) -> Any:
    @dag(
        dag_id=dag_id,
        # Diário às 06:00: o MTE divulga uma vez por mês, sem data fixa.
        schedule="0 6 * * *",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "Milena Rocha",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        tags=["cidades", "novo_caged", "construcao", "conjuntura", "ingestion"],
    )
    def novo_caged() -> None:
        @task
        def extract_to_raw(**context: Any) -> list[str]:
            return steps.extract_to_raw(spec, context["dag_run"].run_after)

        @task
        def convert_to_staging(raw_prefixes: list[str]) -> str:
            return steps.convert_to_staging(spec, raw_prefixes)

        convert_to_staging(extract_to_raw())

    return novo_caged()


DAGS = {dag_id: _build(dag_id, spec) for dag_id, spec in DATASETS.items()}
