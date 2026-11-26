"""The Novo CAGED query on the public Power BI of the Ministério do Trabalho (pure, no I/O).

The CAGED has no API: the DAGs post the query the public report makes, one month at a
time, and land each answer as it comes. Year and month are not in the answer, so they go
in the name of the landed file (`<year>-<month>.json`).
"""

from __future__ import annotations

from datetime import date
from typing import Optional

MONTHS = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto",
          "setembro", "outubro", "novembro", "dezembro"]  # fmt: skip
FIRST_YEAR = 2024  # start of the series the products use
ENDPOINT = "public/reports/querydata?synchronous=true"


def expected_periods(today: date) -> list[tuple[int, int]]:
    """(year, month) from January/FIRST_YEAR to the month before `today`.

    The current month is never published; the last one returned may not be yet either
    (CAGED releases month m around the end of m+1).
    """
    return [
        (year, month)
        for year in range(FIRST_YEAR, today.year + 1)
        for month in range(1, 13)
        if (year, month) < (today.year, today.month)
    ]


def query_payload(year: int, month: int, cnae_division: Optional[str]) -> dict:
    """The report query for one month; `cnae_division=None` is the whole construction."""
    month_name = MONTHS[month - 1]
    where = [
        {
            "Condition": {
                "In": {
                    "Expressions": [
                        {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "e"}},
                                "Property": "Grande Grupamento",
                            }
                        }
                    ],
                    "Values": [[{"Literal": {"Value": "'Construção'"}}]],
                }
            }
        },
    ]
    if cnae_division:
        where.append(
            {
                "Condition": {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "e"}},
                                    "Property": "CNAE 2.0 Divisão",
                                }
                            }
                        ],
                        "Values": [[{"Literal": {"Value": f"'{cnae_division}'"}}]],
                    }
                }
            }
        )
    where.append(
        {
            "Condition": {
                "In": {
                    "Expressions": [
                        {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "l"}},
                                "Property": "Ano",
                            }
                        },
                        {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "l"}},
                                "Property": "Mês",
                            }
                        },
                    ],
                    "Values": [
                        [
                            {"Literal": {"Value": f"{year}L"}},
                            {"Literal": {"Value": f"'{month_name}'"}},
                        ]
                    ],
                }
            }
        }
    )

    payload = {
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
                                        {
                                            "Name": "e",
                                            "Entity": "Econômico",
                                            "Type": 0,
                                        },
                                        {"Name": "m", "Entity": "Medidas", "Type": 0},
                                        {
                                            "Name": "l",
                                            "Entity": "LocalDateTable_9b82530a-b08e-43fc-8e3a-39c225627f7d",
                                            "Type": 0,
                                        },
                                    ],
                                    "Select": [
                                        {
                                            "Measure": {
                                                "Expression": {"SourceRef": {"Source": "m"}},
                                                "Property": "Admitidos",
                                            },
                                            "Name": "Admitidos",
                                        },
                                        {
                                            "Measure": {
                                                "Expression": {"SourceRef": {"Source": "m"}},
                                                "Property": "Desligados",
                                            },
                                            "Name": "Desligados",
                                        },
                                        {
                                            "Measure": {
                                                "Expression": {"SourceRef": {"Source": "m"}},
                                                "Property": "Saldo",
                                            },
                                            "Name": "Saldo",
                                        },
                                        {
                                            "Measure": {
                                                "Expression": {"SourceRef": {"Source": "m"}},
                                                "Property": "Estoque Mensal",
                                            },
                                            "Name": "Estoque",
                                        },
                                        {
                                            "Measure": {
                                                "Expression": {"SourceRef": {"Source": "m"}},
                                                "Property": "Vr. Relativa",
                                            },
                                            "Name": "Variacao",
                                        },
                                    ],
                                    "Where": where,
                                },
                                "Binding": {
                                    "Primary": {"Groupings": [{"Projections": [0, 1, 2, 3, 4]}]},
                                    "DataReduction": {
                                        "DataVolume": 3,
                                        "Primary": {"Window": {"Count": 10}},
                                    },
                                },
                            }
                        }
                    ]
                },
                "DatasetId": "4859b5fd-e3ad-4a7c-95fe-aa62fc046d96",
            }
        ],
        "modelId": 3021080,
    }
    return payload
