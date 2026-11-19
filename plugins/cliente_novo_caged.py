import logging
from http import HTTPStatus
from datetime import date
from cliente_base import ClienteBase
from typing import Optional, Dict, Any, List, Tuple

MAP_MESES = {
    "janeiro": "01",
    "fevereiro": "02",
    "março": "03",
    "abril": "04",
    "maio": "05",
    "junho": "06",
    "julho": "07",
    "agosto": "08",
    "setembro": "09",
    "outubro": "10",
    "novembro": "11",
    "dezembro": "12",
}

FIRST_YEAR = 2024  # start of the series the products use


class MissingMonthsError(RuntimeError):
    """A month the CAGED has already published came back empty."""


def expected_periods(today: date) -> List[Tuple[int, str]]:
    """(year, month name) from January/FIRST_YEAR to the month before `today`.

    The current month is never published; the last one returned may not be yet either
    (CAGED releases month m around the end of m+1).
    """
    return [
        (year, name)
        for year in range(FIRST_YEAR, today.year + 1)
        for number, name in enumerate(MAP_MESES, start=1)
        if (year, number) < (today.year, today.month)
    ]


class ClienteNovoCaged(ClienteBase):
    def __init__(self) -> None:
        base_url = "https://wabi-brazil-south-api.analysis.windows.net/public/reports"
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
        )

        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "X-PowerBI-ResourceKey": "5b95b481-bfbc-4287-935e-ce2b20015ab6",
            "User-Agent": user_agent,
            "Referer": "https://app.powerbi.com/",
        }
        super().__init__(base_url=base_url, headers=headers)
        logging.info("[ClienteNovoCaged] Cliente iniciado com sucesso.")

    def obter_dados_mensais(
        self, ano: int, mes: str, cnae_divisao: Optional[str] = "Construção de Edifícios"
    ) -> Optional[Dict[str, Any]]:
        """
        Busca os dados de empregos na construção para um mês/ano específico.

        `cnae_divisao` filtra por "CNAE 2.0 Divisão" (ex: "Construção de
        Edifícios", "Serviços Especializados para Construção", "Obras de
        Infraestrutura"). Passar `None` remove esse filtro e retorna o total
        da construção (todas as divisões, "Grande Grupamento" = Construção).
        """
        path = "/querydata?synchronous=true"

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
        if cnae_divisao:
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
                            "Values": [[{"Literal": {"Value": f"'{cnae_divisao}'"}}]],
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
                                {"Literal": {"Value": f"{ano}L"}},
                                {"Literal": {"Value": f"'{mes.lower()}'"}},
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
                    "DatasetId": "4859b5fd-e3ad-4a7c-95fe-aa62fc046d96",
                }
            ],
            "modelId": 3021080,
        }

        status, response = self.request("POST", path, json=payload)

        if status == HTTPStatus.OK and response:
            return self._parse_response(response, ano, mes)

        return None

    def _parse_response(
        self, response: Dict[str, Any], ano: int, mes: str
    ) -> Optional[Dict[str, Any]]:
        """
        Trata o JSON complexo do Power BI para um formato amigável para o Postgres.
        """
        try:
            raw_values = response["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][0]["DM0"][
                0
            ]["C"]
            return {
                "ano": ano,
                "mes": MAP_MESES.get(mes.lower(), mes),
                "admitidos": raw_values[0],
                "desligados": raw_values[1],
                "saldo": raw_values[2],
                "estoque": raw_values[3],
                "variacao": raw_values[4],
            }
        except (KeyError, IndexError, TypeError) as e:
            logging.error(
                f"[ClienteNovoCaged] Erro ao fazer o parse da resposta para {mes}/{ano}: {e}"
            )
            return None

    def obter_historico(
        self,
        cnae_divisao: Optional[str] = "Construção de Edifícios",
        today: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """
        Full history (`expected_periods`) up to the month before `today` (default:
        the run date).
        `cnae_divisao`: see `obter_dados_mensais` (None = whole construction sector).
        Only the latest month may be missing (not published yet); any other missing month
        raises MissingMonthsError before the caller rewrites raw/staging without it.
        """
        periods = expected_periods(today or date.today())
        historico_caged = []
        missing = []

        for ano, mes in periods:
            logging.info(f"[ClienteNovoCaged] Processando {mes}/{ano}...")
            dados = self.obter_dados_mensais(ano, mes, cnae_divisao=cnae_divisao)

            if dados:
                historico_caged.append(dados)
            else:
                missing.append((ano, mes))

        if missing and missing[-1] == periods[-1]:
            ano, mes = missing.pop()
            logging.warning(f"[ClienteNovoCaged] {mes}/{ano} not published yet; skipped.")
        if missing:
            raise MissingMonthsError(
                "Novo CAGED months missing: " + ", ".join(f"{mes}/{ano}" for ano, mes in missing)
            )

        return historico_caged
