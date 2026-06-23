import logging
from http import HTTPStatus
from datetime import datetime
from cliente_base import ClienteBase
from typing import Optional, Dict, Any, List

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
    "dezembro": "12"
}

class ClienteNovoCaged(ClienteBase):
    def __init__(self) -> None:
        base_url = "https://wabi-brazil-south-api.analysis.windows.net/public/reports"
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "X-PowerBI-ResourceKey": "5b95b481-bfbc-4287-935e-ce2b20015ab6",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "Referer": "https://app.powerbi.com/",
        }
        super().__init__(base_url=base_url, headers=headers)
        logging.info("[ClienteNovoCaged] Cliente iniciado com sucesso.")

    def obter_dados_mensais(self, ano: int, mes: str) -> Optional[Dict[str, Any]]:
        """
        Busca os dados de Construção de Edifícios para um mês/ano específico.
        """
        path = "/querydata?synchronous=true"
        
        payload = {
            "version": "1.0.0",
            "queries": [{
                "Query": {
                    "Commands": [{
                        "SemanticQueryDataShapeCommand": {
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {"Name": "e", "Entity": "Econômico", "Type": 0},
                                    {"Name": "m", "Entity": "Medidas", "Type": 0},
                                    {"Name": "l", "Entity": "LocalDateTable_9b82530a-b08e-43fc-8e3a-39c225627f7d", "Type": 0}
                                ],
                                "Select": [
                                    {"Measure": {"Expression": {"SourceRef": {"Source": "m"}}, "Property": "Admitidos"}, "Name": "Admitidos"},
                                    {"Measure": {"Expression": {"SourceRef": {"Source": "m"}}, "Property": "Desligados"}, "Name": "Desligados"},
                                    {"Measure": {"Expression": {"SourceRef": {"Source": "m"}}, "Property": "Saldo"}, "Name": "Saldo"},
                                    {"Measure": {"Expression": {"SourceRef": {"Source": "m"}}, "Property": "Estoque Mensal"}, "Name": "Estoque"},
                                    {"Measure": {"Expression": {"SourceRef": {"Source": "m"}}, "Property": "Vr. Relativa"}, "Name": "Variacao"}
                                ],
                                "Where": [
                                    {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "e"}}, "Property": "Grande Grupamento"}}], "Values": [[{"Literal": {"Value": "'Construção'"}}]]}}},
                                    {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "e"}}, "Property": "CNAE 2.0 Divisão"}}], "Values": [[{"Literal": {"Value": "'Construção de Edifícios'"}}]]}}},
                                    {"Condition": {"In": {"Expressions": [
                                        {"Column": {"Expression": {"SourceRef": {"Source": "l"}}, "Property": "Ano"}},
                                        {"Column": {"Expression": {"SourceRef": {"Source": "l"}}, "Property": "Mês"}}
                                    ], "Values": [[{"Literal": {"Value": f"{ano}L"}}, {"Literal": {"Value": f"'{mes.lower()}'"}}]]}}}
                                ]
                            },
                            "Binding": {
                                "Primary": {"Groupings": [{"Projections": [0, 1, 2, 3, 4]}]},
                                "DataReduction": {"DataVolume": 3, "Primary": {"Window": {"Count": 10}}}
                            }
                        }
                    }]
                },
                "DatasetId": "4859b5fd-e3ad-4a7c-95fe-aa62fc046d96"
            }],
            "modelId": 3021080
        }

        status, response = self.request("POST", path, json=payload)

        if status == HTTPStatus.OK and response:
            return self._parse_response(response, ano, mes)
        
        return None

    def _parse_response(self, response: Dict[str, Any], ano: int, mes: str) -> Optional[Dict[str, Any]]:
        """
        Trata o JSON complexo do Power BI para um formato amigável para o Postgres.
        """
        try:
            raw_values = response['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0'][0]['C']
            return {
                "ano": ano,
                "mes": MAP_MESES.get(mes.lower(), mes),
                "admitidos": raw_values[0],
                "desligados": raw_values[1],
                "saldo": raw_values[2],
                "estoque": raw_values[3],
                "variacao": raw_values[4]
            }
        except (KeyError, IndexError, TypeError) as e:
            logging.error(f"[ClienteNovoCaged] Erro ao fazer o parse da resposta para {mes}/{ano}: {e}")
            return None

    def obter_historico(self, anos: Optional[List[int]] = None, meses: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Busca os dados de Construção de Edifícios para uma lista de anos e meses.
        Se não fornecidos, busca do histórico padrão (2024 até o ano/mês atual).
        Retorna uma lista de dicionários pronta para inserção no banco de dados.
        """
        if not anos:
            anos = [2024, 2025, 2026]
        if not meses:
            meses = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", 
                     "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]

        historico_caged = []
        ano_atual = datetime.now().year
        mes_atual = datetime.now().month

        for ano in anos:

            if ano > ano_atual:
                break
                
            for mes in meses:
                if ano == ano_atual and meses.index(mes) - 1> mes_atual:
                    break
                    
                logging.info(f"[ClienteNovoCaged] Processando {mes}/{ano}...")
                dados = self.obter_dados_mensais(ano, mes)
                
                if dados:
                    historico_caged.append(dados)
        
        return historico_caged