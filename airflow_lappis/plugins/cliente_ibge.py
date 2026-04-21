import http
import logging
from typing import Any
from datetime import datetime
from cliente_base import ClienteBase


class ClienteIBGE(ClienteBase):
    """
    Cliente para consumir a API de Dados Abertos do Serviço de Dados IBGE.
    """

    BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/"
    BASE_HEADER = {"accept": "application/json"}

    def __init__(self) -> None:
        super().__init__(base_url=ClienteIBGE.BASE_URL)
        self.data_final = self._trimestre_final_dinamico()
        logging.info(
            "[cliente_ibge.py] Initialized ClienteIBGE with base_url: "
            f"{ClienteIBGE.BASE_URL}"
        )
    def _fetch_data(self, endpoint: str, desc: str, **params: Any) -> list:
        logging.info(f"[cliente_ibge.py] Fetching {desc} with params: {params}")

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(f"[cliente_ibge.py] Successfully fetched {desc}")
            return data
        else:
            logging.warning(f"[cliente_ibge.py] Failed to fetch {desc} with status: {status}")
            return None

 

    def trimestre_final_dinamico(self):
        ano_atual = datetime.now().year
        mes_atual = datetime.now().month

        trimestre = (mes_atual - 1) // 3 + 1

        return f"{ano_atual}{trimestre}"


    def get_rendimento_medio_mensal_real_anual_por_trimestre(self, **params: Any) -> list:
        """
        Obter rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de referência com rendimento de trabalho, habitualmente recebido no trabalho principal do ano inteiro por semestre
        """

        # deixar variavel
        endpoint = f"5442/periodos/202401-{self.data_final}/variaveis/5932?localidades=N1[all]&classificacao=888[47946,47949]"
        return self._fetch_data(endpoint, "rendimento médio mensal real", **params)

    def get_ocupacao_por_atividade_de_trabalho_trimestral(self, **params: Any) -> list:
        """
        Obter ocupação por atividade de trabalho trimestral
        """
        #deixar variavel
        
        endpoint = f"5434/periodos/202401-{self.data_final}/variaveis/4090?localidades=N1[all]&classificacao=888[47946,47949]"
        return self._fetch_data(endpoint, "ocupação por atividade de trabalho trimestral", **params)

