"""Cliente da API Olinda 'MercadoImobiliario' do BCB (OData).

Usada para indicadores do mercado imobiliário que NÃO estão no SGS, como o
Crédito Imobiliário / PIB (%). Recurso:
https://olinda.bcb.gov.br/olinda/servico/MercadoImobiliario/versao/v1/odata/mercadoimobiliario
Colunas: Data, Info (código do indicador), Valor.
"""

import logging
from datetime import datetime
from typing import Any

import requests


class ClienteBacenImobiliario:
    """Consome a série de um indicador (`Info`) da Olinda MercadoImobiliario."""

    BASE = (
        "https://olinda.bcb.gov.br/olinda/servico/MercadoImobiliario/"
        "versao/v1/odata/mercadoimobiliario"
    )

    def __init__(self) -> None:
        logging.info("[cliente_bacen_imobiliario] Initialized ClienteBacenImobiliario")

    def obter_serie(self, info: str) -> list[dict[str, Any]]:
        """Retorna a série mensal de um indicador como lista de registros.

        A URL é montada como string (espaços viram %20 pelo requests). NÃO usar
        params={} aqui: o encoding de espaço como '+' quebra o $filter do OData
        do Olinda (HTTP 400).
        """
        url = f"{self.BASE}?$filter=Info eq '{info}'" f"&$orderby=Data&$format=json"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()

        linhas = resp.json().get("value", [])
        dt_ingest = datetime.now().isoformat()
        return [
            {
                "info": info,
                "data": linha["Data"],
                "valor": linha["Valor"],
                "dt_ingest": dt_ingest,
            }
            for linha in linhas
        ]

    def obter_credito_pib(self) -> list[dict[str, Any]]:
        """Crédito Imobiliário / PIB (%), série mensal — página 4 do boletim."""
        return self.obter_serie("indices_imobiliario_pib_br")
