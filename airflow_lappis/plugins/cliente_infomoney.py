import logging
from typing import List, Dict, Any, Optional
from cliente_base import ClienteBase

class ClienteInfomoney(ClienteBase):
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
 
        super().__init__(base_url="https://www.alphavantage.co")

    def get_daily_series(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Busca a série temporal diária e formata para inserção no banco.
        """
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_key,
            "outputsize": "compact"
        }
        
        logging.info(f"[Infomoney] Buscando dados para {symbol}...")
        status, dados = self.request("GET", "/query", params=params)
        
        if not dados or "Time Series (Daily)" not in dados:
            logging.error(f"Erro ao buscar dados ou limite de API atingido: {dados}")
            logging.error("Para obter uma API Key gratuita, acesse https://www.alphavantage.co/support/#api-keys")
            return []

        return dados["Time Series (Daily)"]