import http
import logging
from cliente_base import ClienteBase


class ClienteBacen(ClienteBase):
    """Cliente para a API SGS (Sistema Gerenciador de Séries Temporais) do BACEN.

    Base URL: https://api.bcb.gov.br/dados/serie
    Documentação: https://dadosabertos.bcb.gov.br
    """

    BASE_URL = "https://api.bcb.gov.br/dados/serie"

    def __init__(self) -> None:
        super().__init__(base_url=ClienteBacen.BASE_URL)
        logging.info(
            "[cliente_bacen.py] Initialized ClienteBacen with base_url: "
            f"{ClienteBacen.BASE_URL}"
        )

    def get_serie(self, codigo: int, ultimos: int = 13) -> list | None:
        """Busca os últimos N registros de uma série temporal do SGS/BACEN.

        Args:
            codigo: Código da série no SGS (ex: 20704).
            ultimos: Número de observações mais recentes a retornar (padrão: 13).

        Returns:
            Lista de dicts com 'data' e 'valor', ou None em caso de falha.
        """
        path = f"/bcdata.sgs.{codigo}/dados"
        params = {"formato": "json", "ultimos": ultimos}

        logging.info(f"[cliente_bacen.py] Fetching serie={codigo}, ultimos={ultimos}")

        status, data = self.request(http.HTTPMethod.GET, path, params=params)

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_bacen.py] Successfully fetched {len(data)} records "
                f"for serie={codigo}"
            )
            return data
        else:
            logging.warning(
                f"[cliente_bacen.py] Failed to fetch serie={codigo} "
                f"with status: {status}"
            )
            return None
