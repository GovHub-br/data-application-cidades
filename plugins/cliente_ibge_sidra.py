"""Cliente da API SIDRA do IBGE (apisidra.ibge.gov.br).

Alternativa à API v3 de agregados para tabelas cujo endpoint /dados está
retornando HTTP 500 no v3 (ex.: PNAD-C por grupamento de atividade, 6323/6391).
A SIDRA entrega os mesmos dados num formato tabular plano.

Formato de resposta: lista de dicts; o 1º elemento é o cabeçalho (mapa de
códigos -> nomes) e deve ser descartado. Colunas úteis:
  D3C/D3N = período (trimestre móvel), D4C/D4N = categoria da classificação,
  D2C = variável, V = valor, MN = unidade de medida.
"""

import logging
from datetime import datetime
from typing import Any

import requests


class ClienteIbgeSidra:
    """Consome uma tabela SIDRA (t/variável/classificação/categorias/períodos)."""

    BASE = "https://apisidra.ibge.gov.br/values"

    def __init__(self) -> None:
        logging.info("[cliente_ibge_sidra] Initialized ClienteIbgeSidra")

    def obter(
        self,
        tabela: int,
        variavel: int,
        classificacao: int,
        categorias: list[int],
        periodos: str = "last 12",
        nivel: str = "n1/all",
    ) -> list[dict[str, Any]]:
        """Retorna os registros planos de uma consulta SIDRA."""
        cats = ",".join(str(c) for c in categorias)
        per = periodos.replace(" ", "%20")
        url = (
            f"{self.BASE}/t/{tabela}/{nivel}/v/{variavel}"
            f"/p/{per}/c{classificacao}/{cats}"
        )
        logging.info(f"[cliente_ibge_sidra] GET {url}")
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
        resp.raise_for_status()

        dados = resp.json()
        if not isinstance(dados, list) or len(dados) <= 1:
            return []

        dt_ingest = datetime.now().isoformat()
        registros = []
        for row in dados[1:]:  # descarta o cabeçalho
            registros.append(
                {
                    "periodo": row.get("D3C"),
                    "periodo_nome": row.get("D3N"),
                    "variavel_id": row.get("D2C"),
                    "categoria_id": row.get("D4C"),
                    "categoria": row.get("D4N"),
                    "unidade": row.get("MN"),
                    "valor": row.get("V"),
                    "dt_ingest": dt_ingest,
                }
            )
        return registros
