import http
import logging
from typing import Optional
from cliente_base import ClienteBase


class ClienteIBGE(ClienteBase):
    """Cliente para a API de Dados Agregados do IBGE.

    Base URL: https://servicodados.ibge.gov.br/api/v3
    Documentação: https://servicodados.ibge.gov.br/api/docs/agregados?versao=3
    """

    BASE_URL = "https://servicodados.ibge.gov.br/api/v3"

    def __init__(self) -> None:
        super().__init__(base_url=ClienteIBGE.BASE_URL)
        logging.info(
            "[cliente_ibge.py] Initialized ClienteIBGE with base_url: "
            f"{ClienteIBGE.BASE_URL}"
        )

    def get_dados_agregados(
        self,
        agregado: int,
        variaveis: str,
        periodos: str = "-20",
        nivel: str = "N1",
        localidade: str = "1",
        classificacao_id: int | str | None = None,
        categoria: int | str | None = None,
    ) -> list | None:
        """Busca dados de um agregado da API IBGE.

        Args:
            agregado: ID do agregado (ex: 5932 para PIB, 2296 para SINAPI).
            variaveis: IDs das variáveis separadas por pipe
                (ex: "6564|6563|6562|6561").
            periodos: Períodos a consultar
                ("-20" = últimos 20, ou ex: "202401|202402").
            nivel: Nível territorial ("N1"=Brasil, "N2"=Região, "N3"=UF).
            localidade: ID da localidade ("1"=Brasil).
            classificacao_id: ID da classificação (ex: 11255). None se não houver.
            categoria: ID da categoria (ex: 90694). None se não houver.

        Returns:
            Lista de dicionários com os dados ou None em caso de falha.
        """
        path = (
            f"/agregados/{agregado}"
            f"/periodos/{periodos}"
            f"/variaveis/{variaveis}"
        )

        params = {
            "localidades": f"{nivel}[{localidade}]",
        }

        if classificacao_id is not None and categoria is not None:
            params["classificacao"] = f"{classificacao_id}[{categoria}]"

        logging.info(
            f"[cliente_ibge.py] Fetching agregado={agregado}, "
            f"variaveis={variaveis}, periodos={periodos}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, path, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_ibge.py] Successfully fetched data for "
                f"agregado={agregado}"
            )
            return data
        else:
            logging.warning(
                f"[cliente_ibge.py] Failed to fetch data for "
                f"agregado={agregado} with status: {status}"
            )
            return None

    @staticmethod
    def transformar_resposta(dados_api: list) -> list[dict]:
        """Transforma o JSON aninhado da API IBGE em lista de dicts flat.

        Args:
            dados_api: Resposta da API retornada por get_dados_agregados().

        Returns:
            Lista de dicts com campos: variavel_id, variavel_nome, unidade,
            periodo, valor, localidade.
        """
        from datetime import datetime

        registros = []

        for variavel in dados_api:
            variavel_id = variavel["id"]
            variavel_nome = variavel["variavel"]
            unidade = variavel["unidade"]

            for resultado in variavel["resultados"]:
                class_ids = []
                class_nomes = []
                cat_ids = []
                cat_nomes = []
                
                for c in resultado.get("classificacoes", []):
                    class_ids.append(c["id"])
                    class_nomes.append(c["nome"])
                    cat_dict = c.get("categoria", {})
                    for k, v in cat_dict.items():
                        cat_ids.append(k)
                        cat_nomes.append(v)
                        
                classificacao_id = "|".join(class_ids) if class_ids else "0"
                classificacao_nome = " | ".join(class_nomes) if class_nomes else ""
                categoria_id = "|".join(cat_ids) if cat_ids else "0"
                categoria_nome = " | ".join(cat_nomes) if cat_nomes else ""

                for serie in resultado["series"]:
                    localidade_id = serie["localidade"]["id"]
                    localidade_nome = serie["localidade"]["nome"]

                    for periodo, valor in serie["serie"].items():
                        registros.append(
                            {
                                "variavel_id": str(variavel_id),
                                "variavel_nome": variavel_nome,
                                "localidade_id": str(localidade_id),
                                "localidade_nome": localidade_nome,
                                "classificacao_id": classificacao_id,
                                "classificacao_nome": classificacao_nome,
                                "categoria_id": categoria_id,
                                "categoria_nome": categoria_nome,
                                "unidade": unidade,
                                "periodo": str(periodo),
                                "valor": valor if valor and valor != "..." else None,
                                "dt_ingest": datetime.now().isoformat(),
                            }
                        )

        return registros
