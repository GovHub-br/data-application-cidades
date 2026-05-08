import io
import logging
import re
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from cliente_base import ClienteBase


class ClienteAbecip(ClienteBase):
    """
    Cliente para extração de dados da ABECIP.

    Fonte: https://www.abecip.org.br/credito-imobiliario/indicadores/caderneta-de-poupanca

    O nome do arquivo XLSX muda a cada atualização — o cliente faz scraping
    da página para obter sempre a URL atual antes do download.

    Dados extraídos: Caderneta de Poupança SBPE Mensal
    Aba: SBPE_Mensal
    Colunas:
        - periodo             → data_referencia
        - deposito            → deposito
        - retirada            → retirada
        - captacao_liq_valor  → captacao_liquida_valor
        - captacao_liq_pct    → captacao_liquida_pct
        - rendimento          → rendimento
        - saldo               → saldo

    Observação: valores em R$ milhões — conversão de unidade é responsabilidade do dbt.
    """

    BASE_URL = "https://www.abecip.org.br"
    PAGINA_POUPANCA = (
        "/credito-imobiliario/indicadores/caderneta-de-poupanca"
    )
    ABA_POUPANCA = "SBPE_Mensal"

    # Índices de coluna na planilha (0-based), a partir da linha 6
    COLUNAS_IDX = [0, 1, 2, 3, 4, 5, 6]
    COLUNAS_NOMES = [
        "data_referencia",
        "deposito",
        "retirada",
        "captacao_liquida_valor",
        "captacao_liquida_pct",
        "rendimento",
        "saldo",
    ]

    def __init__(self) -> None:
        super().__init__(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36"
                )
            },
        )
        logging.info(
            "[cliente_abecip.py] Initialized ClienteAbecip "
            f"com base_url: {self.BASE_URL}"
        )

    def _get_xlsx_url(self, pagina_path: str, pattern: str) -> Optional[str]:
        """
        Faz scraping da página ABECIP e retorna a URL atual do XLSX
        correspondente ao pattern informado.

        Args:
            pagina_path: Path da página (ex: '/credito-imobiliario/...')
            pattern:     Substring para identificar o link correto
                         (ex: 'cp-historico')

        Returns:
            URL completa do XLSX ou None em caso de falha.
        """
        url_pagina = f"{self.BASE_URL}{pagina_path}"
        logging.info(
            f"[cliente_abecip.py] Buscando URL do XLSX em: {url_pagina}"
        )

        try:
            _, html = self.request(
                "GET",
                pagina_path,
                response_type="text",
            )
        except Exception as e:
            logging.error(
                f"[cliente_abecip.py] Erro ao acessar página ABECIP: {e}"
            )
            return None

        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            if pattern in href:
                url = (
                    href
                    if href.startswith("http")
                    else f"{self.BASE_URL}{href}"
                )
                logging.info(
                    f"[cliente_abecip.py] URL do XLSX encontrada: {url}"
                )
                return url

        logging.error(
            f"[cliente_abecip.py] Nenhum link com pattern "
            f"'{pattern}' encontrado na página."
        )
        return None

    def _download_xlsx(self, url: str) -> Optional[bytes]:
        """
        Baixa o XLSX em memória e retorna os bytes.
        Nenhum arquivo é salvo em disco.
        """
        logging.info(f"[cliente_abecip.py] Baixando XLSX de: {url}")
        try:
            _, content = self.request(
                "GET",
                url,
                response_type="bytes",
            )

            return content
        except Exception as e:
            logging.error(
                f"[cliente_abecip.py] Erro ao baixar XLSX: {e}"
            )
            return None

    def fetch_and_transform_poupanca(self) -> Optional[pd.DataFrame]:
        """
        Baixa e processa o XLSX de Saldo da Caderneta de Poupança (SBPE Mensal).

        Returns:
            DataFrame com colunas:
                - data_referencia          (str 'yyyy-MM-dd')
                - deposito                 (float | None) — R$ milhões
                - retirada                 (float | None) — R$ milhões
                - captacao_liquida_valor   (float | None) — R$ milhões
                - captacao_liquida_pct     (float | None) — %
                - rendimento               (float | None) — R$ milhões
                - saldo                    (float | None) — R$ milhões
                - dt_ingest                (str ISO 8601)
            Ou None em caso de falha.
        """
        url = self._get_xlsx_url(self.PAGINA_POUPANCA, "cp-historico")
        if url is None:
            return None

        content = self._download_xlsx(url)
        if content is None:
            return None

        try:
            df_raw = pd.read_excel(
                io.BytesIO(content),
                sheet_name=self.ABA_POUPANCA,
                header=None,
            )

            df = df_raw.iloc[6:, self.COLUNAS_IDX].copy()
            df.columns = self.COLUNAS_NOMES

            # Mantém apenas registros mensais (datetime) — descarta
            # linhas anuais (Total.YYYY), rodapé e futuras vazias
            df = df[
                df["data_referencia"].apply(
                    lambda x: isinstance(x, datetime)
                )
            ].copy()

            # Descarta meses futuros sem dados (captacao = 0 e saldo vazio)
            df = df[
                ~(
                    (df["captacao_liquida_valor"] == 0)
                    & df["saldo"].isna()
                )
            ].copy()

            # Normaliza data para string 'yyyy-MM-dd'
            df["data_referencia"] = pd.to_datetime(
                df["data_referencia"]
            ).dt.strftime("%Y-%m-%d")

            # Converte colunas numéricas
            cols_numericas = [c for c in self.COLUNAS_NOMES if c != "data_referencia"]
            for col in cols_numericas:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            df["dt_ingest"] = datetime.now().isoformat()
            df = df.reset_index(drop=True)

            logging.info(
                f"[cliente_abecip.py] Poupança: {len(df)} registros | "
                f"De {df['data_referencia'].min()} "
                f"até {df['data_referencia'].max()}"
            )

            return df

        except Exception as e:
            logging.error(
                f"[cliente_abecip.py] Erro ao processar XLSX de poupança: {e}"
            )
            return None
