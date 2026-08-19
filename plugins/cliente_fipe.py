import logging
import io
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from cliente_base import ClienteBase


class ClienteFipeZap(ClienteBase):
    """
    Cliente para extração de dados do Índice FipeZAP de locação residencial.

    Fonte: https://www.fipe.org.br/pt-br/indices/fipezap/#indice-fipezap-historico
    URL:   https://downloads.fipe.org.br/indices/fipezap/fipezap-serieshistoricas.xlsx

    Colunas extraídas da aba 'Índice FipeZAP':
      col 1  → data_referencia
      col 27 → imoveis_residenciais_locacao_var_mensal_total
      col 32 → imoveis_residenciais_locacao_var_ano_total

    Observação: a série histórica pode sofrer revisões retroativas a cada
    nova divulgação — a estratégia de carga deve sempre regravar a série
    completa (upsert por data_referencia).
    """

    BASE_URL = "https://downloads.fipe.org.br"
    XLSX_PATH = "/indices/fipezap/fipezap-serieshistoricas.xlsx"
    ABA = "Índice FipeZAP"

    # Índices de coluna na planilha (0-based)
    COL_DATA = 1
    COL_VAR_MENSAL = 27
    COL_VAR_ANO = 32

    # Linhas de cabeçalho (0-3) e início dos dados (4)
    LINHA_INICIO_DADOS = 4
    # Linha onde começam notas de rodapé
    LINHA_FIM_DADOS = 223

    def __init__(self) -> None:
        super().__init__(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " "AppleWebKit/537.36"
                )
            },
        )
        logging.info(
            "[cliente_fipezap.py] Initialized ClienteFipeZap "
            f"com base_url: {self.BASE_URL}"
        )

    def fetch_and_transform(self) -> Optional[pd.DataFrame]:
        """
        Baixa o XLSX da FipeZAP, extrai as colunas de locação residencial
        e retorna um DataFrame limpo pronto para carga no PostgreSQL.

        O arquivo é baixado em memória e descartado após a extração —
        nenhum arquivo é salvo em disco.

        Returns:
            DataFrame com todas as colunas como texto — fiel ao valor original
            da planilha (data_referencia,
            imoveis_residenciais_locacao_var_mensal_total,
            imoveis_residenciais_locacao_var_ano_total, dt_ingest). A tipagem
            real fica por conta do dbt (camada silver).
            Ou None em caso de falha.
        """
        logging.info(
            f"[cliente_fipezap.py] Baixando XLSX de: " f"{self.BASE_URL}{self.XLSX_PATH}"
        )

        try:
            _, content = self.request(
                "GET", f"{self.BASE_URL}{self.XLSX_PATH}", response_type="bytes"
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"[cliente_fipezap.py] Erro ao baixar o XLSX: {e}")
            return None

        # Guarda os bytes brutos do XLSX p/ a camada raw do data lake (formato nativo).
        self.ultimo_conteudo_xlsx = content

        try:
            df_raw = pd.read_excel(
                io.BytesIO(content),
                sheet_name=self.ABA,
                header=None,
                dtype=str,
            )

            # Extrai apenas as colunas necessárias dentro da janela de dados
            df = df_raw.iloc[
                self.LINHA_INICIO_DADOS : self.LINHA_FIM_DADOS,
                [self.COL_DATA, self.COL_VAR_MENSAL, self.COL_VAR_ANO],
            ].copy()

            df.columns = [
                "data_referencia",
                "imoveis_residenciais_locacao_var_mensal_total",
                "imoveis_residenciais_locacao_var_ano_total",
            ]

            # Remove linhas sem data (rodapé residual)
            df = df[pd.notna(df["data_referencia"])].copy()

            df["dt_ingest"] = datetime.now().isoformat()

            logging.info(
                f"[cliente_fipezap.py] Transformação concluída. "
                f"{len(df)} registros | "
                f"De {df['data_referencia'].min()} "
                f"até {df['data_referencia'].max()}"
            )

            return df

        except Exception as e:
            logging.error(f"[cliente_fipezap.py] Erro ao processar o XLSX: {e}")
            return None
