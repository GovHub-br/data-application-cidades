import logging
from datetime import datetime
from http import HTTPStatus
from typing import Optional
import pandas as pd
from cliente_base import ClienteBase

class ClienteBacen(ClienteBase):
    BASE_URL = "https://api.bcb.gov.br"

    def __init__(self) -> None:
        super().__init__(
            base_url=self.BASE_URL,
            headers={"Accept": "application/json"},
        )
        logging.info(
            f"[cliente_bacen.py] Initialized ClienteBacen with base_url: {self.base_url}"
        )

    @staticmethod
    def _to_date(bcb_date: str) -> str:
        return datetime.strptime(bcb_date, "%d/%m/%Y").strftime("%Y-%m-%d")

    def _fetch_series(
            self,
            codigo: int,
            data_inicial: str,
            data_final: str,
    ) -> Optional[pd.DataFrame]:
        path = f"/dados/serie/bcdata.sgs.{codigo}/dados"
        params = {
            "formato": "json",
            "dataInicial": data_inicial,
            "dataFinal": data_final,
        }

        status, data = self.request("GET", path, params=params)

        if status != HTTPStatus.OK or not data:
            logging.error(
                f"[cliente_bacen.py] Falha ao buscar a serie {codigo}."
                f"HTTP {status}"
            )
            return None

        df = pd.DataFrame(data)
        df["data_referencia"] = df['data'].apply(self._to_date)
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

        logging.info(
            f"[cliente_bacen.py] Serie {codigo}: {len(df)} pontos retornados."
        )
        return df

    def fetch_and_transform(
        self,
        series: dict[int, str],
        data_inicial: str,
        data_final: str,
    ) -> Optional[pd.DataFrame]:
        """
        Busca todas as séries informadas e retorna um DataFrame flat
        com uma linha por data e uma coluna por série.

        Args:
            series:       Dicionário {codigo_sgs: "nome_coluna"}.
            data_inicial: Data inicial no formato 'dd/MM/yyyy'.
            data_final:   Data final no formato 'dd/MM/yyyy'.

        Returns:
            DataFrame com colunas [data_referencia, <nome_coluna>, ...]
            ou None em caso de falha total.
        """
        logging.info(
            f"[cliente_bacen.py] Iniciando extração de "
            f"{data_inicial} até {data_final} | "
            f"Séries: {list(series.values())}"
        )

        frames: list[pd.DataFrame] = []

        for codigo, nome_coluna in series.items():
            df_serie = self._fetch_series(codigo, data_inicial, data_final)

            if df_serie is None:
                logging.warning(
                    f"[cliente_bacen.py] Série {codigo} ({nome_coluna}) "
                    f"retornou None — ignorando."
                )
                continue

            df_serie = df_serie.rename(columns={"valor": nome_coluna})
            frames.append(df_serie.set_index("data_referencia"))

        if not frames:
            logging.error(
                "[cliente_bacen.py] Nenhuma série extraída com sucesso. "
                "Verifique os códigos e a janela de datas."
            )
            return None

        # Join de todos os DataFrames pelo índice (data_referencia)
        df_final = pd.concat(frames, axis=1).reset_index()
        df_final["dt_ingest"] = datetime.now().isoformat()

        logging.info(
            f"[cliente_bacen.py] Transformação concluída. "
            f"{len(df_final)} registros | Colunas: {list(df_final.columns)}"
        )
        return df_final
