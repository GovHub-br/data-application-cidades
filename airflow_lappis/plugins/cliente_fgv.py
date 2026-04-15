import logging
import io
from datetime import datetime
from typing import Optional
import pandas as pd
import requests

class ClienteFGV:
    """
    Cliente para extração de dados INCC-M da FGV.

    Fonte: https://sindusconpr.com.br/download/10984/1364
    """

    URL_INCC = "https://sindusconpr.com.br/download/10984/1364"

    def __init__(self) -> None:
        logging.info("[cliente_fgv.py] Initialized ClienteFGV")

    def fetch_and_transform_incc(self) -> Optional[list[dict]]:
        """
        Baixa o arquivo XLSX do INCC-M, limpa os cabeçalhos e formata os dados.

        Returns:
            Lista de dicionários contendo os registros do índice ou None em caso de falha.
        """
        logging.info(f"[cliente_fgv.py] Baixando dados de: {self.URL_INCC}")
        
        # User-Agent previne que alguns servidores bloqueiem
        # requisições de bibliotecas python
        headers = {
            'User-Agent': ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36"
            )
        }

        try:
            response = requests.get(self.URL_INCC, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"[cliente_fgv.py] Erro ao baixar o arquivo da FGV: {e}")
            return None

        try:
            # Lendo o Excel em memória
            # Pula as 3 primeiras linhas de cabeçalho
            df = pd.read_excel(
                io.BytesIO(response.content),
                skiprows=3,
                usecols="A:E",
                names=["mes", "indice", "var_mes", "var_ano", "var_12_meses"]
            )

            # Remove as linhas vazias ou o rodapé de "Fonte FGV"
            df = df.dropna(subset=['mes'])
            df = df[~df['mes'].astype(str).str.contains('Fonte', case=False, na=False)]

            # Trata os valores de "..." presentes em algumas linhas de 1994 e 1995
            df = df.replace('...', pd.NA)

            # Formata a coluna de datas para o padrão de banco de dados
            df['mes'] = pd.to_datetime(df['mes']).dt.strftime('%Y-%m-%d')
            
            # Adiciona data de ingestão
            df['dt_ingest'] = datetime.now().isoformat()

            # Converte valores pd.NA/NaN para None (NULL do Postgres)
            df = df.where(pd.notnull(df), None)

            registros = df.to_dict(orient='records')

            logging.info(
                f"[cliente_fgv.py] Transformação concluída. "
                f"{len(registros)} registros preparados."
            )

            return registros

        except Exception as e:
            logging.error(f"[cliente_fgv.py] Erro ao processar arquivo com Pandas: {e}")
            return None

