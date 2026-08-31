import logging
import io
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from cliente_base import ClienteBase, LayoutFonteMudou


class ClienteFipeZap(ClienteBase):
    """
    Cliente para extração de dados do Índice FipeZAP de locação residencial.

    Fonte: https://www.fipe.org.br/pt-br/indices/fipezap/#indice-fipezap-historico
    URL:   https://downloads.fipe.org.br/indices/fipezap/fipezap-serieshistoricas.xlsx

    Colunas extraídas da aba 'Índice FipeZAP':
      col 1  → data_referencia
      col 22 → imoveis_residenciais_locacao_numero_indice_total
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
    COL_NUMERO_INDICE = 22
    COL_VAR_MENSAL = 27
    COL_VAR_ANO = 32

    # Linhas de cabeçalho (0-3) e início dos dados (4)
    LINHA_INICIO_DADOS = 4

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
            "[cliente_fipezap.py] Initialized ClienteFipeZap "
            f"com base_url: {self.BASE_URL}"
        )


    @staticmethod
    def _conferir_coerencia_indice(df: "pd.DataFrame") -> None:
        """Valida que número-índice, variação mensal e variação em 12 meses
        contam a mesma história.

        As três colunas são lidas por POSIÇÃO na planilha (o cabeçalho é
        mesclado e não dá pra casar por nome). O pior modo de falha desta
        fonte é a FIPE inserir ou reordenar uma coluna: a extração continua
        rodando, os valores continuam parecendo percentuais plausíveis, e a
        gente grava a série errada **sem erro nenhum**.

        A defesa é semântica, não posicional — o próprio dado se verifica:

            indice[t] / indice[t-1]  - 1  ==  var_mensal[t]
            indice[t] / indice[t-12] - 1  ==  var_ano[t]

        Se as posições mudarem, essas identidades quebram de imediato.

        Tolerância de 0,15 p.p.: a FIPE publica as variações arredondadas, e
        a série sofre revisão retroativa entre divulgações.
        """
        idx = "imoveis_residenciais_locacao_numero_indice_total"
        vm = "imoveis_residenciais_locacao_var_mensal_total"
        va = "imoveis_residenciais_locacao_var_ano_total"

        d = df.sort_values("data_referencia").reset_index(drop=True)
        TOLERANCIA = 0.0015

        for coluna, defasagem, rotulo in ((vm, 1, "mensal"), (va, 12, "12 meses")):
            esperado = d[idx] / d[idx].shift(defasagem) - 1
            comparavel = esperado.notna() & d[coluna].notna()
            if comparavel.sum() < defasagem + 12:
                continue  # série curta demais pra concluir qualquer coisa
            divergencia = (esperado - d[coluna]).abs()
            fora = (divergencia > TOLERANCIA) & comparavel
            proporcao = fora.sum() / comparavel.sum()
            # exige desacordo generalizado: revisão pontual da FIPE não deve
            # derrubar a ingestão, mas coluna trocada desalinha quase tudo.
            if proporcao > 0.20:
                pior = d.loc[divergencia.idxmax(), "data_referencia"]
                raise LayoutFonteMudou(
                    f"[cliente_fipezap.py] Layout da planilha mudou: em "
                    f"{proporcao:.0%} dos meses a variação {rotulo} não confere "
                    f"com o número-índice (pior caso em {pior}). Conferir as "
                    f"posições COL_NUMERO_INDICE/COL_VAR_MENSAL/COL_VAR_ANO."
                )

    def fetch_and_transform(self) -> Optional[pd.DataFrame]:
        """
        Baixa o XLSX da FipeZAP, extrai as colunas de locação residencial
        e retorna um DataFrame limpo pronto para carga no PostgreSQL.

        O arquivo é baixado em memória e descartado após a extração —
        nenhum arquivo é salvo em disco.

        Returns:
            DataFrame com colunas:
                - data_referencia (str 'yyyy-MM-dd')
                - imoveis_residenciais_locacao_numero_indice_total (float)
                - imoveis_residenciais_locacao_var_mensal_total (float)
                - imoveis_residenciais_locacao_var_ano_total (float)
                - dt_ingest (str ISO 8601)
            Ou None em caso de falha.
        """
        logging.info(
            f"[cliente_fipezap.py] Baixando XLSX de: "
            f"{self.BASE_URL}{self.XLSX_PATH}"
        )

        try:
            _, content = self.request(
                "GET",
                f"{self.BASE_URL}{self.XLSX_PATH}",
                response_type="bytes"
            ) 
        except requests.exceptions.RequestException as e:
            logging.error(
                f"[cliente_fipezap.py] Erro ao baixar o XLSX: {e}"
            )
            return None

        # Guarda os bytes brutos do XLSX p/ a camada raw do data lake (formato nativo).
        self.ultimo_conteudo_xlsx = content

        try:
            df_raw = pd.read_excel(
                io.BytesIO(content),
                sheet_name=self.ABA,
                header=None,
            )

            # Extrai apenas as colunas necessárias a partir do início dos
            # dados. Sem limite superior fixo: um LINHA_FIM_DADOS hardcoded
            # (antes 223) corta silenciosamente os meses mais novos conforme
            # a FIPE vai publicando — o filtro de "linha sem data" logo
            # abaixo já remove o rodapé real, então basta ir até o fim da
            # planilha.
            df = df_raw.iloc[
                self.LINHA_INICIO_DADOS:,
                [self.COL_DATA, self.COL_NUMERO_INDICE, self.COL_VAR_MENSAL, self.COL_VAR_ANO],
            ].copy()

            df.columns = [
                "data_referencia",
                "imoveis_residenciais_locacao_numero_indice_total",
                "imoveis_residenciais_locacao_var_mensal_total",
                "imoveis_residenciais_locacao_var_ano_total",
            ]

            # Remove linhas sem data (rodapé residual)
            df = df[pd.notna(df["data_referencia"])].copy()

            # Normaliza data para string 'yyyy-MM-dd'
            df["data_referencia"] = pd.to_datetime(
                df["data_referencia"]
            ).dt.strftime("%Y-%m-%d")

            # Converte valores para numérico
            for coluna in (
                "imoveis_residenciais_locacao_numero_indice_total",
                "imoveis_residenciais_locacao_var_mensal_total",
                "imoveis_residenciais_locacao_var_ano_total",
            ):
                df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

            self._conferir_coerencia_indice(df)

            df["dt_ingest"] = datetime.now().isoformat()

            logging.info(
                f"[cliente_fipezap.py] Transformação concluída. "
                f"{len(df)} registros | "
                f"De {df['data_referencia'].min()} "
                f"até {df['data_referencia'].max()}"
            )

            return df

        except LayoutFonteMudou:
            # propaga: é diagnóstico acionável, não erro de parse
            raise
        except Exception as e:
            logging.error(
                f"[cliente_fipezap.py] Erro ao processar o XLSX: {e}"
            )
            return None
