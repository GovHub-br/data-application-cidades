import io
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import requests


class ClienteMRV:
    """
    Cliente para consumir a API da MZ Group e extrair os dados de
    Lançamentos da Planilha Interativa da Central de Resultados da MRV.
    """

    # O company_id da MRV na plataforma da MZ
    COMPANY_ID = "4b56353d-d5d9-435f-bf63-dcbf0a6c25d5"
    URL_API = (
        f"https://apicatalog.mziq.com/filemanager/company/{COMPANY_ID}"
        f"/filter/categories/year/meta"
    )

    # Categoria interna que identifica a Planilha Interativa no JSON
    CAT_PLANILHA = "central_de_resultados_planilha_interativa"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/114.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": "https://ri.mrv.com.br",
                "Referer": "https://ri.mrv.com.br/",
            }
        )
        logging.info("[cliente_mrv.py] Initialized ClienteMRV")

    def _buscar_link_planilha_recente(self) -> Optional[str]:
        """
        Consulta a API buscando do ano atual para trás. Retorna a URL (permalink)
        da Planilha Interativa do maior trimestre encontrado no ano mais recente.
        """
        ano_atual = datetime.now().year

        for ano in range(ano_atual, ano_atual - 3, -1):
            logging.info(f"[cliente_mrv.py] Buscando documentos do ano {ano}...")

            payload = {
                "year": str(ano),
                "categories": [self.CAT_PLANILHA],
                "language": "pt_BR",
                "published": True,
            }

            try:
                response = self.session.post(self.URL_API, json=payload, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[cliente_mrv.py] Falha na API da MZ: {e}")
                return None

            dados = response.json()
            documentos = dados.get("data", {}).get("document_metas", [])

            if not documentos:
                logging.info(
                    f"[cliente_mrv.py] Nenhum doc para {ano}, tentando ano anterior."
                )
                continue

            planilhas = [
                doc
                for doc in documentos
                if doc.get("internal_name") == self.CAT_PLANILHA
            ]

            if not planilhas:
                continue

            planilhas.sort(key=lambda x: x.get("file_quarter", 0), reverse=True)

            planilha_mais_recente = planilhas[0]
            trimestre = planilha_mais_recente.get("file_quarter")
            link = planilha_mais_recente.get("permalink")

            if link:
                logging.info(
                    f"[cliente_mrv.py] Planilha encontrada! "
                    f"Referência: {trimestre}T{str(ano)[2:]}"
                )
                return link
            else:
                logging.warning("[cliente_mrv.py] Documento encontrado sem link.")

        logging.error("[cliente_mrv.py] Nenhuma planilha achada nos últimos 3 anos.")
        return None

    def _ler_dados_operacionais(self, link: str) -> Optional[pd.DataFrame]:
        """
        Baixa a planilha em memória e extrai a aba bruta de Dados Operacionais.
        """
        logging.info(f"[cliente_mrv.py] Baixando planilha: {link}")
        try:
            response = self.session.get(link, timeout=20)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"[cliente_mrv.py] Falha ao baixar planilha: {e}")
            return None

        logging.info("[cliente_mrv.py] Lendo aba 'Dados Oper. MRV&Co | Oper.Data'...")
        try:
            arquivo_em_memoria = io.BytesIO(response.content)
            nome_aba = "Dados Oper. MRV&Co | Oper.Data"
            
            # Limpa o binário da requisição
            del response

            # O dtype=str força a leitura de tudo como texto bruto desde a origem.
            df = pd.read_excel(
                arquivo_em_memoria,
                sheet_name=nome_aba,
                header=None,
                dtype=str,
            )

            # Fecha o buffer e limpa memória
            arquivo_em_memoria.close()
            del arquivo_em_memoria

            logging.info(f"[cliente_mrv.py] Aba lida com sucesso! Shape: {df.shape}")
            return df
        except Exception as e:
            logging.error(f"[cliente_mrv.py] Erro ao processar Excel: {e}")
            return None

    def _extrair_lancamentos_incorporacao(
        self, df_bruto: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        """
        Localiza a seção 'Lançamentos %MRV' -> 'MRV Incorporação', extrai as 3 linhas
        de KPIs e transpõe os dados para formato tabular.
        """
        logging.info("[cliente_mrv.py] Iniciando extração do bloco de Lançamentos...")

        try:
            # Pega a linha de índice 1 que possui os cabeçalhos temporais
            cabecalhos = df_bruto.iloc[1].astype(str)

            # Localiza o índice "Lançamentos %MRV"
            col_0_str = df_bruto[0].astype(str)
            mask_secao = col_0_str.str.contains(
                "Lançamentos %MRV", case=False, na=False
            )
            if not mask_secao.any():
                logging.error("[cliente_mrv.py] Seção 'Lançamentos %MRV' não achada.")
                return None
            idx_secao = df_bruto[mask_secao].index[0]

            # Faz um recorte a partir dessa índice e procura o subgrupo
            df_recorte = df_bruto.iloc[idx_secao:]
            col_0_recorte = df_recorte[0].astype(str)
            mask_sub = col_0_recorte.str.contains(
                "MRV Incorporação", case=False, na=False
            )
            if not mask_sub.any():
                logging.error("[cliente_mrv.py] Subgrupo 'MRV Incorporação' não achado")
                return None
            idx_subgrupo = df_recorte[mask_sub].index[0]

            # Isola as 3 linhas de interesse abaixo do subgrupo
            colunas_alvo = [0] + list(range(2, df_bruto.shape[1]))
            df_kpis = df_bruto.iloc[
                idx_subgrupo + 1 : idx_subgrupo + 4, colunas_alvo
            ].copy()

            # Usamos os cabeçalhos como índice antes de transpor
            df_kpis.index = [
                "vgv_lancamentos_milhoes",
                "unidades",
                "preco_medio_unidade_mil",
            ]
            df_kpis.columns = ["periodo"] + cabecalhos.iloc[2:].tolist()

            # Remove a primeira coluna (nomes em português/inglês)
            df_kpis = df_kpis.drop(columns=["periodo"])

            # Transpõe a matriz (linhas viram colunas e vice-versa)
            df_tabela = df_kpis.T.reset_index()

            # Força o nome das 4 colunas resultantes de forma cravada
            df_tabela.columns = [
                "periodo",
                "vgv_lancamentos_milhoes",
                "unidades",
                "preco_medio_unidade_mil",
            ]

            # Limpeza do período (1T26 / 1Q26 -> 1T26)
            df_tabela["periodo"] = df_tabela["periodo"].apply(
                lambda x: x.split(" / ")[0].strip() if " / " in str(x) else str(x)
            )
            df_tabela["periodo"] = df_tabela["periodo"].str.replace(
                ".0", "", regex=False
            )

            # Remove linhas nulas ('nan' string do astype)
            df_tabela = df_tabela[
                ~df_tabela["vgv_lancamentos_milhoes"].isin(["nan", "None", ""])
            ]

            logging.info(
                f"[cliente_mrv.py] Extração tabular concluída. "
                f"Shape final: {df_tabela.shape}"
            )
            return df_tabela

        except Exception as e:
            logging.error(f"[cliente_mrv.py] Erro ao fatiar o dataframe Pandas: {e}")
            return None

    def fetch_dados_lancamentos(self) -> Optional[list[dict]]:
        """
        Orquestrador do fluxo completo de busca, extração e transformação
        dos dados operacionais de lançamentos da MRV.

        Returns:
            Lista de dicionários contendo os registros da tabela ou None em falha.
        """
        link_recente = self._buscar_link_planilha_recente()
        if not link_recente:
            return None

        df_bruto = self._ler_dados_operacionais(link_recente)
        if df_bruto is None:
            return None

        df_tabela = self._extrair_lancamentos_incorporacao(df_bruto)
        if df_tabela is None:
            return None

        # Adiciona data de ingestão padrão da camada raw
        df_tabela["dt_ingest"] = datetime.now().isoformat()

        # Converte valores pd.NA/NaN para None (NULL do Postgres)
        df_tabela = df_tabela.where(pd.notnull(df_tabela), None)

        registros = df_tabela.to_dict(orient="records")

        logging.info(
            f"[cliente_mrv.py] Transformação final concluída. "
            f"{len(registros)} registros preparados para ingestão."
        )

        return registros

