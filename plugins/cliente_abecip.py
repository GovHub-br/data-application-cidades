import io
import logging
import re
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from cliente_base import ClienteBase, LayoutFonteMudou


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


    @staticmethod
    def _conferir_poupanca(df: "pd.DataFrame") -> None:
        """Valida que os campos da poupança são internamente coerentes.

        As colunas são lidas por POSIÇÃO (`COLUNAS_IDX`), então uma inserção
        ou reordenação na origem faria a gente gravar série trocada sem erro
        nenhum. As identidades abaixo amarram as posições ao significado:

            captacao_liquida = deposito - retirada          (vale em 535/535)
            saldo[t] = saldo[t-1] + captacao + rendimento   (vale em 526/534)

        A primeira é exata e serve como checagem dura. A segunda falha em 8
        meses ao longo de 44 anos de série (mudança de metodologia na
        origem), então é avaliada por proporção — não pode derrubar a
        ingestão por causa de exceção histórica, mas desalinha em massa se as
        colunas trocarem.
        """
        d = df.sort_values("data_referencia").reset_index(drop=True)

        cap = (d["deposito"] - d["retirada"] - d["captacao_liquida_valor"]).abs()
        comparavel = cap.notna()
        if comparavel.sum() and (cap[comparavel] > 0.01).mean() > 0.01:
            raise LayoutFonteMudou(
                "[cliente_abecip.py] Layout da aba de poupança mudou: "
                "`captacao_liquida` deixou de ser `deposito - retirada` em "
                f"{(cap[comparavel] > 0.01).mean():.0%} das linhas. Conferir COLUNAS_IDX."
            )

        saldo = (
            d["saldo"].shift(1) + d["captacao_liquida_valor"] + d["rendimento"] - d["saldo"]
        ).abs()
        comparavel2 = saldo.notna()
        if comparavel2.sum() and (saldo[comparavel2] > 1.0).mean() > 0.10:
            raise LayoutFonteMudou(
                "[cliente_abecip.py] Layout da aba de poupança mudou: a evolução "
                f"do saldo não fecha em {(saldo[comparavel2] > 1.0).mean():.0%} dos "
                "meses. Conferir COLUNAS_IDX."
            )

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

        # Guarda os bytes brutos do XLSX p/ a camada raw do data lake (formato nativo).
        self.ultimo_conteudo_xlsx = content

        try:
            # Nome exato primeiro; prefixo só como plano B. Nome fixo quebra
            # em silêncio quando a origem renomeia (aconteceu com a MRV em
            # 2026-08), mas prefixo solto é pior: esta planilha tem as abas
            # 'SBPE' E 'SBPE_Mensal', e casar por prefixo pegava a errada —
            # devolvendo zero registro sem erro. Se o plano B ficar ambíguo,
            # falha alto em vez de escolher por conta própria.
            planilha = pd.ExcelFile(io.BytesIO(content))
            abas = planilha.sheet_names
            if self.ABA_POUPANCA in abas:
                nome_aba = self.ABA_POUPANCA
            else:
                candidatas = [
                    a for a in abas
                    if a.strip().lower().startswith(self.ABA_POUPANCA.lower())
                ]
                if len(candidatas) != 1:
                    raise LayoutFonteMudou(
                        f"[cliente_abecip.py] Aba {self.ABA_POUPANCA!r} não encontrada e "
                        f"o prefixo casou com {len(candidatas)} abas ({candidatas}). "
                        f"Abas disponíveis: {abas}"
                    )
                nome_aba = candidatas[0]
                logging.warning(
                    "[cliente_abecip.py] Aba %r não existe mais; usando %r por prefixo",
                    self.ABA_POUPANCA, nome_aba,
                )
            df_raw = pd.read_excel(planilha, sheet_name=nome_aba, header=None)

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

            self._conferir_poupanca(df)

            logging.info(
                f"[cliente_abecip.py] Poupança: {len(df)} registros | "
                f"De {df['data_referencia'].min()} "
                f"até {df['data_referencia'].max()}"
            )

            return df

        except LayoutFonteMudou:
            # propaga: é diagnóstico acionável, não erro de parse
            raise
        except Exception as e:
            logging.error(
                f"[cliente_abecip.py] Erro ao processar XLSX de poupança: {e}"
            )
            return None

    # ------------------------------------------------------------------
    # Financiamentos SBPE por modalidade (Construção / Aquisição)
    # ------------------------------------------------------------------

    PAGINA_FINANCIAMENTO = "/credito-imobiliario/indicadores/financiamento"
    ABA_FINANCIAMENTO = "BD_Unidades"

    #: Ordem das colunas na aba (0-based). Não é lida do cabeçalho porque ele
    #: é mesclado em duas linhas ("Unidades Financiadas" / "Valores em R$"
    #: acima de "Construção | Aquisição | Total"), o que o pandas não resolve
    #: sozinho. A posição é conferida em tempo de execução pela identidade
    #: Total == Construção + Aquisição — ver `_conferir_totais`.
    COLUNAS_FIN_NOMES = [
        "data_referencia",
        "unidades_construcao",
        "unidades_aquisicao",
        "unidades_total",
        "valor_construcao_milhoes",
        "valor_aquisicao_milhoes",
        "valor_total_milhoes",
    ]

    @staticmethod
    def _conferir_totais(df: pd.DataFrame) -> None:
        """Valida que Total == Construção + Aquisição nas duas métricas.

        Serve de guarda contra o pior modo de falha desta fonte: a ABECIP
        inserir ou reordenar uma coluna e a extração passar a ler a série
        errada **sem erro nenhum**. Se a identidade quebrar, é porque as
        posições mudaram — melhor falhar alto do que gravar dado trocado.
        """
        for total, partes in (
            ("unidades_total", ("unidades_construcao", "unidades_aquisicao")),
            ("valor_total_milhoes", ("valor_construcao_milhoes", "valor_aquisicao_milhoes")),
        ):
            soma = df[list(partes)].sum(axis=1)
            # tolerância relativa: os valores em R$ têm arredondamento na origem
            divergente = ((df[total] - soma).abs() > (df[total].abs() * 0.001 + 1)).sum()
            if divergente:
                raise LayoutFonteMudou(
                    f"[cliente_abecip.py] Layout da aba mudou: {divergente} linhas "
                    f"onde {total} != {' + '.join(partes)}. Conferir posição das "
                    f"colunas em {ClienteAbecip.ABA_FINANCIAMENTO}."
                )

    def fetch_and_transform_financiamentos(self) -> Optional[pd.DataFrame]:
        """Série mensal de financiamentos SBPE por modalidade.

        Fonte: aba `BD_Unidades` do XLSX de unidades da página de
        financiamento da ABECIP. Traz unidades e valores (R$ milhões) para
        Construção, Aquisição e Total, desde 2002.

        É a fonte do indicador "Financiamentos Habitacionais (UH) — SBPE
        Const." do boletim: a soma trimestral de `unidades_construcao` bate
        EXATO com o publicado em 1T2025 (19.130), 3T2025 (43.782), 4T2025
        (47.766) e 1T2026 (47.609), e o acumulado de 12 meses de mar/2026
        (161.338) também.
        """
        url = self._get_xlsx_url(self.PAGINA_FINANCIAMENTO, "unidades")
        if not url:
            return None

        conteudo = self._download_xlsx(url)
        if not conteudo:
            return None
        self.ultimo_conteudo_xlsx_financiamentos = conteudo

        try:
            planilha = pd.ExcelFile(io.BytesIO(conteudo))
            abas = [a for a in planilha.sheet_names if a.strip().lower().startswith("bd_unidades")]
            if not abas:
                raise ValueError(
                    f"aba de unidades não encontrada; abas: {planilha.sheet_names}"
                )

            bruto = pd.read_excel(planilha, sheet_name=abas[0], header=None)

            # O cabeçalho ocupa linhas mescladas; em vez de fixar a linha de
            # início, fica só o que tem data válida na primeira coluna.
            df = bruto.iloc[:, : len(self.COLUNAS_FIN_NOMES)].copy()
            df.columns = self.COLUNAS_FIN_NOMES
            df["data_referencia"] = pd.to_datetime(
                df["data_referencia"], errors="coerce"
            )
            df = df[df["data_referencia"].notna()].copy()

            for coluna in self.COLUNAS_FIN_NOMES[1:]:
                df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

            # meses futuros já vêm como linha vazia na planilha
            df = df[df["unidades_total"].notna()].copy()

            self._conferir_totais(df)

            df["data_referencia"] = df["data_referencia"].dt.strftime("%Y-%m-%d")
            df["dt_ingest"] = datetime.now().isoformat()
            df["fonte"] = "ABECIP"

            logging.info(
                f"[cliente_abecip.py] Financiamentos: {len(df)} registros | "
                f"De {df['data_referencia'].min()} até {df['data_referencia'].max()}"
            )
            return df

        except LayoutFonteMudou:
            # propaga: é diagnóstico acionável, não erro de parse
            raise
        except Exception as e:
            logging.error(
                f"[cliente_abecip.py] Erro ao processar XLSX de financiamentos: {e}"
            )
            return None
