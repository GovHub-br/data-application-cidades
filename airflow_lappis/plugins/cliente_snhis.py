import logging
import io
import logging
import os
import subprocess
import pandas as pd
from datetime import datetime
import re
from typing import List, Dict, Any, Optional
from cliente_base import ClienteBase

class ClienteSnhis(ClienteBase):
    """
    Cliente para extração de dados de Regularidade dos Entes (SNHIS)
    diretamente do portal gov.br.
    """

    def __init__(self, headers: Optional[dict] = None) -> None:
        if not headers:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "*/*"
            }

        super().__init__(base_url="https://www.gov.br", headers=headers)

    def get_latest_regularidade_url(self) -> str:
        """
        Busca a URL mais recente de Regularidade dos Entes na página de Bases de Dados.
        Garante a captura de arquivos de 2026 através de ordenação cronológica.
        """
        # URL da página de Bases de Dados (onde o arquivo de 2026 foi confirmado)
        page_url = "https://www.gov.br/cidades/pt-br/acesso-a-informacao/acoes-e-programas/habitacao/programa-minha-casa-minha-vida/bases-de-dados-do-programa-minha-casa-minha-vida"
        
        logging.info(f"[ClienteSnhis] Buscando links em: {page_url}")
        
        response = self.client.get(page_url, timeout=30.0)
        response.raise_for_status()

        # 1. Tenta capturar links absolutos (com https://...)
        pattern = r'https?://[^\s"<>]+SNHIS_REGULARIDADE_ENTES_\d+\.xls'
        matches = re.findall(pattern, response.text)

        # 2. Se não achar, tenta capturar links relativos (que começam com /cidades/...)
        if not matches:
            relative_pattern = r'\/cidades\/[^\s"<>]+SNHIS_REGULARIDADE_ENTES_\d+\.xls'
            rel_matches = re.findall(relative_pattern, response.text)
            # Normaliza os links relativos para URLs completas
            matches = [f"https://www.gov.br{m}" for m in rel_matches]

        if not matches:
            raise Exception("Nenhum arquivo de regularidade SNHIS encontrado na página de Bases de Dados.")

        # 3. Função de ordenação: Converte DDMMYYYY para YYYYMMDD para comparar datas corretamente
        def sort_by_date(url: str):
            date_match = re.search(r'(\d{8})', url)
            if date_match:
                d = date_match.group(1) # Ex: 09022026
                # Retorna 20260209 (Garante que 2026 > 2025)
                return d[4:] + d[2:4] + d[0:2]
            return "00000000"

        # Ordena e pega o último (mais recente cronologicamente)
        matches.sort(key=sort_by_date)
        latest_url = matches[-1]

        logging.info(f"[ClienteSnhis] Link mais recente identificado: {latest_url}")
        return latest_url
    def get_regularidade_entes(self) -> List[Dict[str, Any]]:
        """
        Baixa o arquivo mais recente de regularidade dos entes
        e retorna como lista de dicts.
        """

        full_url = self.get_latest_regularidade_url()
        logging.info(f"[ClienteSnhis] Baixando arquivo: {full_url}")


        response = self.client.get(full_url, timeout=120.0)
        response.raise_for_status()


        try:
            df = pd.read_excel(io.BytesIO(response.content))
            df = df.where(pd.notna(df), None) # Trata NaNs 
            return df.to_dict(orient="records")
        except Exception as e:
            logging.error(f"[ClienteSnhis] Erro ao ler Excel: {e}")
            raise

    def get_latest_fgts_url(self) -> str:
        """
        Busca na página de bases de dados a URL do arquivo RAR mais recente.
        """
        page_url = "https://www.gov.br/cidades/pt-br/acesso-a-informacao/acoes-e-programas/habitacao/programa-minha-casa-minha-vida/bases-de-dados-do-programa-minha-casa-minha-vida"
        response = self.client.get(page_url, timeout=30.0)
        response.raise_for_status()
        
        # Regex para capturar o padrão específico de URL
        pattern = r'https://www\.cidades\.gov\.br/images/stories/ArquivosSNH/ArquivosZIP/dados_abertos_FGTS_ANALITICO_\d+\.rar'
        matches = re.findall(pattern, response.text)
        
        if not matches:
            matches = re.findall(r'https?://[^\s"<>]+FGTS_ANALITICO[^\s"<>]*\.rar', response.text)
            
        if not matches:
            raise Exception("Nenhum arquivo FGTS encontrado na página.")
        
        # Retorna o último link (ordem cronológica costuma ser a última na página)
        logging.info(f"[ClienteSnhis] Arquivos encontrados: {matches}")
        return matches[-1]

    def download_and_extract_fgts(self, target_dir: str) -> str:
        """
        Baixa, extrai e retorna o caminho para o arquivo extraído.
        """
        full_url = self.get_latest_fgts_url()
        logging.info(f"[ClienteSnhis] Iniciando download: {full_url}")

        rar_path = os.path.join(target_dir, "fgts_downloaded.rar")

        # 1. Download em stream 
        with self.client.stream("GET", full_url, timeout=900.0) as r:
            r.raise_for_status()
            with open(rar_path, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=16384): # Aumentado para 16KB
                    f.write(chunk)

        # 2. Verificação de integridade básica
        file_size = os.path.getsize(rar_path)
        logging.info(f"[ClienteSnhis] Download concluído. Tamanho: {file_size} bytes")

        # 3. Extração com utilitários de sistema
        logging.info(f"[ClienteSnhis] Extraindo {rar_path} para {target_dir}...")
        
        extracted = False
        errors = []


        # Tentar com bsdtar (incluso no libarchive, que frequentemente lida com rar de boa)
        if not extracted:
            try:
                subprocess.run(
                    ["bsdtar", "-xf", rar_path, "-C", target_dir],
                    check=True, capture_output=True, text=True
                )
                logging.info("[ClienteSnhis] Extração concluída com sucesso via bsdtar.")
                extracted = True
            except subprocess.CalledProcessError as e:
                errors.append(f"bsdtar erro: {e.stderr}")
            except Exception as e:
                errors.append(f"bsdtar erro: {e}")

        if not extracted:
            logging.error(f"Todas as tentativas de extração falharam. Erros: {errors}")
            raise RuntimeError(f"Falha na extração do RAR. Verifique se unrar ou um 7z compatível com RAR5 está instalado. Erros: {errors}")

        # 4. Localiza o arquivo extraído
        for file in os.listdir(target_dir):
            file_upper = file.upper()
            if file.lower().endswith((".csv", ".xls", ".xlsx")) and "ANALITICO" in file_upper:
                return os.path.join(target_dir, file)
        
        raise FileNotFoundError("O arquivo RAR foi extraído, mas nenhum CSV ou XLS correspondente foi encontrado.")