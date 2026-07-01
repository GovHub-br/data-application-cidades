"""
Cliente SFTP para ingestão de dados históricos.

Centraliza a lógica de conexão, listagem, download e processamento
de arquivos do servidor SFTP do MCID.

Uso nas DAGs:
    from cliente_sftp import ClienteSFTP

    cliente = ClienteSFTP(sftp_conn=sftp)
    arquivos = cliente.listar_arquivos_recursivo("/home/fabrica")
    df = cliente.baixar_e_ler("/home/fabrica/GEFUS/arquivo.csv")
"""

import hashlib
import io
import logging
import re
import unicodedata
import zipfile
from stat import S_ISDIR
from typing import List, Optional, Set, Tuple
from airflow.models import Variable

import pandas as pd


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

EXTENSOES_SUPORTADAS = {".csv", ".txt", ".xlsx", ".xls", ".zip"}

DIRS_IGNORAR = {
    "/bin", "/boot", "/dev", "/etc", "/lib", "/lib64",
    "/lost+found", "/proc", "/run", "/sbin", "/snap",
    "/sys", "/usr", "/var", "/root", "/tmp",
}

# Diretórios base a serem varridos no SFTP
DIRS_RAIZ = Variable.get(
    "SFTP_DIRS_RAIZ", 
    default_var=["/home/fabrica", "/home/caixa"], 
    deserialize_json=True
)

MAX_PROFUNDIDADE = 10


class ClienteSFTP:
    """Cliente para interação com o servidor SFTP."""

    def __init__(self, sftp_conn) -> None:
        """
        Args:
            sftp_conn: Conexão SFTP ativa (paramiko.SFTPClient).
        """
        self.sftp = sftp_conn

    # ------------------------------------------------------------------
    # Listagem
    # ------------------------------------------------------------------

    def listar_arquivos_recursivo(
        self,
        caminho: str,
        extensoes: Optional[Set[str]] = None,
        profundidade_max: int = MAX_PROFUNDIDADE,
        _profundidade: int = 0,
    ) -> List[Tuple[str, int, int]]:
        """Percorre o SFTP recursivamente e retorna arquivos com extensão suportada.

        Trata UnicodeDecodeError causado por nomes de arquivo com encoding
        não-UTF-8 (Latin-1) no paramiko.

        Args:
            caminho: Diretório SFTP para iniciar a busca.
            extensoes: Conjunto de extensões aceitas (ex: {'.csv', '.xlsx'}).
                       Se None, usa EXTENSOES_SUPORTADAS.
            profundidade_max: Limite de recursão.

        Returns:
            Lista de tuplas (caminho_completo, tamanho_bytes, mtime).
        """
        if extensoes is None:
            extensoes = EXTENSOES_SUPORTADAS

        resultados: List[Tuple[str, int, int]] = []

        if _profundidade > profundidade_max:
            return resultados

        try:
            itens = self.sftp.listdir_attr(caminho)
        except UnicodeDecodeError:
            logging.warning(
                "Encoding não-UTF-8 em %s — tentando fallback", caminho
            )
            try:
                nomes = self.sftp.listdir(caminho)
            except Exception:
                logging.warning("Fallback também falhou para %s", caminho)
                return resultados

            for nome in nomes:
                caminho_completo = f"{caminho.rstrip('/')}/{nome}"
                try:
                    attr = self.sftp.stat(caminho_completo)
                    if S_ISDIR(attr.st_mode):
                        if caminho_completo not in DIRS_IGNORAR:
                            resultados.extend(
                                self.listar_arquivos_recursivo(
                                    caminho_completo,
                                    extensoes,
                                    profundidade_max,
                                    _profundidade + 1,
                                )
                            )
                    else:
                        if self._tem_extensao_suportada(nome, extensoes):
                            resultados.append(
                                (caminho_completo, attr.st_size or 0, attr.st_mtime or 0)
                            )
                except Exception:
                    pass
            return resultados
        except IOError:
            logging.warning(
                "Sem permissão ou caminho inexistente: %s", caminho
            )
            return resultados

        for item in itens:
            caminho_completo = f"{caminho.rstrip('/')}/{item.filename}"
            if S_ISDIR(item.st_mode):
                if caminho_completo not in DIRS_IGNORAR:
                    resultados.extend(
                        self.listar_arquivos_recursivo(
                            caminho_completo,
                            extensoes,
                            profundidade_max,
                            _profundidade + 1,
                        )
                    )
            else:
                if self._tem_extensao_suportada(item.filename, extensoes):
                    resultados.append(
                        (caminho_completo, item.st_size or 0, item.st_mtime or 0)
                    )

        return resultados

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def baixar_arquivo(self, caminho_remoto: str) -> bytes:
        """Faz download de um arquivo SFTP para memória.

        Args:
            caminho_remoto: Caminho completo do arquivo no SFTP.

        Returns:
            Conteúdo do arquivo em bytes.
        """
        buffer = io.BytesIO()
        self.sftp.getfo(caminho_remoto, buffer)
        buffer.seek(0)
        return buffer.read()

    # ------------------------------------------------------------------
    # Leitura / Parse
    # ------------------------------------------------------------------

    def baixar_e_ler(
        self, caminho_remoto: str
    ) -> Tuple[str, List[Tuple[str, pd.DataFrame]]]:
        """Faz download e lê um arquivo, retornando DataFrames.

        Para arquivos simples (CSV, TXT, XLSX), retorna uma lista com
        uma tupla (nome_do_arquivo, DataFrame).

        Para arquivos ZIP, extrai cada arquivo suportado de dentro do ZIP
        e retorna uma lista de tuplas (nome_do_arquivo_interno, DataFrame).

        Args:
            caminho_remoto: Caminho completo do arquivo no SFTP.

        Returns:
            Tupla contendo (hash_md5, Lista de tuplas (nome_base, DataFrame)).
        """
        conteudo = self.baixar_arquivo(caminho_remoto)
        nome_arquivo = caminho_remoto.rsplit("/", 1)[-1]
        ext = self._extrair_extensao(nome_arquivo)

        file_hash = hashlib.md5(conteudo).hexdigest()

        if ext == ".zip":
            return file_hash, self._ler_zip(conteudo, nome_arquivo)
        else:
            df = self._ler_arquivo_unico(conteudo, nome_arquivo, ext)
            return file_hash, [(nome_arquivo, df)]

    def _ler_arquivo_unico(
        self, conteudo: bytes, nome: str, ext: str
    ) -> pd.DataFrame:
        """Lê um único arquivo e retorna DataFrame."""
        if ext in (".csv", ".txt"):
            return self._ler_csv_txt(conteudo, nome)
        elif ext in (".xlsx", ".xls"):
            return self._ler_excel(conteudo, nome)
        else:
            raise ValueError(f"Extensão não suportada: {ext} ({nome})")

    def _ler_csv_txt(self, conteudo: bytes, nome: str) -> pd.DataFrame:
        """Lê CSV/TXT tentando detectar separador e encoding de forma robusta."""
        # Tenta encodings comuns
        for encoding in ("utf-8", "latin-1", "cp1252"):
            try:
                texto = conteudo.decode(encoding)
                break
            except (UnicodeDecodeError, ValueError):
                continue
        else:
            texto = conteudo.decode("latin-1", errors="replace")

        # Tenta separadores do mais estrito para o mais comum
        separadores = ["|", ";", "\t", ","]
        melhor_df = None

        for sep in separadores:
            try:
                # Tenta ler. Se der erro (ex: C engine expected 2 fields, saw 3), cai no except
                df = pd.read_csv(io.StringIO(texto), sep=sep, dtype=str)
                
                # Se leu sem erro e dividiu em mais de uma coluna, achamos o separador!
                if len(df.columns) > 1:
                    return df
                
                # Se não deu erro mas só tem 1 coluna, guarda como backup
                if melhor_df is None:
                    melhor_df = df
            except Exception:
                continue

        if melhor_df is not None:
            logging.warning("Arquivo %s parseado com 1 coluna. Usando como fallback.", nome)
            return melhor_df

        # Se todos falharem (ex: arquivo com linhas inconsistentes), força ler com ';' pulando erros
        logging.warning("Falha ao detectar delimitador limpo para %s. Forçando ';' e ignorando erros...", nome)
        try:
            return pd.read_csv(
                io.StringIO(texto), sep=";", dtype=str, on_bad_lines="skip"
            )
        except Exception as e:
            logging.error("Falha catastrófica ao ler %s: %s", nome, e)
            raise

    def _ler_excel(self, conteudo: bytes, nome: str) -> pd.DataFrame:
        """Lê arquivo Excel (xlsx/xls)."""
        try:
            df = pd.read_excel(io.BytesIO(conteudo), dtype=str)
        except Exception as e:
            logging.error("Erro ao ler Excel %s: %s", nome, e)
            raise
        return df

    def _ler_zip(
        self, conteudo: bytes, nome_zip: str
    ) -> List[Tuple[str, pd.DataFrame]]:
        """Extrai e lê arquivos suportados de dentro de um ZIP."""
        resultados: List[Tuple[str, pd.DataFrame]] = []

        try:
            with zipfile.ZipFile(io.BytesIO(conteudo)) as zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    nome_interno = info.filename.rsplit("/", 1)[-1]
                    ext = self._extrair_extensao(nome_interno)

                    if ext not in EXTENSOES_SUPORTADAS or ext == ".zip":
                        logging.info(
                            "ZIP %s: ignorando %s (extensão %s)",
                            nome_zip, nome_interno, ext,
                        )
                        continue

                    try:
                        conteudo_interno = zf.read(info.filename)
                        df = self._ler_arquivo_unico(
                            conteudo_interno, nome_interno, ext
                        )
                        resultados.append((nome_interno, df))
                    except Exception as e:
                        logging.error(
                            "ZIP %s: erro ao ler %s: %s",
                            nome_zip, nome_interno, e,
                        )
        except zipfile.BadZipFile:
            logging.error("Arquivo ZIP corrompido: %s", nome_zip)
            raise

        return resultados

    # ------------------------------------------------------------------
    # Nomeação de tabelas
    # ------------------------------------------------------------------

    @staticmethod
    def gerar_nome_tabela(nome_arquivo: str) -> str:
        """Gera nome de tabela Postgres válido a partir do nome do arquivo.

        Regras:
            1. Remove extensão
            2. Remove acentos (ç→c, ã→a, ó→o)
            3. Converte para lowercase
            4. Substitui caracteres não alfanuméricos por _
            5. Colapsa underscores múltiplos
            6. Remove underscores inicial/final
            7. Trunca em 63 caracteres (limite Postgres)
            8. Se começar com dígito, prefixa com '_'

        Args:
            nome_arquivo: Nome do arquivo (sem caminho).

        Returns:
            Nome sanitizado para tabela Postgres.
        """
        # Remove extensão
        nome = nome_arquivo
        for ext in (".xlsx", ".xls", ".csv", ".txt", ".zip", ".gz"):
            if nome.lower().endswith(ext):
                nome = nome[: -len(ext)]
                break

        # Remove acentos
        nfkd = unicodedata.normalize("NFKD", nome)
        nome = "".join(c for c in nfkd if not unicodedata.combining(c))

        # Lowercase
        nome = nome.lower()

        # Substitui caracteres não alfanuméricos por _
        nome = re.sub(r"[^a-z0-9]", "_", nome)

        # Colapsa underscores múltiplos e remove dos extremos
        nome = re.sub(r"_+", "_", nome).strip("_")

        # Prefixo se começa com dígito
        if nome and nome[0].isdigit():
            nome = f"_{nome}"

        # Trunca em 63 chars
        nome = nome[:63]

        return nome

    # ------------------------------------------------------------------
    # Utilitários privados
    # ------------------------------------------------------------------

    @staticmethod
    def _extrair_extensao(nome: str) -> str:
        """Extrai a extensão de um nome de arquivo (lowercase)."""
        ponto = nome.rfind(".")
        if ponto == -1:
            return ""
        return nome[ponto:].lower()

    @staticmethod
    def _tem_extensao_suportada(nome: str, extensoes: Set[str]) -> bool:
        """Verifica se o arquivo tem extensão suportada."""
        ext = ClienteSFTP._extrair_extensao(nome)
        return ext in extensoes
