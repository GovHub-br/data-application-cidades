import logging
import os
import tempfile
from stat import S_ISDIR
from typing import Iterator, Optional, Tuple

import paramiko

# Erros que indicam queda de conexão (justificam reconectar e retentar).
SOCKET_ERRORS = (paramiko.SSHException, EOFError, OSError, ConnectionResetError)
_CONN_ERROR_STRINGS = (
    "garbage packet",
    "eof",
    "connection reset",
    "broken pipe",
    "timed out",
    "channel closed",
    "socket is closed",
)


def _extensao(nome: str) -> str:
    pos = nome.rfind(".")
    return nome[pos:].lower() if pos != -1 else ""


class ClienteSftp:
    """Cliente de I/O do servidor SFTP (camada de conexão do estágio de ingestão).

    Encapsula conexão paramiko com keepalive/timeout, reconexão, listagem recursiva e
    download para disco. A lógica de ingestão (zip/multivolume, nomeação, controle) mora
    no script `sftp_para_minio.py`, que chama este cliente.
    """

    KEEPALIVE_S = 30
    TIMEOUT_S = 300
    MAX_PROFUNDIDADE = 10

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        self.host = host or os.environ["SFTP_HOST"]
        self.port = int(port or os.environ.get("SFTP_PORT", 22))
        self.user = user or os.environ["SFTP_USER"]
        self.password = password or os.environ.get("SFTP_PASSWORD", "")
        self.transport: Optional[paramiko.Transport] = None
        self.sftp: Optional[paramiko.SFTPClient] = None

    # Conexão
    def conectar(self) -> paramiko.SFTPClient:
        if not self.host:
            raise ValueError("SFTP_HOST não encontrada no ambiente")
        if not self.password:
            raise ValueError("SFTP_PASSWORD não encontrada no ambiente")
        transport = paramiko.Transport((self.host, self.port))
        transport.connect(username=self.user, password=self.password)
        transport.set_keepalive(self.KEEPALIVE_S)
        transport.sock.settimeout(self.TIMEOUT_S)
        sftp = paramiko.SFTPClient.from_transport(transport)
        if sftp is None:
            transport.close()
            raise RuntimeError("Falha ao abrir sessão SFTP")
        self.transport, self.sftp = transport, sftp
        logging.info(
            f"[cliente_sftp.py] Conectado a {self.user}@{self.host}:{self.port}"
        )
        return sftp

    def fechar(self) -> None:
        for obj in (self.sftp, self.transport):
            try:
                if obj is not None:
                    obj.close()
            except Exception:
                pass
        self.sftp = self.transport = None

    def reconectar(self) -> paramiko.SFTPClient:
        self.fechar()
        return self.conectar()

    def __enter__(self) -> "ClienteSftp":
        self.conectar()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.fechar()

    @staticmethod
    def is_conn_error(e: Exception) -> bool:
        return isinstance(e, SOCKET_ERRORS) or any(
            s in str(e).lower() for s in _CONN_ERROR_STRINGS
        )

    # Operações
    def listar_arquivos(
        self,
        caminho: str,
        extensoes: Optional[set] = None,
        profundidade: int = 0,
        pastas_ignorar: Optional[set] = None,
    ) -> Iterator[Tuple[str, int, int]]:
        """Gera (caminho, tamanho, mtime) para cada arquivo suportado, recursivo."""
        if self.sftp is None:
            raise RuntimeError("SFTP não conectado — chame conectar() primeiro")
        if profundidade > self.MAX_PROFUNDIDADE:
            return
        pastas_ignorar = pastas_ignorar or set()
        try:
            itens = self.sftp.listdir_attr(caminho)
        except IOError:
            logging.warning(f"[cliente_sftp.py] Sem acesso ou inexistente: {caminho}")
            return

        for item in itens:
            caminho_completo = f"{caminho.rstrip('/')}/{item.filename}"
            if S_ISDIR(item.st_mode):
                if item.filename in pastas_ignorar:
                    logging.info(
                        f"[cliente_sftp.py] Ignorando pasta excluída: {caminho_completo}"
                    )
                    continue
                yield from self.listar_arquivos(
                    caminho_completo, extensoes, profundidade + 1, pastas_ignorar
                )
            elif extensoes is None or _extensao(item.filename) in extensoes:
                yield caminho_completo, item.st_size or 0, item.st_mtime or 0

    def baixar_para_tempfile(
        self, caminho: str, tmpdir: Optional[str] = None
    ) -> str:
        """Baixa para disco — evita OOM em arquivos grandes."""
        if self.sftp is None:
            raise RuntimeError("SFTP não conectado — chame conectar() primeiro")
        from pathlib import Path

        tmp = tempfile.NamedTemporaryFile(
            delete=False, suffix=Path(caminho).suffix, dir=tmpdir
        )
        try:
            self.sftp.getfo(caminho, tmp)
            tmp.flush()
        finally:
            tmp.close()
        return tmp.name
