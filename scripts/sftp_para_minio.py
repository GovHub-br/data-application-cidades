# scripts/sftp_para_minio.py

"""
Ingestão de arquivos do SFTP para o MinIO (camada raw).

Percorre Analise_SNH e GEFUS em /home/fabrica, baixa todos os arquivos
suportados (CSV, TXT, XLSX, XLS, ZIP — expandindo ZIPs internamente) e os sobe
para raw/ no MinIO com o nome original do arquivo.

Colisões de nome recebem sufixo contador: arquivo_0000.csv, arquivo_0001.csv, ...

Controle de reprocessamento via tabela sftp._ingest_minio_log no PostgreSQL.
Arquivos são processados do menor para o maior: Analise_SNH primeiro, depois GEFUS.
"""

import hashlib
import itertools
import logging
import os
import sys
import tempfile
import threading
import time
import zipfile
from datetime import datetime
from pathlib import Path
from stat import S_ISDIR
from typing import Iterator, Optional, Tuple

import boto3
from boto3.s3.transfer import TransferConfig
import paramiko
import psycopg2
from dotenv import load_dotenv

load_dotenv()

SFTP_HOST     = os.environ["SFTP_HOST"]
SFTP_PORT     = int(os.environ.get("SFTP_PORT", 22))
SFTP_USER     = os.environ["SFTP_USER"]
SFTP_PASSWORD = os.environ["SFTP_PASSWORD"]

MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET     = os.environ["MINIO_BUCKET"]

PG_HOST     = os.environ["DB_DW_HOST_MCID"]
PG_PORT     = int(os.environ.get("DB_DW_PORT_MCID", 5432))
PG_USER     = os.environ["DB_DW_USER_MCID"]
PG_PASSWORD = os.environ["DB_DW_PASSWORD_MCID"]
PG_DBNAME   = os.environ["DB_DW_DBNAME_MCID"]

SCHEMA    = os.environ.get("SFTP_SCHEMA", "sftp")
LOG_TABLE = "_ingest_minio_log"

DIRS_RAIZ             = ["/home/fabrica"]
EXTENSOES_SUPORTADAS  = {".csv", ".txt", ".xlsx", ".xls", ".zip"}
MAX_PROFUNDIDADE      = 10
PASTAS_IGNORAR        = {"CadUnico"}
PASTAS_LISTAR_PREVIEW = ["Analise_SNH", "GEFUS"]

SOCKET_ERRORS = (paramiko.SSHException, EOFError, OSError, ConnectionResetError)
_CONN_ERROR_STRINGS = (
    "garbage packet", "eof", "connection reset",
    "broken pipe", "timed out", "channel closed", "socket is closed",
)

_LOG_FILE = (
    Path(__file__).parent
    / f"sftp_ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
_formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

logging.root.setLevel(logging.INFO)
logging.root.addHandler(
    (lambda h: (h.setFormatter(_formatter), h)[1])(
        logging.FileHandler(_LOG_FILE, encoding="utf-8")
    )
)
logging.root.addHandler(
    (lambda h: (h.setFormatter(_formatter), h)[1])(
        logging.StreamHandler(sys.stderr)
    )
)

log = logging.getLogger(__name__)

class _Spinner:
    _FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

    def __init__(self):
        self._ciclo  = itertools.cycle(self._FRAMES)
        self._thread: Optional[threading.Thread] = None
        self._ativo  = False
        self._lock   = threading.Lock()
        self._msg    = ""

    def iniciar(self, mensagem: str) -> None:
        with self._lock:
            self._msg = mensagem
        self._ativo = True
        self._thread = threading.Thread(target=self._girar, daemon=True)
        self._thread.start()

    def atualizar(self, mensagem: str) -> None:
        with self._lock:
            self._msg = mensagem

    def parar(self, ok: bool = True) -> None:
        self._ativo = False
        if self._thread:
            self._thread.join()
        icone = "✓" if ok else "✗"
        with self._lock:
            msg = self._msg
        sys.stdout.write(f"\r{icone} {msg}\n")
        sys.stdout.flush()

    def _girar(self) -> None:
        while self._ativo:
            with self._lock:
                msg = self._msg
            sys.stdout.write(f"\r{next(self._ciclo)} {msg}")
            sys.stdout.flush()
            time.sleep(0.1)


spinner = _Spinner()

def _conn_str() -> str:
    return (
        f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )


def _criar_schema_e_log(conn_str: str) -> None:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")

            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (SCHEMA, LOG_TABLE))
            colunas_existentes = {row[0] for row in cur.fetchall()}

            if colunas_existentes and "minio_key" not in colunas_existentes:
                log.warning(
                    "Tabela %s.%s existe com colunas antigas — recriando...",
                    SCHEMA, LOG_TABLE,
                )
                cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{LOG_TABLE};")

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.{LOG_TABLE} (
                    id            SERIAL PRIMARY KEY,
                    sftp_path     TEXT NOT NULL,
                    file_name     TEXT NOT NULL,
                    file_size     BIGINT,
                    file_mtime    TIMESTAMPTZ,
                    file_hash     TEXT,
                    minio_key     TEXT,
                    status        TEXT DEFAULT 'pending',
                    error_message TEXT,
                    started_at    TIMESTAMPTZ,
                    finished_at   TIMESTAMPTZ,
                    created_at    TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (sftp_path, file_hash)
                );
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_ingest_minio_log_sftp_path
                ON {SCHEMA}.{LOG_TABLE} (sftp_path);
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_ingest_minio_log_status
                ON {SCHEMA}.{LOG_TABLE} (status);
            """)
            # Índice parcial para deduplicar erros sem hash (NULL != NULL no UNIQUE padrão)
            cur.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ingest_minio_log_sftp_null_hash
                ON {SCHEMA}.{LOG_TABLE} (sftp_path) WHERE file_hash IS NULL;
            """)
            conn.commit()
    log.info("Schema '%s' e tabela '%s' garantidos.", SCHEMA, LOG_TABLE)


def _obter_ja_inseridos(conn_str: str) -> set:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT sftp_path FROM {SCHEMA}.{LOG_TABLE}
                WHERE status = 'success'
            """)
            return {row[0] for row in cur.fetchall()}


def _obter_nomes_usados(conn_str: str) -> set:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT file_name FROM {SCHEMA}.{LOG_TABLE}
                WHERE status = 'success'
            """)
            return {row[0] for row in cur.fetchall()}


def _registrar_ingest(
    conn_str: str,
    sftp_path: str,
    file_name: str,
    minio_key: Optional[str],
    file_size: int,
    file_mtime: Optional[int],
    file_hash: Optional[str],
    status: str,
    error_message: Optional[str] = None,
) -> None:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            if status == "success":
                # Remove registros de erro anteriores para evitar duplicatas de hash NULL vs hash calculado
                cur.execute(
                    f"DELETE FROM {SCHEMA}.{LOG_TABLE} WHERE sftp_path = %s AND status = 'error'",
                    (sftp_path,),
                )

            if file_hash is not None:
                conflict_clause = "ON CONFLICT (sftp_path, file_hash)"
            else:
                # NULL != NULL no UNIQUE padrão — usa índice parcial dedicado
                conflict_clause = "ON CONFLICT (sftp_path) WHERE file_hash IS NULL"

            cur.execute(
                f"""
                INSERT INTO {SCHEMA}.{LOG_TABLE}
                    (sftp_path, file_name, file_size, file_mtime, file_hash,
                     minio_key, status, error_message, started_at, finished_at)
                VALUES (%s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, NOW(), NOW())
                {conflict_clause}
                DO UPDATE SET
                    status        = EXCLUDED.status,
                    minio_key     = EXCLUDED.minio_key,
                    file_name     = EXCLUDED.file_name,
                    file_size     = EXCLUDED.file_size,
                    error_message = EXCLUDED.error_message,
                    finished_at   = NOW()
                """,
                (
                    sftp_path, file_name, file_size, file_mtime, file_hash,
                    minio_key, status, error_message,
                ),
            )
            conn.commit()

def conectar_sftp() -> Tuple[paramiko.Transport, paramiko.SFTPClient]:
    if not SFTP_HOST:
        raise ValueError("SFTP_HOST não encontrada no .env")
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
    transport.set_keepalive(30)
    transport.sock.settimeout(300)
    sftp = paramiko.SFTPClient.from_transport(transport)
    if sftp is None:
        raise RuntimeError("Falha ao abrir sessão SFTP")
    return transport, sftp


def _is_conn_error(e: Exception) -> bool:
    return isinstance(e, SOCKET_ERRORS) or any(
        s in str(e).lower() for s in _CONN_ERROR_STRINGS
    )


def _extensao(nome: str) -> str:
    pos = nome.rfind(".")
    return nome[pos:].lower() if pos != -1 else ""


def listar_arquivos(
    sftp: paramiko.SFTPClient,
    caminho: str,
    profundidade: int = 0,
) -> Iterator[Tuple[str, int, int]]:
    """Gera (caminho_completo, tamanho, mtime) para cada arquivo suportado."""
    if profundidade > MAX_PROFUNDIDADE:
        return
    try:
        itens = sftp.listdir_attr(caminho)
    except IOError:
        log.warning("Sem acesso ou inexistente: %s", caminho)
        return

    for item in itens:
        caminho_completo = f"{caminho.rstrip('/')}/{item.filename}"
        if S_ISDIR(item.st_mode):
            if item.filename in PASTAS_IGNORAR:
                log.info("Ignorando pasta excluída: %s", caminho_completo)
                continue
            yield from listar_arquivos(sftp, caminho_completo, profundidade + 1)
        elif _extensao(item.filename) in EXTENSOES_SUPORTADAS:
            yield caminho_completo, item.st_size or 0, item.st_mtime or 0


def baixar_para_tempfile(sftp: paramiko.SFTPClient, caminho: str) -> str:
    """Baixa para disco — evita OOM em arquivos grandes."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(caminho).suffix)
    try:
        sftp.getfo(caminho, tmp)
        tmp.flush()
    finally:
        tmp.close()
    return tmp.name

def format_size(size_bytes: int) -> str:
    value: float = size_bytes
    for unit in ["B", "KB", "MB", "GB"]:
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def _compute_hash(file_path: str) -> str:
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            md5.update(chunk)
    return md5.hexdigest()


def gerar_nome_unico(nome: str, usados: set) -> str:
    if nome not in usados:
        return nome
    pos = nome.rfind(".")
    stem, ext = (nome[:pos], nome[pos:]) if pos != -1 else (nome, "")
    contador = 0
    while f"{stem}_{contador:04d}{ext}" in usados:
        contador += 1
    return f"{stem}_{contador:04d}{ext}"


def _label(nome: str, tamanho: int, idx: int, total: int) -> str:
    return f"[{idx}/{total}] {nome} ({format_size(tamanho)})"

def criar_cliente_minio():
    endpoint = MINIO_ENDPOINT
    if not endpoint.startswith("http"):
        endpoint = f"http://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def _garantir_bucket(s3) -> None:
    """Cria o bucket se não existir. A pasta raw/ surge no primeiro upload."""
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        log.info("Bucket '%s' já existe.", MINIO_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        log.info("Bucket '%s' criado.", MINIO_BUCKET)


def _abortar_multiparts_incompletos(s3) -> None:
    """Aborta todos os multipart uploads incompletos no bucket — evita locks no MinIO."""
    abortados = 0
    paginator = s3.get_paginator("list_multipart_uploads")
    try:
        for page in paginator.paginate(Bucket=MINIO_BUCKET):
            for upload in page.get("Uploads", []):
                s3.abort_multipart_upload(
                    Bucket=MINIO_BUCKET,
                    Key=upload["Key"],
                    UploadId=upload["UploadId"],
                )
                abortados += 1
    except Exception as e:
        log.warning("Não foi possível listar multipart uploads: %s", e)
        return
    if abortados:
        log.info("%d multipart upload(s) incompleto(s) abortado(s).", abortados)
    else:
        log.info("Nenhum multipart upload incompleto encontrado.")


def _abortar_multiparts_chave(s3, key: str) -> None:
    """Aborta uploads incompletos de uma chave específica antes de retentar."""
    try:
        paginator = s3.get_paginator("list_multipart_uploads")
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=key):
            for upload in page.get("Uploads", []):
                if upload["Key"] == key:
                    s3.abort_multipart_upload(
                        Bucket=MINIO_BUCKET,
                        Key=upload["Key"],
                        UploadId=upload["UploadId"],
                    )
    except Exception:
        pass


_TRANSFER_CONFIG = TransferConfig(
    multipart_chunksize=5 * 1024 * 1024,  # 5 MB por parte (mínimo S3)
    max_concurrency=1,                     # upload serial — mais estável em conexões instáveis
)


def subir_para_minio(s3, tmp_path: str, nome_destino: str) -> str:
    key = f"raw/{nome_destino}"
    for tentativa in range(3):
        try:
            with open(tmp_path, "rb") as f:
                s3.upload_fileobj(f, MINIO_BUCKET, key, Config=_TRANSFER_CONFIG)
            return key
        except Exception as e:
            if tentativa == 2:
                raise
            espera = 15 * (tentativa + 1)
            log.warning(
                "Upload falhou (tentativa %d/3): %s. Aguardando %ds...",
                tentativa + 1, e, espera,
            )
            _abortar_multiparts_chave(s3, key)
            time.sleep(espera)
    raise RuntimeError("unreachable")

def _exibir_preview(sftp: paramiko.SFTPClient) -> None:
    linhas = []
    for dir_raiz in DIRS_RAIZ:
        for pasta in PASTAS_LISTAR_PREVIEW:
            caminho = f"{dir_raiz}/{pasta}"
            arquivos = sorted(listar_arquivos(sftp, caminho), key=lambda x: x[1])
            if not arquivos:
                continue
            linhas.append(f"[{caminho}] — {len(arquivos)} arquivos")
            for caminho_arq, tamanho, _ in arquivos:
                linhas.append(f"  {format_size(tamanho):>10}  {caminho_arq}")
            linhas.append("")

    preview_path = Path(__file__).parent / "preview_arquivos.txt"
    preview_path.write_text("\n".join(linhas), encoding="utf-8")

    log.info("=" * 60)
    log.info("PREVIEW — arquivos disponíveis nas pastas monitoradas")
    log.info("=" * 60)
    for linha in linhas:
        if linha:
            log.info(linha)
    log.info("Preview salvo em %s", preview_path)
    log.info("=" * 60)

def processar_arquivo_simples(
    sftp: paramiko.SFTPClient,
    s3,
    conn_str: str,
    caminho_sftp: str,
    tamanho: int,
    mtime: int,
    nomes_usados: set,
    ja_inseridos: set,
    idx: int = 0,
    total: int = 0,
) -> str:
    """Retorna 'skipped', 'success' ou 'error'."""
    nome_arquivo = caminho_sftp.rsplit("/", 1)[-1]

    if caminho_sftp in ja_inseridos:
        log.info("→ [%d/%d] %s — já inserido, pulando", idx, total, nome_arquivo)
        return "skipped"

    nome_destino = gerar_nome_unico(nome_arquivo, nomes_usados)
    nomes_usados.add(nome_destino)

    spinner.iniciar(_label(nome_arquivo, tamanho, idx, total))
    tmp_path = None
    try:
        tmp_path  = baixar_para_tempfile(sftp, caminho_sftp)
        file_hash = _compute_hash(tmp_path)
        minio_key = subir_para_minio(s3, tmp_path, nome_destino)
        spinner.parar(ok=True)
        log.info("✓ [%d/%d] %s → raw/%s", idx, total, nome_arquivo, nome_destino)
        _registrar_ingest(
            conn_str, caminho_sftp, nome_destino, minio_key,
            tamanho, mtime, file_hash, "success",
        )
        return "success"
    except Exception as e:
        if _is_conn_error(e):
            spinner.parar(ok=False)
            raise
        spinner.parar(ok=False)
        log.error("✗ [%d/%d] %s: %s", idx, total, nome_arquivo, e)
        _registrar_ingest(
            conn_str, caminho_sftp, nome_destino, None,
            tamanho, mtime, None, "error", str(e)[:500],
        )
        return "error"
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

def processar_zip(
    sftp: paramiko.SFTPClient,
    s3,
    conn_str: str,
    caminho_zip: str,
    tamanho: int,
    mtime: int,
    nomes_usados: set,
    ja_inseridos: set,
    idx: int = 0,
    total: int = 0,
) -> Tuple[int, int]:
    """Retorna (qtd_sucesso, qtd_erro)."""
    sucesso = erro = 0
    nome_zip = caminho_zip.rsplit("/", 1)[-1]

    spinner.iniciar(_label(nome_zip, tamanho, idx, total))
    tmp_zip = None
    try:
        tmp_zip = baixar_para_tempfile(sftp, caminho_zip)
    except Exception as e:
        spinner.parar(ok=False)
        if _is_conn_error(e):
            raise
        log.error("✗ [%d/%d] %s (download): %s", idx, total, nome_zip, e)
        _registrar_ingest(
            conn_str, caminho_zip, nome_zip, None,
            tamanho, mtime, None, "error", str(e)[:500],
        )
        return 0, 1

    try:
        with zipfile.ZipFile(tmp_zip) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue

                nome_interno = info.filename.rsplit("/", 1)[-1]
                ext_interna  = _extensao(nome_interno)

                if ext_interna not in EXTENSOES_SUPORTADAS or ext_interna == ".zip":
                    continue

                caminho_log = f"{caminho_zip}@{nome_interno}"
                if caminho_log in ja_inseridos:
                    log.info("  ↳ %s — já inserido, pulando", nome_interno)
                    continue

                nome_destino = gerar_nome_unico(nome_interno, nomes_usados)
                nomes_usados.add(nome_destino)

                spinner.atualizar(f"[{idx}/{total}] {nome_zip} └─ {nome_interno}")
                tmp_interno = None
                try:
                    with tempfile.NamedTemporaryFile(
                        delete=False, suffix=ext_interna
                    ) as tf:
                        tf.write(zf.read(info.filename))
                        tmp_interno = tf.name

                    file_hash = _compute_hash(tmp_interno)
                    minio_key = subir_para_minio(s3, tmp_interno, nome_destino)
                    log.info("  ✓ ↳ %s → raw/%s", nome_interno, nome_destino)
                    _registrar_ingest(
                        conn_str, caminho_log, nome_destino, minio_key,
                        info.file_size, mtime, file_hash, "success",
                    )
                    sucesso += 1
                except Exception as e:
                    log.error("  ✗ ↳ %s: %s", nome_interno, e)
                    _registrar_ingest(
                        conn_str, caminho_log, nome_destino, None,
                        info.file_size, mtime, None, "error", str(e)[:500],
                    )
                    erro += 1
                finally:
                    if tmp_interno and os.path.exists(tmp_interno):
                        os.unlink(tmp_interno)

        spinner.parar(ok=erro == 0)
        log.info(
            "✓ [%d/%d] %s — %d ok, %d erro(s)",
            idx, total, nome_zip, sucesso, erro,
        )

    except zipfile.BadZipFile:
        spinner.parar(ok=False)
        log.warning(
            "⚠ [%d/%d] %s — não reconhecido como ZIP pelo Python, subindo como arquivo bruto...",
            idx, total, nome_zip,
        )
        if caminho_zip in ja_inseridos:
            log.info("  → já inserido como bruto, pulando", )
            if tmp_zip and os.path.exists(tmp_zip):
                os.unlink(tmp_zip)
            return 0, 0
        nome_destino = gerar_nome_unico(nome_zip, nomes_usados)
        nomes_usados.add(nome_destino)
        try:
            file_hash = _compute_hash(tmp_zip)
            minio_key = subir_para_minio(s3, tmp_zip, nome_destino)
            log.info("✓ [%d/%d] %s → raw/%s (bruto)", idx, total, nome_zip, nome_destino)
            _registrar_ingest(
                conn_str, caminho_zip, nome_destino, minio_key,
                tamanho, mtime, file_hash, "success",
            )
            return 1, 0
        except Exception as e2:
            log.error("✗ [%d/%d] %s (fallback bruto): %s", idx, total, nome_zip, e2)
            _registrar_ingest(
                conn_str, caminho_zip, nome_destino, None,
                tamanho, mtime, None, "error", str(e2)[:500],
            )
            return 0, 1
        finally:
            if tmp_zip and os.path.exists(tmp_zip):
                os.unlink(tmp_zip)
    finally:
        if tmp_zip and os.path.exists(tmp_zip):
            os.unlink(tmp_zip)

    return sucesso, erro

def _processar_entry(
    sftp, s3, conn_str, caminho, tamanho, mtime,
    nomes_usados, ja_inseridos, idx, total,
) -> Tuple[int, int]:
    """Despacha para simples ou zip. Retorna (sucesso, erro)."""
    ext = _extensao(caminho.rsplit("/", 1)[-1])
    if ext == ".zip":
        return processar_zip(
            sftp, s3, conn_str, caminho, tamanho, mtime,
            nomes_usados, ja_inseridos, idx, total,
        )
    resultado = processar_arquivo_simples(
        sftp, s3, conn_str, caminho, tamanho, mtime,
        nomes_usados, ja_inseridos, idx, total,
    )
    return (1, 0) if resultado == "success" else (0, 1) if resultado == "error" else (0, 0)


def main() -> None:
    if not SFTP_PASSWORD:
        raise ValueError("SFTP_PASSWORD não encontrada no .env")

    log.info("Log: %s", _LOG_FILE)

    conn_str = _conn_str()
    _criar_schema_e_log(conn_str)

    ja_inseridos = _obter_ja_inseridos(conn_str)
    nomes_usados = _obter_nomes_usados(conn_str)
    log.info(
        "%d sftp_paths já inseridos | %d nomes em uso no MinIO",
        len(ja_inseridos), len(nomes_usados),
    )

    s3 = criar_cliente_minio()
    _garantir_bucket(s3)
    _abortar_multiparts_incompletos(s3)

    log.info("Conectando ao SFTP %s@%s:%s ...", SFTP_USER, SFTP_HOST, SFTP_PORT)
    transport, sftp = conectar_sftp()

    total_sucesso = total_erro = 0

    try:
        _exibir_preview(sftp)

        todos_arquivos = []
        for pasta in PASTAS_LISTAR_PREVIEW:
            arquivos_pasta = []
            for dir_raiz in DIRS_RAIZ:
                log.info("Varrendo %s/%s ...", dir_raiz, pasta)
                arquivos_pasta.extend(listar_arquivos(sftp, f"{dir_raiz}/{pasta}"))
            arquivos_pasta.sort(key=lambda x: x[1])
            todos_arquivos.extend(arquivos_pasta)

        total      = len(todos_arquivos)
        total_size = sum(t for _, t, _ in todos_arquivos)
        log.info("Total: %d arquivos | %s", total, format_size(total_size))

        for idx, (caminho, tamanho, mtime) in enumerate(todos_arquivos, 1):
            try:
                s, e = _processar_entry(
                    sftp, s3, conn_str, caminho, tamanho, mtime,
                    nomes_usados, ja_inseridos, idx, total,
                )
                total_sucesso += s
                total_erro    += e

            except Exception as e:
                if not _is_conn_error(e):
                    log.error("Erro inesperado em %s: %s", caminho, e)
                    total_erro += 1
                    continue

                log.warning(
                    "Conexão perdida (%s — %s), reconectando...",
                    type(e).__name__, e,
                )
                try:
                    sftp.close()
                    transport.close()
                except Exception:
                    pass

                try:
                    transport, sftp = conectar_sftp()
                    log.info("Reconectado. Retentando %s ...", caminho)
                except Exception as conn_e:
                    log.error("Falha ao reconectar: %s. Abortando.", conn_e)
                    break

                try:
                    s, e = _processar_entry(
                        sftp, s3, conn_str, caminho, tamanho, mtime,
                        nomes_usados, ja_inseridos, idx, total,
                    )
                    total_sucesso += s
                    total_erro    += e
                except Exception as e2:
                    log.error("Erro após reconexão em %s: %s", caminho, e2)
                    total_erro += 1

    finally:
        sftp.close()
        transport.close()

    error_lines = sum(
        1 for line in _LOG_FILE.read_text(encoding="utf-8").splitlines()
        if " ERROR " in line
    )
    log.info(
        "Concluído. Sucesso: %d | Erro: %d | Log: %s",
        total_sucesso, total_erro, _LOG_FILE,
    )
    if error_lines:
        log.warning("%d linha(s) ERROR no log → %s", error_lines, _LOG_FILE)


if __name__ == "__main__":
    main()
