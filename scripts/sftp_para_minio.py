# scripts/sftp_para_minio.py

"""
Ingestão de arquivos do SFTP para o MinIO (camada raw).

Percorre Analise_SNH, GEFUS e CadUnico em /home/fabrica, além de
/home/caixa.geavo/GEAVO, baixa todos os arquivos suportados (CSV, TXT, XLSX,
XLS, MDB, ZIP — expandindo ZIPs internamente) e os sobe para raw/ no MinIO com
o nome original do arquivo.

Arquivos .mdb extraídos de um ZIP recebem o stem do ZIP como prefixo
(MC20260227__MCidades_AO_1.mdb), pois o mesmo nome interno se repete em todos os
zips e a data de referência só existe no nome do ZIP.

Colisões de nome recebem sufixo contador: arquivo_0000.csv, arquivo_0001.csv, ...

Controle de reprocessamento via tabela sftp._ingest_minio_log no PostgreSQL.
Arquivos são processados do menor para o maior: Analise_SNH primeiro, depois GEFUS.
"""

import argparse
import hashlib
import itertools
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Tuple

from boto3.s3.transfer import TransferConfig
import paramiko
import psycopg2
from dotenv import load_dotenv

# plugins/ (ClienteMinio/ClienteSftp) está na PYTHONPATH dentro do container Airflow; rodando
# standalone, adiciona airflow_lappis/plugins ao sys.path para o import resolver.
_plugins = Path(__file__).resolve().parents[1] / "airflow_lappis" / "plugins"
if _plugins.is_dir() and str(_plugins) not in sys.path:
    sys.path.insert(0, str(_plugins))

from cliente_minio import ClienteMinio  # noqa: E402
from cliente_sftp import ClienteSftp  # noqa: E402

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

EXTENSOES_SUPORTADAS  = {".csv", ".txt", ".xlsx", ".xls", ".zip", ".mdb"}
# Extensões cujos nomes se repetem entre zips (ex.: MCidades_AO_1.mdb em todo
# MC2026*.zip). A data só existe no nome do zip, então ela vai para o destino.
EXTENSOES_PREFIXAR_ORIGEM = {".mdb"}
MAX_PROFUNDIDADE      = 10
PASTAS_IGNORAR        = set()  # "CadUnico" liberado pela infra
PASTAS_MONITORADAS    = [
    "/home/fabrica/Analise_SNH",
    "/home/fabrica/GEFUS",
    "/home/fabrica/CadUnico",
    "/home/caixa.geavo/GEAVO",
]

# Erro de conexão (queda de socket): delegado ao cliente SFTP.
_is_conn_error = ClienteSftp.is_conn_error

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

def _extensao(nome: str) -> str:
    pos = nome.rfind(".")
    return nome[pos:].lower() if pos != -1 else ""


def _listar_arquivos(sftp_cli: ClienteSftp, caminho: str) -> Iterator[Tuple[str, int, int]]:
    """Lista arquivos suportados sob `caminho` (via ClienteSftp)."""
    return sftp_cli.listar_arquivos(
        caminho, extensoes=EXTENSOES_SUPORTADAS, pastas_ignorar=PASTAS_IGNORAR
    )


def baixar_para_tempfile(sftp: paramiko.SFTPClient, caminho: str) -> str:
    """Baixa para disco — evita OOM em arquivos grandes."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(caminho).suffix, dir="/var/tmp")
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


def _nome_com_origem(nome_zip: str, nome_interno: str) -> str:
    """Prefixa o nome interno com o stem do zip quando a extensão repete entre zips."""
    if _extensao(nome_interno) not in EXTENSOES_PREFIXAR_ORIGEM:
        return nome_interno
    stem_zip = nome_zip[:-4] if nome_zip.lower().endswith(".zip") else nome_zip
    return f"{stem_zip}__{nome_interno}"


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

# Upload serial em partes de 5 MB — mais estável em conexões instáveis (mín. S3 = 5 MB).
_SFTP_TRANSFER = TransferConfig(multipart_chunksize=5 * 1024 * 1024, max_concurrency=1)


def subir_para_minio(minio: ClienteMinio, tmp_path: str, nome_destino: str) -> str:
    return minio.upload_arquivo(
        tmp_path, f"raw/{nome_destino}", transfer_config=_SFTP_TRANSFER
    )


def _subir_stream_com_hash(
    minio: ClienteMinio, nome_destino: str, stream_factory
) -> Tuple[str, str]:
    """Upload a partir de um stream (sem temp file), retornando (minio_key, file_hash).
    stream_factory é um callable que retorna um context manager com .read()."""
    return minio.subir_stream_com_hash(
        f"raw/{nome_destino}", stream_factory, transfer_config=_SFTP_TRANSFER
    )


def _exibir_preview(sftp_cli: ClienteSftp, pastas: list) -> None:
    linhas = []
    for caminho in pastas:
        arquivos = sorted(_listar_arquivos(sftp_cli, caminho), key=lambda x: x[1])
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
    minio: ClienteMinio,
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
        minio_key = subir_para_minio(minio, tmp_path, nome_destino)
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

def _baixar_volumes_complementares(
    sftp: paramiko.SFTPClient, caminho_zip: str, tmp_zip: str, tmp_dir: str
) -> bool:
    """Baixa volumes .z01/.z02… com mesmo stem para tmp_dir. Retorna True se encontrou."""
    dir_sftp = caminho_zip.rsplit("/", 1)[0]
    nome_zip = caminho_zip.rsplit("/", 1)[-1]
    stem = nome_zip[:-4] if nome_zip.lower().endswith(".zip") else nome_zip

    entries = sftp.listdir_attr(dir_sftp)
    volumes = sorted(
        e.filename for e in entries
        if e.filename != nome_zip
        and e.filename.startswith(stem)
        and re.match(r"\.z\d+$", e.filename[len(stem):], re.IGNORECASE)
    )
    if not volumes:
        return False

    log.info("  → %d volume(s) complementar(es): %s", len(volumes), volumes)
    shutil.copy2(tmp_zip, os.path.join(tmp_dir, nome_zip))
    for vol_name in volumes:
        vol_dest = os.path.join(tmp_dir, vol_name)
        with open(vol_dest, "wb") as fh:
            sftp.getfo(f"{dir_sftp}/{vol_name}", fh)
        log.info("  → baixado: %s (%.1f MiB)", vol_name, os.path.getsize(vol_dest) / 1_048_576)
    return True


def _tem_eocd(data: bytes) -> bool:
    return b"PK\x05\x06" in data


def _preparar_multivolume_implicito(
    sftp: paramiko.SFTPClient, caminho_zip: str, tmp_zip: str, tmp_dir: str
) -> "Optional[str]":
    """Detecta volumes complementares com nomes distintos verificando presença/ausência de EOCD.
    Só age quando o arquivo atual tem EOCD mas não extrai sozinho (é o último volume).
    Retorna o caminho do arquivo a passar para 7z, ou None."""
    dir_sftp = caminho_zip.rsplit("/", 1)[0]
    nome_zip = caminho_zip.rsplit("/", 1)[-1]

    with open(tmp_zip, "rb") as f:
        f.seek(max(0, os.path.getsize(tmp_zip) - 100))
        atual_tem_eocd = _tem_eocd(f.read())

    if not atual_tem_eocd:
        return None

    candidatos = [
        e.filename for e in sftp.listdir_attr(dir_sftp)
        if e.filename != nome_zip and e.filename.lower().endswith(".zip")
    ]
    if not candidatos:
        return None

    parceiros = []
    for cand in candidatos:
        try:
            with sftp.open(f"{dir_sftp}/{cand}", "rb") as fh:
                fh.seek(-100, 2)
                cand_tem_eocd = _tem_eocd(fh.read(100))
        except Exception:
            continue
        if not cand_tem_eocd:
            parceiros.append(cand)

    if not parceiros:
        return None

    log.info("  → parceiro(s) multi-volume implícito(s): %s", parceiros)
    shutil.copy2(tmp_zip, os.path.join(tmp_dir, nome_zip))
    for i, parc in enumerate(sorted(parceiros), 1):
        dest = os.path.join(tmp_dir, nome_zip[:-4] + f".z{i:02d}")
        with open(dest, "wb") as fh:
            sftp.getfo(f"{dir_sftp}/{parc}", fh)
        log.info("  → .z%02d: %s (%.1f MiB)", i, parc, os.path.getsize(dest) / 1_048_576)
    return os.path.join(tmp_dir, nome_zip)


def processar_zip(
    sftp: paramiko.SFTPClient,
    minio: ClienteMinio,
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
                    log.info(
                        "  ↳ %s — extensão '%s' não suportada, pulando",
                        nome_interno, ext_interna or "(sem extensão)",
                    )
                    continue

                caminho_log = f"{caminho_zip}@{nome_interno}"
                if caminho_log in ja_inseridos:
                    log.info("  ↳ %s — já inserido, pulando", nome_interno)
                    continue

                nome_destino = gerar_nome_unico(
                    _nome_com_origem(nome_zip, nome_interno), nomes_usados
                )
                nomes_usados.add(nome_destino)

                spinner.atualizar(f"[{idx}/{total}] {nome_zip} └─ {nome_interno}")
                try:
                    minio_key, file_hash = _subir_stream_com_hash(
                        minio, nome_destino,
                        lambda fn=info.filename: zf.open(fn),
                    )
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

        spinner.parar(ok=erro == 0)
        log.info(
            "✓ [%d/%d] %s — %d ok, %d erro(s)",
            idx, total, nome_zip, sucesso, erro,
        )

    except zipfile.BadZipFile:
        spinner.parar(ok=False)

        if caminho_zip in ja_inseridos:
            log.info("  → já inserido como bruto, pulando")
            return 0, 0

        with open(tmp_zip, "rb") as _f:
            _magic = _f.read(16).hex()
        log.warning(
            "⚠ [%d/%d] %s — não reconhecido pelo Python (magic: %s), tentando extratores alternativos...",
            idx, total, nome_zip, _magic,
        )

        tmp_dir = None
        extraido = False
        for extrator in ("unzip", "7z"):
            tmp_dir = tempfile.mkdtemp(dir="/var/tmp")
            if extrator == "unzip":
                cmd: list[str] = ["unzip", "-o", tmp_zip, "-d", tmp_dir]
            else:
                cmd = ["7z", "e", tmp_zip, f"-o{tmp_dir}", "-y"]
            try:
                res = subprocess.run(cmd, capture_output=True, timeout=300)
                log.warning("  [%s] rc=%d stderr=%s", extrator, res.returncode, res.stderr.decode(errors="replace")[:200])
                if res.returncode in (0, 1) and any(Path(tmp_dir).iterdir()):
                    log.info("  → extraído com %s (rc=%d)", extrator, res.returncode)
                    extraido = True
                    break
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass
            shutil.rmtree(tmp_dir, ignore_errors=True)
            tmp_dir = None

        if not extraido:
            tmp_dir_vols = tempfile.mkdtemp(dir="/var/tmp")
            tmp_dir_extract = tempfile.mkdtemp(dir="/var/tmp")
            try:
                if _baixar_volumes_complementares(sftp, caminho_zip, tmp_zip, tmp_dir_vols):
                    alvo_7z: Optional[str] = os.path.join(tmp_dir_vols, nome_zip)
                else:
                    alvo_7z = _preparar_multivolume_implicito(sftp, caminho_zip, tmp_zip, tmp_dir_vols)

                if alvo_7z:
                    cmd_mv = ["7z", "e", alvo_7z, f"-o{tmp_dir_extract}", "-y"]
                    try:
                        res_mv = subprocess.run(cmd_mv, capture_output=True, timeout=600)
                        log.info(
                            "  [7z multi-vol] rc=%d stderr=%s",
                            res_mv.returncode,
                            res_mv.stderr.decode(errors="replace")[:200],
                        )
                        if res_mv.returncode in (0, 1) and any(Path(tmp_dir_extract).iterdir()):
                            log.info("  → extraído como multi-volume (rc=%d)", res_mv.returncode)
                            extraido = True
                            tmp_dir = tmp_dir_extract
                            tmp_dir_extract = None
                    except (subprocess.TimeoutExpired, FileNotFoundError):
                        pass
            except Exception as e_mv:
                log.warning("  → tentativa multi-volume falhou: %s", e_mv)
            finally:
                shutil.rmtree(tmp_dir_vols, ignore_errors=True)
                if tmp_dir_extract:
                    shutil.rmtree(tmp_dir_extract, ignore_errors=True)

        if extraido and tmp_dir:
            try:
                for arq_path in sorted(Path(tmp_dir).rglob("*")):
                    if arq_path.is_dir():
                        continue
                    nome_interno = arq_path.name
                    ext_interna  = _extensao(nome_interno)
                    if ext_interna not in EXTENSOES_SUPORTADAS or ext_interna == ".zip":
                        log.info(
                            "  ↳ %s — extensão '%s' não suportada, pulando",
                            nome_interno, ext_interna or "(sem extensão)",
                        )
                        continue
                    caminho_log = f"{caminho_zip}@{nome_interno}"
                    if caminho_log in ja_inseridos:
                        log.info("  ↳ %s — já inserido, pulando", nome_interno)
                        continue
                    nome_destino = gerar_nome_unico(
                        _nome_com_origem(nome_zip, nome_interno), nomes_usados
                    )
                    nomes_usados.add(nome_destino)
                    spinner.atualizar(f"[{idx}/{total}] {nome_zip} └─ {nome_interno}")
                    try:
                        file_hash = _compute_hash(str(arq_path))
                        minio_key = subir_para_minio(minio, str(arq_path), nome_destino)
                        log.info("  ✓ ↳ %s → raw/%s", nome_interno, nome_destino)
                        _registrar_ingest(
                            conn_str, caminho_log, nome_destino, minio_key,
                            arq_path.stat().st_size, mtime, file_hash, "success",
                        )
                        sucesso += 1
                    except Exception as e:
                        log.error("  ✗ ↳ %s: %s", nome_interno, e)
                        _registrar_ingest(
                            conn_str, caminho_log, nome_destino, None,
                            arq_path.stat().st_size, mtime, None, "error", str(e)[:500],
                        )
                        erro += 1
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            spinner.parar(ok=erro == 0)
            log.info(
                "✓ [%d/%d] %s — %d ok, %d erro(s) (extrator alternativo)",
                idx, total, nome_zip, sucesso, erro,
            )
            return sucesso, erro

        # Último fallback: sobe o arquivo bruto
        log.warning("⚠ [%d/%d] %s — extratores falharam, subindo como arquivo bruto...", idx, total, nome_zip)
        nome_destino = gerar_nome_unico(nome_zip, nomes_usados)
        nomes_usados.add(nome_destino)
        try:
            file_hash = _compute_hash(tmp_zip)
            minio_key = subir_para_minio(minio, tmp_zip, nome_destino)
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
    finally:
        if tmp_zip and os.path.exists(tmp_zip):
            os.unlink(tmp_zip)

    return sucesso, erro

def _processar_entry(
    sftp, minio, conn_str, caminho, tamanho, mtime,
    nomes_usados, ja_inseridos, idx, total,
) -> Tuple[int, int]:
    """Despacha para simples ou zip. Retorna (sucesso, erro)."""
    ext = _extensao(caminho.rsplit("/", 1)[-1])
    if ext == ".zip":
        return processar_zip(
            sftp, minio, conn_str, caminho, tamanho, mtime,
            nomes_usados, ja_inseridos, idx, total,
        )
    resultado = processar_arquivo_simples(
        sftp, minio, conn_str, caminho, tamanho, mtime,
        nomes_usados, ja_inseridos, idx, total,
    )
    return (1, 0) if resultado == "success" else (0, 1) if resultado == "error" else (0, 0)


def _resolver_pastas(filtros: Optional[list]) -> list:
    """Converte os valores de --pasta em caminhos completos.
    Aceita nome curto (casado contra PASTAS_MONITORADAS) ou caminho absoluto."""
    if not filtros:
        return list(PASTAS_MONITORADAS)

    resolvidas = []
    for filtro in filtros:
        if filtro.startswith("/"):
            resolvidas.append(filtro.rstrip("/"))
            continue
        casadas = [p for p in PASTAS_MONITORADAS if p.rsplit("/", 1)[-1] == filtro]
        if not casadas:
            raise ValueError(
                f"Pasta '{filtro}' não é monitorada. Opções: "
                + ", ".join(p.rsplit("/", 1)[-1] for p in PASTAS_MONITORADAS)
                + " (ou passe um caminho absoluto)"
            )
        resolvidas.extend(casadas)
    return resolvidas


# Execução
def run(pastas: Optional[list] = None, preview: bool = False) -> Tuple[int, int]:
    """Ingere os arquivos do SFTP para raw/ no MinIO. Retorna (sucesso, erro).

    Ponto de entrada reutilizável (CLI via main(); DAG do Airflow chama run()).
    Não há dry-run: a ingestão é idempotente pelo controle sftp._ingest_minio_log.
    """
    pastas_filtro = _resolver_pastas(pastas)

    if not SFTP_PASSWORD:
        raise ValueError("SFTP_PASSWORD não encontrada no .env")

    log.info("Log: %s", _LOG_FILE)

    sftp_cli = ClienteSftp()

    if preview:
        log.info("Conectando ao SFTP %s@%s:%s ...", SFTP_USER, SFTP_HOST, SFTP_PORT)
        sftp_cli.conectar()
        try:
            _exibir_preview(sftp_cli, pastas_filtro)
        finally:
            sftp_cli.fechar()
        return 0, 0

    conn_str = _conn_str()
    _criar_schema_e_log(conn_str)

    ja_inseridos = _obter_ja_inseridos(conn_str)
    nomes_usados = _obter_nomes_usados(conn_str)
    log.info(
        "%d sftp_paths já inseridos | %d nomes em uso no MinIO",
        len(ja_inseridos), len(nomes_usados),
    )

    minio = ClienteMinio()
    minio.garantir_bucket()
    minio.abortar_multiparts_incompletos()

    log.info("Conectando ao SFTP %s@%s:%s ...", SFTP_USER, SFTP_HOST, SFTP_PORT)
    sftp = sftp_cli.conectar()

    total_sucesso = total_erro = 0

    try:
        _exibir_preview(sftp_cli, pastas_filtro)

        todos_arquivos = []
        for pasta in pastas_filtro:
            log.info("Varrendo %s ...", pasta)
            arquivos_pasta = list(_listar_arquivos(sftp_cli, pasta))
            arquivos_pasta.sort(key=lambda x: x[1])
            todos_arquivos.extend(arquivos_pasta)

        total      = len(todos_arquivos)
        total_size = sum(t for _, t, _ in todos_arquivos)
        log.info("Total: %d arquivos | %s", total, format_size(total_size))

        for idx, (caminho, tamanho, mtime) in enumerate(todos_arquivos, 1):
            try:
                s, e = _processar_entry(
                    sftp, minio, conn_str, caminho, tamanho, mtime,
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
                    sftp = sftp_cli.reconectar()
                    log.info("Reconectado. Retentando %s ...", caminho)
                except Exception as conn_e:
                    log.error("Falha ao reconectar: %s. Abortando.", conn_e)
                    break

                try:
                    s, e = _processar_entry(
                        sftp, minio, conn_str, caminho, tamanho, mtime,
                        nomes_usados, ja_inseridos, idx, total,
                    )
                    total_sucesso += s
                    total_erro    += e
                except Exception as e2:
                    log.error("Erro após reconexão em %s: %s", caminho, e2)
                    total_erro += 1

    finally:
        sftp_cli.fechar()

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
    return total_sucesso, total_erro


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pasta", metavar="PASTA",
        help="Processa só esta pasta (nome, ex: CadUnico, ou caminho completo). "
             "Repita para várias.",
        action="append", dest="pastas",
    )
    parser.add_argument(
        "--preview", action="store_true",
        help="Só lista os arquivos das pastas e sai, sem baixar nem subir nada.",
    )
    args = parser.parse_args()
    run(pastas=args.pastas, preview=args.preview)


if __name__ == "__main__":
    main()
