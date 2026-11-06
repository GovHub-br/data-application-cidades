"""Configuração do mascaramento: variáveis de ambiente, limites e log."""

import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from lake_utils import MDB_EXT

load_dotenv()

# logs e cópias locais ficam ao lado de mascarar_minio.py, como antes do pacote
SCRIPTS_DIR = Path(__file__).resolve().parents[1]

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

PG_HOST = os.environ["DB_DW_HOST_MCID"]
PG_PORT = int(os.environ.get("DB_DW_PORT_MCID", 5432))
PG_USER = os.environ["DB_DW_USER_MCID"]
PG_PASSWORD = os.environ["DB_DW_PASSWORD_MCID"]
PG_DBNAME = os.environ["DB_DW_DBNAME_MCID"]

SCHEMA = os.environ.get("LAKE_SCHEMA", "lake")
CONTROL_TABLE = "_masking_log"

RAW_PREFIX = os.environ.get("MASKING_PREFIX", "raw/")
DRYRUN_PREFIX = "masked_dryrun/"
AUDIT_PREFIX = "audit/masking/"

HMAC_SECRET = os.environ.get("MASKING_HMAC_SECRET", "").encode("utf-8")
TOKEN_LEN = int(os.environ.get("MASKING_TOKEN_LEN", 16))
REDACTION = os.environ.get("MASKING_REDACTION", "***")

# Só XLSX e MDB passam pelo disco (original + mascarado); CSV/TXT vai em streaming.
# /tmp costuma ser tmpfs pequeno: aponte para um disco com espaço.
TMPDIR = os.environ.get("MASKING_TMPDIR") or None
if TMPDIR:
    os.makedirs(TMPDIR, exist_ok=True)

# CSV/TXT grande e sem aspas é mascarado em pedaços, um por processo (ver
# `_mascarar_tabular_paralelo`). Um núcleo fica livre para o Airflow.
PROCESSOS = int(os.environ.get("MASKING_PROCESSOS") or max(1, (os.cpu_count() or 2) - 1))
PARALELO_MIN = int(os.environ.get("MASKING_PARALELO_MIN_MB", 1024)) * 1024 * 1024
PEDACO = int(os.environ.get("MASKING_PEDACO_MB", 2048)) * 1024 * 1024
PEDACOS_PREFIX = "tmp/masking/"

SUPPORTED_TABULAR = {".csv", ".txt"}
SUPPORTED_EXCEL = {".xlsx"}
SUPPORTED_MDB = MDB_EXT  # .mdb/.accdb — só LEITURA (ver _analisar_mdb)
UNSUPPORTED = {".xls", ".zip"}

# csv pode ter campos grandes (linhas longas de bases bancárias)
csv.field_size_limit(2**31 - 1)

# Artefatos locais (arquivo de log, cópia local da auditoria) — úteis rodando standalone,
# mas o diretório do script pode não ser gravável (ex.: bind-mount no Airflow). Controlado
# por LAKE_LOCAL_ARTIFACTS (default "1"): o container do Airflow define "0" para
# desligá-los. O log em stderr fica sempre ativo (o Airflow o captura na UI).
_LOCAL_ARTIFACTS = os.environ.get("LAKE_LOCAL_ARTIFACTS", "1").lower() not in (
    "0",
    "false",
    "no",
)
_LOG_FILE = SCRIPTS_DIR / f"mascarar_minio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
_formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)
if _LOCAL_ARTIFACTS and not logging.root.handlers:
    # Standalone: o script gerencia os próprios handlers (stderr + arquivo de log).
    # Importado por outro script que já configurou o log (o raw_para_staging usa
    # `mascarar_csv`), não instala outro par: cada linha sairia duplicada.
    logging.root.setLevel(logging.INFO)
    for _h in (
        logging.StreamHandler(sys.stderr),
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
    ):
        _h.setFormatter(_formatter)
        logging.root.addHandler(_h)
# Sob o Airflow o logger só propaga: um StreamHandler(sys.stderr) aqui multiplicaria cada
# linha, porque o Airflow redireciona stderr de volta ao logging.
