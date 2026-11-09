"""Configuração da conversão: variáveis de ambiente, prefixos, formatos e log."""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from lake_utils import MDB_EXT

load_dotenv()

# logs e cópias locais ficam ao lado de raw_para_staging.py, como antes do pacote
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
CONTROL_TABLE = "_staging_log"

RAW_PREFIX = os.environ.get("MASKING_PREFIX", "raw/")
STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "staging/")
AUDIT_PREFIX = "audit/staging/"

TMPDIR = os.environ.get("LAKE_TMPDIR") or os.environ.get("MASKING_TMPDIR") or None
if TMPDIR:
    os.makedirs(TMPDIR, exist_ok=True)

SUPPORTED_TABULAR = {".csv", ".txt"}
SUPPORTED_EXCEL = {".xlsx"}
SUPPORTED_MDB = MDB_EXT  # .mdb/.accdb (Access) — via mdbtools
SUPPORTED_JSON = {".json"}  # registros já achatados (list[dict]) pelo cliente da fonte
UNSUPPORTED = {".xls", ".zip"}
SUPORTADAS = SUPPORTED_TABULAR | SUPPORTED_EXCEL | SUPPORTED_MDB | SUPPORTED_JSON

# Pastas de raw/ que nunca viram parquet: dados_historicos/ tem tratamento próprio da
# equipe de ciência de dados. Segue sendo mascarada normalmente.
PASTAS_IGNORADAS = {"dados_historicos"}

LINEAGE_COLS = ["_source_file", "_ingested_at", "_source_hash"]


# Artefatos locais (log em arquivo, cópia da auditoria): úteis standalone, mas o diretório
# do script pode não ser gravável. O container do Airflow define LAKE_LOCAL_ARTIFACTS=0.
_LOCAL_ARTIFACTS = os.environ.get("LAKE_LOCAL_ARTIFACTS", "1").lower() not in (
    "0",
    "false",
    "no",
)
_LOG_FILE = (
    SCRIPTS_DIR / f"raw_para_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
_formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)
if _LOCAL_ARTIFACTS:
    # Standalone: o script gerencia os próprios handlers (stderr + arquivo de log).
    logging.root.setLevel(logging.INFO)
    for _h in (
        logging.StreamHandler(sys.stderr),
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
    ):
        _h.setFormatter(_formatter)
        logging.root.addHandler(_h)
# Sob o Airflow o logger só propaga: um StreamHandler(sys.stderr) aqui multiplicaria cada
# linha, porque o Airflow redireciona stderr de volta ao logging.
