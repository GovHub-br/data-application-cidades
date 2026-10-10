# scripts/lake_utils.py

"""
Utilitários compartilhados dos scripts do data lake (MinIO).

Reúne o que mais de uma etapa do pipeline usa: detecção de encoding/dialeto dos arquivos
heterogêneos do raw/, normalização de nome de coluna, hash de arquivo e leitura de bases
Access (.mdb). O I/O de S3/MinIO fica em plugins/cliente_minio.py (ClienteMinio).

Usado por `mascarar_minio.py` e `raw_para_staging.py`. Cada script mantém o que é próprio
dele (conexão Postgres, tabela de controle, regras de negócio).
"""

import csv
import hashlib
import io
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

# csv pode ter campos grandes (linhas longas de bases bancárias)
csv.field_size_limit(2**31 - 1)


# Detecção de encoding / dialeto

# Detecção de encoding/dialeto e normalização de nomes de coluna: em
# plugins/ingestion/text.py (o mesmo código da ingestão nova).
_plugins = Path(__file__).resolve().parents[1] / "plugins"
if _plugins.is_dir() and str(_plugins) not in sys.path:
    sys.path.insert(0, str(_plugins))

from ingestion.text import (  # noqa: E402,F401
    corrigir_mojibake_texto,
    detectar_dialeto,
    detectar_encoding,
    encoding_fallback,
    norm_header,
    normalizar_colunas,
)

# Bases Access (.mdb) — leitura via mdbtools, que é READ-ONLY: não há mdb-import, e quem
# precisar modificar um .mdb tem que falhar explicitamente. O mdb-export entrega CSV em
# UTF-8, então este caminho dispensa detectar_encoding/detectar_dialeto.
MDB_EXT = {".mdb", ".accdb"}
MDB_DELIM = ","
MDB_ENCODING = "utf-8"


def mdb_disponivel() -> bool:
    """True se o binário mdbtools está no PATH."""
    return shutil.which("mdb-tables") is not None


def mdb_tabelas(path: str) -> List[str]:
    """Nomes das tabelas de usuário do .mdb (mdbtools já omite as MSys* de sistema)."""
    r = subprocess.run(["mdb-tables", "-1", path], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(
            f"mdb-tables falhou: {r.stderr.strip() or 'erro desconhecido'}"
        )
    return [t.strip() for t in r.stdout.splitlines() if t.strip()]


def mdb_contar(path: str, tabela: str) -> int:
    """Nº de linhas da tabela. -1 se o mdb-count falhar (não é motivo p/ abortar)."""
    r = subprocess.run(["mdb-count", path, tabela], capture_output=True, text=True)
    if r.returncode != 0:
        return -1
    try:
        return int(r.stdout.strip())
    except ValueError:
        return -1


def mdb_header(path: str, tabela: str) -> List[str]:
    """Só o cabeçalho da tabela — sem materializar as linhas.

    Tabelas de .mdb podem ter milhões de linhas (ex.: acompanhamento_termino_obra com
    6,1M), então lê o stdout incrementalmente e para na 1ª linha, matando o processo em
    seguida.
    """
    p = subprocess.Popen(
        ["mdb-export", path, tabela],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding=MDB_ENCODING,
    )
    try:
        linha = p.stdout.readline() if p.stdout else ""
    finally:
        p.kill()
        p.wait()
    if not linha.strip():
        return []
    return next(csv.reader(io.StringIO(linha), delimiter=MDB_DELIM, quotechar='"'), [])


def mdb_export_para_csv(path: str, tabela: str, dst: str) -> None:
    """Exporta a tabela inteira para um CSV em disco (streaming, memória constante)."""
    with open(dst, "wb") as f:
        r = subprocess.run(["mdb-export", path, tabela], stdout=f, stderr=subprocess.PIPE)
    if r.returncode != 0:
        raise RuntimeError(
            f"mdb-export falhou em '{tabela}': "
            f"{r.stderr.decode('utf-8', 'replace').strip()}"
        )


# Hash de arquivo
def md5_arquivo(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


# NOTA: os helpers de S3/MinIO (cliente, amostra por Range, download) foram movidos para
# plugins/cliente_minio.py (classe ClienteMinio), usada pelos scripts e pela DAG.


def format_size(size_bytes: float) -> str:
    value: float = size_bytes
    for unit in ["B", "KB", "MB", "GB"]:
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


# Partições do pipeline novo de ingestão (plugins/ingestion)
#
# O pipeline novo grava raw/<domínio>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/ e fecha cada
# partição com um _SUCCESS. Ele mesmo converte para a staging, no mesmo caminho que o
# raw_para_staging usaria; e a raw dele é imutável (o _SUCCESS guarda o sha256 de cada
# arquivo), então o mascaramento in-place também não pode tocá-la.
MARCADOR_PIPELINE_NOVO = "_SUCCESS"


def separar_pipeline_novo(
    objetos: List[Tuple[str, int]],
) -> Tuple[List[Tuple[str, int]], List[Tuple[str, int]]]:
    """(mantidos, pulados): pula todo objeto de pasta que tenha o _SUCCESS do pipeline
    novo. Precisa da listagem inteira, porque o marcador pode vir depois dos arquivos."""
    pastas = {
        key.rsplit("/", 1)[0]
        for key, _ in objetos
        if key.rsplit("/", 1)[-1] == MARCADOR_PIPELINE_NOVO
    }
    mantidos: List[Tuple[str, int]] = []
    pulados: List[Tuple[str, int]] = []
    for key, size in objetos:
        (pulados if key.rsplit("/", 1)[0] in pastas else mantidos).append((key, size))
    return mantidos, pulados
