"""Layout do lake: como domínio, dataset e data de ingestão viram caminho."""

import re
from datetime import datetime
from zoneinfo import ZoneInfo

#: A pasta do dia é o dia em Brasília: é nele que alguém procura "a ingestão de hoje".
TIMEZONE = ZoneInfo("America/Sao_Paulo")

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")


def safe_segment(value: str) -> str:
    """Segmento de caminho seguro para S3 e disco: só `[A-Za-z0-9._-]`.

    Cada caractere fora disso vira `_` (inclusive acento, `/` e o `:`/`+` que o
    Airflow põe no run_id). Vazio, `.` e `..` são recusados: viram caminho relativo.
    """
    segment = _UNSAFE.sub("_", value)
    if segment in {"", ".", ".."}:
        raise ValueError(f"segmento de caminho inválido: {value!r}")
    return segment


def ingestion_partition(run_after: datetime) -> str:
    """Partição `AAAA-MM-DD/HHMMSS` da ingestão, no horário de Brasília.

    Recebe o `run_after` da run do Airflow. A data separa as ingestões por dia; a
    hora separa duas execuções no mesmo dia, e a ordem alfabética das partições é
    a cronológica, então a última ingestão é a maior.
    """
    if run_after.tzinfo is None or run_after.utcoffset() is None:
        raise ValueError(f"run_after sem fuso horário: {run_after!r}")
    return run_after.astimezone(TIMEZONE).strftime("%Y-%m-%d/%H%M%S")
