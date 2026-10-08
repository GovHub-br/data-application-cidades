"""Layout do lake: como domínio, dataset e data de ingestão viram caminho."""

import re

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
