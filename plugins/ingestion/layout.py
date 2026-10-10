"""Layout do lake, o da main: a raw e a staging espelham o caminho da fonte.

raw/<domain>/<caminho na fonte>             (sobrescrito a cada ingestão)
staging/<domain>/<caminho sem extensão>[__<aba>].parquet
"""

import posixpath
import re
from zoneinfo import ZoneInfo

#: O dia da ingestão é o dia em Brasília (o e-mail "do dia", o mês corrente).
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


def raw_key(domain: str, name: str) -> str:
    """`raw/<domain>/<name>`: `name` é o caminho do arquivo como na fonte.

    O caminho é mantido como veio (espaço e acento inclusive, como na main); só é
    recusado o que sairia da pasta do domínio ou criaria segmento vazio.
    """
    segments = name.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        raise ValueError(f"caminho inválido na raw de {domain}: {name!r}")
    return f"raw/{safe_segment(domain)}/{name}"


def staging_key(raw_key: str, suffix: str | None = None) -> str:
    """O Parquet de um arquivo da raw: mesmo caminho, sem a extensão de origem.

    `foo.csv` e `foo.txt` são o mesmo dado e viram um `foo.parquet`. `suffix` é a
    aba ou a tabela, quando o arquivo gera mais de um (`foo__aba.parquet`).
    """
    if not raw_key.startswith("raw/"):
        raise ValueError(f"não é uma chave da raw: {raw_key!r}")
    stem = posixpath.splitext(raw_key[len("raw/") :])[0]
    return f"staging/{stem}{f'__{suffix}' if suffix else ''}.parquet"
