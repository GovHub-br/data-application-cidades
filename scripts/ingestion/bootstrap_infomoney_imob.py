"""Partição inicial do IMOB na staging nova, a partir da staging antiga. Uma vez só.

A ingestão antiga do Infomoney acumulava o histórico no Postgres
(`infomoney.acoes_imob`) e regravava `staging/infomoney/acoes_imob.parquet` a
partir dele, porque a API do Alpha Vantage devolve só os ~100 últimos pregões. No
pipeline novo o histórico se acumula pelo merge das partições da staging; este
script grava o histórico antigo como a partição mais antiga do dataset:

- data e hora da partição = último `dt_ingest` da staging antiga (Brasília), antes
  de qualquer ingestão nova: no merge, o pregão que a API ainda devolve vence;
- um arquivo por símbolo (`<símbolo>.parquet`, o mesmo nome que a conversão dá),
  com as colunas da conversão nova (`data_pregao`, `1. open`…), em texto;
- `_SUCCESS` com manifesto, como toda partição; nunca sobrescreve.

Sem `--executar`, só mostra o que faria. Usa o mesmo ambiente das DAGs
(`MINIO_BUCKET`, Connection `minio_lake`); com `INGESTION_STORAGE_PREFIX=tests/`, a
partição vai para `tests/` (a staging antiga é lida sempre do bucket real).

    python scripts/ingestion/bootstrap_infomoney_imob.py [--executar]
"""

import argparse
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "plugins"))

from ingestion.extractors import describe_file  # noqa: E402
from ingestion.layout import (  # noqa: E402
    TIMEZONE,
    ingestion_partition,
    safe_segment,
    staging_prefix,
)
from ingestion.storage import (  # noqa: E402
    StorageBackend,
    StorageFactory,
    land,
    storage_from_env,
)

OLD_KEY = "staging/infomoney/acoes_imob.parquet"
COLUMNS = {
    "data_pregao": "data_pregao",
    "open": "1. open",
    "high": "2. high",
    "low": "3. low",
    "close": "4. close",
    "volume": "5. volume",
}


def last_ingestion(table: pa.Table) -> datetime:
    """Último `dt_ingest` da staging antiga, no fuso de Brasília."""
    latest = max(
        datetime.fromisoformat(value) for value in table.column("dt_ingest").to_pylist()
    )
    return latest if latest.tzinfo else latest.replace(tzinfo=TIMEZONE)


def bootstrap(
    source: StorageBackend, target: StorageBackend, work: Path, executar: bool
) -> str:
    """Grava (ou só calcula, sem `executar`) a partição inicial; devolve o prefixo."""
    work.mkdir(parents=True, exist_ok=True)
    old = work / "acoes_imob_antigo.parquet"
    source.get_file(OLD_KEY, old)
    table = pq.read_table(old)
    prefix = staging_prefix(
        "infomoney", "acoes_imob", ingestion_partition(last_ingestion(table))
    )
    if target.list(prefix):
        raise FileExistsError(f"a partição {prefix} já existe; nada foi gravado")
    if not executar:
        return prefix
    parts = []
    for symbol in sorted(set(table.column("symbol").to_pylist())):
        rows = table.filter(pc.equal(table.column("symbol"), symbol))
        renamed = pa.table(
            {
                new: rows.column(old_name).cast(pa.string())
                for old_name, new in COLUMNS.items()
            }
        )
        path = work / f"{safe_segment(symbol)}.parquet"
        pq.write_table(renamed, path)
        parts.append(describe_file(path))
    land(target, parts, prefix, details=lambda part: {"origem": OLD_KEY})
    return prefix


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--executar", action="store_true", help="grava a partição")
    args = parser.parse_args()
    source = StorageFactory.create(
        "s3",
        bucket=os.environ["MINIO_BUCKET"],
        conn_id=os.environ.get("INGESTION_STORAGE_CONN_ID", "minio_lake"),
    )
    with tempfile.TemporaryDirectory() as work:
        prefix = bootstrap(source, storage_from_env(), Path(work), args.executar)
    print(("gravada: " if args.executar else "seria gravada: ") + prefix)


if __name__ == "__main__":
    main()
