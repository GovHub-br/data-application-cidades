"""Carregamento de amostras CSV e do schema do banco PostgreSQL.

O módulo oferece duas fontes de dados:

* ``carregar_amostra`` — lê uma amostra tab-separated de ``data/table_samples/``
  (200 linhas por tabela) como ``DataFrame`` de strings, com a primeira coluna
  (índice de linha 0,1,2...) usada como ``index`` do pandas. Isso faz com que
  ``df.columns`` contenha apenas as colunas de dados, alinhado à regra
  "excluindo o índice pandas" (ver spec R3a).
* ``carregar_schema`` — registra ``data/columns_*.csv`` (schema do PostgreSQL)
  como uma view consultável via duckdb.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

# Raiz do projeto: src/classificacao/carregamento.py -> parents[2]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = _PROJECT_ROOT / "data"

# Conexão duckdb de módulo para que as views/relations persistam entre chamadas.
_DUCK = duckdb.connect()


def _resolve_data_dir(data_dir: Path | str | None) -> Path:
    """Retorna o diretório base de dados a ser usado."""
    return Path(data_dir) if data_dir is not None else _DATA_DIR


def carregar_csv(path: Path | str) -> pd.DataFrame:
    """Lê um CSV tab-separated como strings, com a primeira coluna como índice.

    Parâmetros de leitura (compartilhados por amostras reais e fixtures):
    ``sep="\\t"``, ``dtype=str``, ``on_bad_lines="skip"``, ``index_col=0``.
    O ``index_col=0`` descarta a coluna de índice de linha (0,1,2...) que existe
    em todas as amostras, deixando ``df.columns`` apenas com colunas de dados.
    """
    df = pd.read_csv(
        Path(path),
        sep="\t",
        dtype=str,
        on_bad_lines="skip",
        index_col=0,
    )
    # Normaliza nomes de coluna para str (pandas pode inferir int quando o
    # "cabeçalho" é numérico — caso típico de tabelas sem_cabecalho).
    df.columns = [str(c) for c in df.columns]
    return df


def carregar_amostra(
    table_name: str, data_dir: Path | str | None = None
) -> pd.DataFrame:
    """Lê a amostra de ``data/table_samples/<table_name>.csv``.

    A leitura usa ``sep="\\t"``, ``dtype=str`` e ``on_bad_lines="skip"`` conforme
    spec. A primeira coluna (índice de linha) é usada como ``index`` do pandas.
    """
    base = _resolve_data_dir(data_dir)
    path = base / "table_samples" / f"{table_name}.csv"
    return carregar_csv(path)


def carregar_schema(data_dir: Path | str | None = None) -> "duckdb.DuckDBPyRelation":
    """Registra ``data/columns_*.csv`` como view ``schema`` e retorna a relação.

    O schema do PostgreSQL tem colunas ``table_name, column_name, data_type``.
    A relação retornada permite queries SQL cross-tabela sobre o schema.
    """
    base = _resolve_data_dir(data_dir)
    glob = (base / "columns_*.csv").as_posix().replace("'", "''")
    _DUCK.execute(
        "CREATE OR REPLACE VIEW schema AS "
        f"SELECT * FROM read_csv_auto('{glob}', header=true)"
    )
    return _DUCK.sql("SELECT * FROM schema")
