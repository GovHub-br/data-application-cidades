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

import re
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


def _detectar_data_movimento_col0(path: Path) -> bool:
    """Detecta se a coluna 0 contém datas de movimento (DD/MM/YYYY ou YYYY-MM-DD).

    Lê as primeiras 20 linhas do CSV tab-separated, ignora o cabeçalho (linha 0),
    e verifica se pelo menos 80% dos valores não-nulos na coluna 0 correspondem
    a padrões de data ``\\d{2}/\\d{2}/\\d{4}`` ou ``\\d{4}-\\d{2}-\\d{2}``.

    Retorna ``True`` se a coluna 0 parece conter datas (data_de_movimento),
    ``False`` caso contrário (provavelmente é um índice de linha numérico).
    """
    date_pattern = re.compile(r"^\d{2}/\d{2}/\d{4}$|^\d{4}-\d{2}-\d{2}$")

    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = []
            for i, line in enumerate(f):
                if i >= 20:
                    break
                lines.append(line)
    except Exception:
        return False

    # Precisamos de pelo menos 1 linha de dados além do cabeçalho.
    if len(lines) < 2:
        return False

    # Ignora cabeçalho (linha 0); extrai coluna 0 das linhas de dados.
    values: list[str] = []
    for line in lines[1:]:
        parts = line.strip().split("\t")
        if parts:
            val = parts[0].strip()
            if val:  # não-nulo
                values.append(val)

    if not values:
        return False

    matches = sum(1 for v in values if date_pattern.match(v))
    return (matches / len(values)) >= 0.80


def carregar_csv(path: Path | str) -> pd.DataFrame:
    """Lê um CSV tab-separated como strings, com a primeira coluna como índice.

    Parâmetros de leitura (compartilhados por amostras reais e fixtures):
    ``sep="\\t"``, ``dtype=str``, ``on_bad_lines="skip"``, ``index_col=0``.
    O ``index_col=0`` descarta a coluna de índice de linha (0,1,2...) que existe
    em todas as amostras, deixando ``df.columns`` apenas com colunas de dados.

    Quando a coluna 0 contém datas de movimento (detectado por
    ``_detectar_data_movimento_col0``), a coluna é mantida como uma coluna regular
    ``data_de_movimento`` em vez de ser usada como índice.
    """
    path_obj = Path(path)

    if _detectar_data_movimento_col0(path_obj):
        # Lê sem index_col=0 para preservar a coluna de datas
        df = pd.read_csv(
            path_obj,
            sep="\t",
            dtype=str,
            on_bad_lines="skip",
        )

        # Renomeia a primeira coluna se o cabeçalho for genérico
        first_col_name: str = str(df.columns[0])
        first_col_str = first_col_name.lower().strip()
        is_generic: bool = (
            first_col_str == "0"
            or first_col_str == ""
            or first_col_str == "unnamed:0"
            or first_col_str == "unnamed_0"
            or first_col_str.startswith("unnamed")
        )

        col_name = "data_de_movimento" if is_generic else first_col_name
        if is_generic:
            df.rename(columns={first_col_name: col_name}, inplace=True)

        # Normaliza valores de data para ISO (YYYY-MM-DD)
        def _normalize_date(val: object) -> object:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return val
            val_str = str(val).strip()
            # DD/MM/YYYY → YYYY-MM-DD
            m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", val_str)
            if m:
                return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
            # Já está em YYYY-MM-DD ou outro formato — mantém como está
            return val_str

        df[col_name] = df[col_name].apply(_normalize_date)

        # Normaliza nomes de coluna para str (consistência com o comportamento padrão)
        df.columns = [str(c) for c in df.columns]
        return df

    # Comportamento padrão: usa a primeira coluna como índice
    df = pd.read_csv(
        path_obj,
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
