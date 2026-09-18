"""Tratamento de casos especiais: pipe-separator, sem cabeçalho,
vazias e dados sem utilidade.

Cada função recebe os parâmetros necessários e retorna um DataFrame
tratado + dict de metadados (ou apenas o dict para descartes).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .carregamento import _DATA_DIR, _DUCK
from .tratamento import (
    clean_dataframe,
    extrair_periodo_filename,
    inferir_instituicao,
    tratar_bem_formada,
)


def tratar_separador_pipe(
    table_name: str, data_dir: Path | None = None
) -> tuple[pd.DataFrame, dict]:
    """Trata tabela classificada como 'separador_|'.

    O arquivo é tab-separated, mas os valores de dados (na primeira
    coluna de dados) contêm pipe separando múltiplos campos. A função
    extrai esses campos, expande para colunas e aplica o pipeline
    ``tratar_bem_formada``.

    O perfil reportado é ``'separador_pipe'``.

    Args:
        table_name: Nome da tabela (sem extensão).
        data_dir: Diretório base de dados (usa o padrão se ``None``).

    Returns:
        Tupla ``(df_tratado, info_dict)``.
    """
    base = Path(data_dir) if data_dir is not None else _DATA_DIR
    path = base / "table_samples" / f"{table_name}.csv"

    # Carrega com tab (formato padrão) para obter as células com pipe
    df_original = pd.read_csv(
        path,
        sep="\t",
        dtype=str,
        on_bad_lines="skip",
        index_col=0,
        header=0,
    )

    if df_original.empty:
        df_vazio = pd.DataFrame()
        info_vazio = {
            "profile": "separador_pipe",
            "institution": inferir_instituicao(table_name),
            "report_date": extrair_periodo_filename(table_name) or "",
            "n_cols_original": 0,
            "n_cols_tratado": 0,
            "n_rows": 0,
            "warnings": ["arquivo vazio"],
        }
        return df_vazio, info_vazio

    # A primeira coluna de dados contém valores pipe-separados
    pipe_col = df_original.iloc[:, 0]

    # Expande cada valor por pipe em múltiplas colunas
    expandidos = pipe_col.dropna().str.split("|", expand=True)

    # Se a expansão falhou, retorna vazio
    if expandidos is None or expandidos.empty:
        df_vazio = pd.DataFrame()
        info_vazio = {
            "profile": "separador_pipe",
            "institution": inferir_instituicao(table_name),
            "report_date": extrair_periodo_filename(table_name) or "",
            "n_cols_original": 1,
            "n_cols_tratado": 0,
            "n_rows": 0,
            "warnings": ["falha ao expandir pipe separator"],
        }
        return df_vazio, info_vazio

    # Nomeia colunas
    n_cols = expandidos.shape[1]
    expandidos.columns = [f"campo_{i}" for i in range(n_cols)]

    # Limpeza pré-tratamento (remove linhas/colunas vazias)
    expandidos = clean_dataframe(expandidos)

    # Aplica pipeline bem_formada
    df_tratado, info = tratar_bem_formada(table_name, expandidos)

    # Sobrescreve o perfil
    info["profile"] = "separador_pipe"
    if "profile" in df_tratado.columns:
        df_tratado["profile"] = "separador_pipe"

    return df_tratado, info


def _carregar_inventario_colunas(data_dir: Path | None = None) -> pd.DataFrame:
    """Carrega o inventário de colunas do PostgreSQL em um DataFrame.

    Lê o arquivo ``columns_*.csv`` do diretório de dados e retorna um
    DataFrame com colunas ``table_name``, ``column_name``, ``data_type``.
    """
    base = Path(data_dir) if data_dir is not None else _DATA_DIR
    cols_csv = sorted(base.glob("columns_*.csv"))
    if not cols_csv:
        return pd.DataFrame(columns=["table_name", "column_name", "data_type"])
    return pd.read_csv(cols_csv[0], dtype=str)


def _buscar_tabela_referencia(
    n_cols: int, instituicao: str, data_dir: Path | None = None
) -> list[str] | None:
    """Busca no inventário uma tabela bem formada com mesmo número de
    colunas e mesma instituição.

    Usa o schema carregado no duckdb para encontrar uma tabela de
    referência. Prioriza tabelas ``caixa_`` ou ``bb_`` com exatamente
    ``n_cols`` colunas. Retorna os nomes de coluna ou ``None``.
    """
    base = Path(data_dir) if data_dir is not None else _DATA_DIR

    # Garante que o schema está registrado no duckdb
    cols_csv = sorted(base.glob("columns_*.csv"))
    if not cols_csv:
        return None

    try:
        _DUCK.execute(
            "CREATE OR REPLACE VIEW schema_inventario AS "
            f"SELECT * FROM read_csv_auto('{cols_csv[0].as_posix()}', header=true)"
        )
    except Exception:  # noqa: BLE001
        pass

    # Prepara prefixo para filtro
    prefixos = (
        [f"{instituicao}_"] if instituicao in ("bb", "caixa") else ["bb_", "caixa_"]
    )

    for prefixo in prefixos:
        try:
            rel = _DUCK.sql(
                """
                SELECT column_name FROM schema_inventario
                WHERE table_name LIKE ?
                GROUP BY table_name
                HAVING COUNT(*) = ?
                ORDER BY table_name
                LIMIT 1
                """,
                params=[f"{prefixo}%", n_cols],
            )
            rows = rel.fetchall()
            if rows:
                table = _DUCK.sql(
                    """
                    SELECT column_name FROM schema_inventario
                    WHERE table_name = (
                        SELECT table_name FROM schema_inventario
                        WHERE table_name LIKE ?
                        GROUP BY table_name
                        HAVING COUNT(*) = ?
                        ORDER BY table_name
                        LIMIT 1
                    )
                    ORDER BY column_name
                    """,
                    params=[f"{prefixo}%", n_cols],
                )
                return [str(r[0]) for r in table.fetchall()]
        except Exception:  # noqa: BLE001
            continue

    return None


def tratar_sem_cabecalho(
    table_name: str,
    df: pd.DataFrame,
    data_dir: Path | None = None,
    inferir_nomes: bool = True,
) -> tuple[pd.DataFrame, dict]:
    """Trata tabela sem cabeçalho inferindo nomes de coluna.

    Estratégia:
    1. Tenta encontrar tabela de referência (bem formada) com mesmo
       número de colunas e mesma instituição no inventário. Se
       encontrar, usa os nomes dela.
    2. Se não encontrar e ``inferir_nomes=True``, infere nomes a partir
       dos dados usando heurísticas de domínio MCMV.
    3. Caso contrário, nomeia ``col_0``, ``col_1``, ...
    4. Aplica pipeline ``tratar_bem_formada``.
    5. Adiciona warning sobre confiança da inferência.

    Args:
        table_name: Nome da tabela (sem extensão).
        df: DataFrame bruto (sem cabeçalho).
        data_dir: Diretório base de dados (opcional).
        inferir_nomes: Se ``True`` (default), infere nomes descritivos
            a partir do conteúdo quando o matching de referência falhar.

    Returns:
        Tupla ``(df_tratado, info_dict)``.
    """
    from classificacao.inferencia_colunas import inferir_nomes_colunas

    n_cols = len(df.columns)
    instituicao = inferir_instituicao(table_name)

    # Tenta encontrar tabela de referência
    nomes_referencia = _buscar_tabela_referencia(n_cols, instituicao, data_dir)

    if nomes_referencia and len(nomes_referencia) == n_cols:
        df = df.copy()
        df.columns = nomes_referencia
        confidence_note = "nomes inferidos de tabela de referencia no inventario"
    elif inferir_nomes:
        df = df.copy()
        nomes, confidence_note = inferir_nomes_colunas(df)
        df.columns = nomes
    else:
        df = df.copy()
        df.columns = [f"col_{i}" for i in range(n_cols)]
        confidence_note = "nomes genericos (col_0...) - inferencia desabilitada"

    # Limpeza pré-tratamento (remove linhas/colunas vazias)
    df = clean_dataframe(df)

    # Aplica pipeline bem_formada
    df_tratado, info = tratar_bem_formada(table_name, df)

    # Adiciona warning sobre confiança
    info.setdefault("warnings", []).append(confidence_note)

    return df_tratado, info


def tratar_vazia(table_name: str) -> dict:
    """Registra tabela vazia como descartada.

    Não gera CSV de saída. Retorna dict com status ``discarded``.

    Args:
        table_name: Nome da tabela.

    Returns:
        Dict com metadados de descarte.
    """
    return {
        "table_name": table_name,
        "status": "discarded",
        "reason": "vazia",
        "n_rows": 0,
        "n_cols": 0,
        "profile": "vazia",
        "institution": inferir_instituicao(table_name),
        "report_date": extrair_periodo_filename(table_name) or "",
        "missing_pct": 0.0,
        "encoding_issues": 0,
        "date_parse_errors": 0,
        "type_coercion_warnings": 0,
    }


def tratar_dados_sem_utilidade(table_name: str) -> dict:
    """Registra tabela sem utilidade como descartada.

    Não gera CSV de saída. Retorna dict com status ``discarded``.

    Args:
        table_name: Nome da tabela.

    Returns:
        Dict com metadados de descarte.
    """
    return {
        "table_name": table_name,
        "status": "discarded",
        "reason": "dados_sem_utilidade",
        "n_rows": 0,
        "n_cols": 0,
        "profile": "dados_sem_utilidade",
        "institution": inferir_instituicao(table_name),
        "report_date": extrair_periodo_filename(table_name) or "",
        "missing_pct": 0.0,
        "encoding_issues": 0,
        "date_parse_errors": 0,
        "type_coercion_warnings": 0,
    }
