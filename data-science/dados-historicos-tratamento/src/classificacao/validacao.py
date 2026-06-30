"""Validação pós-tratamento de tabelas tratadas.

Funções puras que verificam a qualidade da saída do pipeline de tratamento
e reportam issues para o relatório de qualidade.
"""

from __future__ import annotations

import pandas as pd


def validar_shape(df: pd.DataFrame) -> list[str]:
    """Verifica que o DataFrame tratado tem pelo menos 1 linha e 1 coluna.

    Retorna lista de warnings (vazia se OK).
    """
    warnings: list[str] = []
    if len(df) == 0:
        warnings.append("DataFrame tratado tem 0 linhas")
    if len(df.columns) == 0:
        warnings.append("DataFrame tratado tem 0 colunas")
    return warnings


def validar_chaves_nao_nulas(
    df: pd.DataFrame, colunas_chave: list[str] | None = None
) -> list[str]:
    """Verifica que colunas de chave não têm valores nulos.

    Se ``colunas_chave`` não for fornecido, detecta automaticamente colunas
    cujo nome contém 'apf', 'cod_', 'cnpj', 'contrato', 'nr_'.
    """
    warnings: list[str] = []
    if colunas_chave is None:
        colunas_chave = [
            c
            for c in df.columns
            if any(
                k in str(c).lower() for k in ("apf", "cod_", "cnpj", "contrato", "nr_")
            )
        ]
    for col in colunas_chave:
        if col in df.columns:
            series = pd.Series(df[col])
            n_null = int(series.isna().sum())
            if n_null > 0:
                warnings.append(f"Coluna chave '{col}' tem {n_null} valores nulos")
    return warnings


def calcular_missing_pct(df: pd.DataFrame) -> float:
    """Calcula percentual de valores ausentes no DataFrame."""
    if df.size == 0:
        return 0.0
    return float(df.isna().sum().sum() / df.size * 100)


def validar_tabela(df: pd.DataFrame) -> dict:
    """Executa todas as validações e retorna um dict de métricas.

    Campos:
    - ``shape_ok``: bool
    - ``n_rows``: int
    - ``n_cols``: int
    - ``missing_pct``: float
    - ``warnings``: list[str]
    """
    warnings = validar_shape(df)
    warnings.extend(validar_chaves_nao_nulas(df))
    return {
        "shape_ok": len(df) > 0 and len(df.columns) > 0,
        "n_rows": len(df),
        "n_cols": len(df.columns),
        "missing_pct": calcular_missing_pct(df),
        "warnings": warnings,
    }
