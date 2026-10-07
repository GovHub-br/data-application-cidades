"""Escrita da tabela de classificação em CSV."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .classificador import COLUNAS


def escrever_classificacao(df_resultados: pd.DataFrame, path: str | Path) -> Path:
    """Escreve ``data/classificacao_formacao.csv`` com colunas
    ``table_name, formacao, confidence, notes``.

    O ``DataFrame`` é ordenado por ``table_name`` para garantir output
    reproduzível entre execuções.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = df_resultados[COLUNAS].sort_values(by=["table_name"]).reset_index(drop=True)
    df.to_csv(path, index=False)
    return path
