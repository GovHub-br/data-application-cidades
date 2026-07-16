"""Escrita de saída e relatórios de qualidade do pipeline de tratamento.

Duas funções principais:

- ``escrever_saida`` — grava DataFrame tratado como CSV tab-separated
- ``gerar_relatorio_qualidade`` — compila métricas de qualidade de todos
  os resultados do pipeline em um DataFrame resumo
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def escrever_saida(df: pd.DataFrame, table_name: str, output_dir: Path) -> Path:
    """Escreve CSV tratado em ``output_dir/<table_name>_tratado.csv``.

    Formato: tab-separated (``sep='\\t'``), ``index=False``. Cria o
    diretório se não existir.

    Args:
        df: DataFrame tratado.
        table_name: Nome base da tabela (sem extensão).
        output_dir: Diretório de saída.

    Returns:
        ``Path`` do arquivo escrito.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    path = output_dir / f"{table_name}_tratado.csv"
    df.to_csv(path, sep="\t", index=False)
    return path


def gerar_relatorio_qualidade(
    resultados: list[dict],
    output_path: Path,
) -> pd.DataFrame:
    """Compila métricas de qualidade a partir da lista de info_dicts
    retornados pelos pipelines de tratamento.

    Colunas do relatório:
    - ``table_name`` — nome da tabela
    - ``status`` — ``treated`` ou ``discarded``
    - ``n_rows`` — número de linhas (0 para descartadas)
    - ``n_cols`` — número de colunas (0 para descartadas)
    - ``profile`` — perfil estrutural (ou reason para descartadas)
    - ``institution`` — ``bb``, ``caixa`` ou ``unknown``
    - ``report_date`` — data de referência extraída do nome
    - ``data_de_movimento`` — data de movimento preservada da coluna 0 (vazio se não aplicável)
    - ``missing_pct`` — percentual de valores ausentes
    - ``encoding_issues`` — contagem de problemas de encoding
    - ``date_parse_errors`` — contagem de falhas no parsing de datas
    - ``type_coercion_warnings`` — contagem de warnings de coerção
    - ``error`` — mensagem da exceção para tabelas com status=error (string vazia caso contrário)

    Para tabelas descartadas: ``n_rows=0``, ``n_cols=0``,
    ``profile=reason``.

    Args:
        resultados: Lista de dicts de informação (info_dict) retornados
            por cada pipeline de tratamento.
        output_path: Caminho para salvar o CSV do relatório.

    Returns:
        ``DataFrame`` com as métricas compiladas.
    """
    colunas = [
        "table_name",
        "status",
        "n_rows",
        "n_cols",
        "profile",
        "institution",
        "report_date",
        "data_de_movimento",
        "missing_pct",
        "encoding_issues",
        "date_parse_errors",
        "type_coercion_warnings",
        "error",
        "metadata_warnings",
        "colunas_removidas",
        "linhas_removidas",
        "linhas_header_removidas",
    ]

    registros: list[dict] = []
    for res in resultados:
        registro: dict = {}
        for col in colunas:
            val = res.get(col, "")
            # Serializa listas/dicts como string para CSV
            if isinstance(val, list):
                val = "; ".join(str(v) for v in val)
            elif isinstance(val, dict):
                val = "; ".join(f"{k}: {v}" for k, v in val.items())
            registro[col] = val
        # Para descartados, garante profile = reason
        if registro["status"] == "discarded":
            registro["n_rows"] = 0
            registro["n_cols"] = 0
            registro["profile"] = res.get("reason", "discarded")
        registros.append(registro)

    df_relatorio = pd.DataFrame(registros, columns=colunas)
    df_relatorio = df_relatorio.sort_values("table_name").reset_index(drop=True)

    # Escreve CSV
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_relatorio.to_csv(output_path, sep="\t", index=False)

    return df_relatorio
