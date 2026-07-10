#!/usr/bin/env python3
"""Verifica consistência temporal entre nomes de arquivo e dados no dump.

Para cada CSV nos diretórios de dados históricos e dados históricos formatados,
extrai a data a partir do nome do arquivo (usando padrões de regex em ordem) e
a data a partir do conteúdo (coluna data_de_movimento / report_date), compara
as duas e classifica o resultado como ok, divergente ou indeterminado.

Uso:
    uv run python scripts/sftp/verificar_consistencia_temporal.py
    uv run python scripts/sftp/verificar_consistencia_temporal.py \
        --dh-dir data/dados_historicos/table_samples/ \
        --dhf-dir data/dados_historicos_formatados/table_samples/ \
        --output data/sftp/analise/consistencia_temporal.csv
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# Regex patterns para extrair data do nome do arquivo (em ordem de precedência)
# Cada padrão retorna uma tupla (ano, mês, dia) ou (ano, mês) ou string YYYYMM.
_PATTERNS: list[tuple[str, str]] = [
    # 1. historico_recente_YYYYMM_ → YYYY-MM-01
    (r"historico_recente_(\d{4})(\d{2})_", "YYYYMM-01"),
    # 2. o_recente_YYYYMM_ → YYYY-MM-01
    (r"o_recente_(\d{4})(\d{2})_", "YYYYMM-01"),
    # 3. (ecente|storico)_YYYY_MM_ → YYYY-MM-01
    (r"^(ecente|storico)_(20\d{2})_(\d{2})_", "storico_YYYY_MM"),
    # 4. ^YYYYMM_  → YYYYMM01
    (r"^(\d{6})_", "YYYYMM"),
    # 5. _YYYY_MM_  → YYYY-MM-01   (qualquer posição)
    (r"_(\d{4})_(\d{2})_", "YYYY_MM"),
    # 6. _YYYYMMDD$ → YYYY-MM-DD
    (r"_(\d{8})$", "YYYYMMDD"),
    # 7. _DD_MM_YYYY$ → YYYY-MM-DD
    (r"_(\d{2})_(\d{2})_(\d{4})$", "DD_MM_YYYY"),
]


def _extrair_data_nome(stem: str) -> Optional[str]:
    """Extrai uma data no formato YYYY-MM-DD a partir do stem do arquivo.

    Percorre os padrões de regex em ordem e usa o primeiro que encontrar.
    Retorna None se nenhum padrão casar.
    """
    for pattern, fmt in _PATTERNS:
        m = re.search(pattern, stem)
        if not m:
            continue

        if fmt == "YYYYMM-01":  # padrões 1 e 2
            year, month = m.group(1), m.group(2)
            return f"{year}-{month}-01"

        if fmt == "storico_YYYY_MM":  # padrão 3
            year, month = m.group(2), m.group(3)
            return f"{year}-{month}-01"

        if fmt == "YYYYMM":  # padrão 4: ^(6 dígitos)_
            yyyymm = m.group(1)
            return f"{yyyymm[:4]}-{yyyymm[4:]}-01"

        if fmt == "YYYY_MM":  # padrão 5: _YYYY_MM_
            year, month = m.group(1), m.group(2)
            return f"{year}-{month}-01"

        if fmt == "YYYYMMDD":  # padrão 6: _YYYYMMDD$
            yyyymmdd = m.group(1)
            return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"

        if fmt == "DD_MM_YYYY":  # padrão 7: _DD_MM_YYYY$
            day, month, year = m.group(1), m.group(2), m.group(3)
            return f"{year}-{month}-{day}"

    return None


def _normalizar_data(valor: str) -> Optional[str]:
    """Normaliza valor de data para YYYY-MM-DD.

    Reconhece os formatos:
      - DD/MM/YYYY
      - YYYY-MM-DD
      - DD/MM/YY (assume 20YY)
      - YYYYMMDD (8 dígitos)
    Retorna None se não for possível interpretar.
    """
    if pd.isna(valor) or not isinstance(valor, str):
        return None
    valor = valor.strip().strip('"').strip()
    if not valor:
        return None

    # DD/MM/YYYY
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", valor)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", valor)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # DD/MM/YY
    m = re.match(r"^(\d{2})/(\d{2})/(\d{2})$", valor)
    if m:
        return f"20{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # YYYYMMDD (8 dígitos)
    m = re.match(r"^(\d{4})(\d{2})(\d{2})$", valor)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    return None


def _parse_date_iso(data_str: str) -> Optional[datetime]:
    """Converte string YYYY-MM-DD para datetime."""
    try:
        return datetime.strptime(data_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _extrair_data_dados(caminho: Path) -> tuple[Optional[str], Optional[str]]:
    """Extrai a data a partir do conteúdo do CSV.

    Tenta primeiro ler sem index_col=0, depois com index_col=0,
    procurando pela coluna 'data_de_movimento'. Se não encontrar,
    procura por 'report_date' (presente nos formatados).

    Retorna (data_normalizada YYYY-MM-DD, fonte_da_data).
    """
    # --- Tentativa 1: sem index_col=0 ---
    try:
        df = pd.read_csv(
            caminho,
            sep="\t",
            dtype=str,
            on_bad_lines="skip",
            nrows=5,
        )
        data, fonte = _procurar_data_no_df(df)
        if data is not None:
            return data, fonte
    except Exception:
        pass

    # --- Tentativa 2: com index_col=0 ---
    try:
        df = pd.read_csv(
            caminho,
            sep="\t",
            dtype=str,
            on_bad_lines="skip",
            nrows=5,
            index_col=0,
        )
        data, fonte = _procurar_data_no_df(df)
        if data is not None:
            return data, fonte
    except Exception:
        pass

    return None, None


def _procurar_data_no_df(df: pd.DataFrame) -> tuple[Optional[str], Optional[str]]:
    """Procura coluna de data no DataFrame e retorna o primeiro valor não-nulo."""
    # Normalizar nomes de colunas para lower-strip
    colunas = {str(c).strip().lower(): c for c in df.columns}

    # 1. data_de_movimento
    for alias in ("data_de_movimento", "data do movimento", "data_movimento"):
        if alias in colunas:
            col = colunas[alias]
            valores = df[col].dropna().astype(str).str.strip('"').str.strip()
            for v in valores:
                norm = _normalizar_data(v)
                if norm is not None:
                    return norm, "data_de_movimento"

    # 2. report_date (presente nos formatados)
    for alias in ("report_date", "data_reporte", "data_referencia"):
        if alias in colunas:
            col = colunas[alias]
            valores = df[col].dropna().astype(str).str.strip('"').str.strip()
            for v in valores:
                norm = _normalizar_data(v)
                if norm is not None:
                    return norm, "report_date"

    return None, None


def _processar_arquivo(
    caminho: Path,
    schema: str,
) -> dict:
    """Processa um único CSV e retorna um dicionário com os resultados."""
    tabela = caminho.name
    stem = caminho.stem

    # Extrair data do nome do arquivo
    data_nome = _extrair_data_nome(stem)

    # Extrair data do conteúdo
    data_dados, fonte = _extrair_data_dados(caminho)

    # Comparar
    diferenca_dias: Optional[int] = None
    status = "indeterminado"

    if data_nome is not None and data_dados is not None:
        dt_nome = _parse_date_iso(data_nome)
        dt_dados = _parse_date_iso(data_dados)
        if dt_nome is not None and dt_dados is not None:
            diff = abs((dt_dados - dt_nome).days)
            diferenca_dias = diff
            status = "ok" if diff <= 31 else "divergente"

    return {
        "tabela": tabela,
        "schema": schema,
        "data_nome_arquivo": data_nome if data_nome else "",
        "data_dados": data_dados if data_dados else "",
        "fonte_data_dados": fonte if fonte else "",
        "diferenca_dias": diferenca_dias if diferenca_dias is not None else "",
        "status": status,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verifica consistência temporal entre nomes de arquivo e dados"
    )
    parser.add_argument(
        "--dh-dir",
        default="data/dados_historicos/table_samples/",
        help="Diretório com amostras de dados históricos (CSV)",
    )
    parser.add_argument(
        "--dhf-dir",
        default="data/dados_historicos_formatados/table_samples/",
        help="Diretório com amostras de dados históricos formatados (CSV)",
    )
    parser.add_argument(
        "--output",
        default="data/sftp/analise/consistencia_temporal.csv",
        help="Caminho para o CSV de saída",
    )
    args = parser.parse_args()

    dh_dir = Path(args.dh_dir)
    dhf_dir = Path(args.dhf_dir)
    output_path = Path(args.output)

    # Garantir diretório de saída
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Coletar todos os CSVs
    resultados: list[dict] = []

    def _coletar(diretorio: Path, schema: str) -> None:
        if not diretorio.is_dir():
            print(f"Aviso: diretório não encontrado: {diretorio}", file=sys.stderr)
            return
        for csv_path in sorted(diretorio.glob("*.csv")):
            try:
                res = _processar_arquivo(csv_path, schema)
                resultados.append(res)
            except Exception as e:
                print(
                    f"Erro ao processar {csv_path.name}: {e}",
                    file=sys.stderr,
                )
                resultados.append(
                    {
                        "tabela": csv_path.name,
                        "schema": schema,
                        "data_nome_arquivo": "",
                        "data_dados": "",
                        "fonte_data_dados": "",
                        "diferenca_dias": "",
                        "status": "indeterminado",
                    }
                )

    _coletar(dh_dir, "dados_historicos")
    _coletar(dhf_dir, "dados_historicos_formatados")

    if not resultados:
        print("Nenhum CSV encontrado para análise.")
        return

    # --- Escrever CSV de saída ---
    colunas = [
        "tabela",
        "schema",
        "data_nome_arquivo",
        "data_dados",
        "fonte_data_dados",
        "diferenca_dias",
        "status",
    ]
    df = pd.DataFrame(resultados, columns=colunas)
    df.to_csv(output_path, index=False)
    print(f"Resultado salvo: {output_path}")

    # --- Resumo no stdout ---
    total = len(df)
    contagem = df["status"].value_counts()
    ok_count = contagem.get("ok", 0)
    div_count = contagem.get("divergente", 0)
    ind_count = contagem.get("indeterminado", 0)

    print(f"\n{'=' * 50}")
    print("RESUMO DE CONSISTÊNCIA TEMPORAL")
    print(f"{'=' * 50}")
    print(f"  Total de tabelas analisadas: {total}")
    print(f"  Status ok:                   {ok_count}")
    print(f"  Status divergente:           {div_count}")
    print(f"  Status indeterminado:        {ind_count}")
    print()

    # Top 5 maiores discrepâncias (apenas divergentes com diferença numérica)
    divergentes_list = []
    for _, row in df[df["status"] == "divergente"].iterrows():
        diff_val = row.get("diferenca_dias", "")
        try:
            diff_int = int(str(diff_val))
            divergentes_list.append(
                (str(row.get("tabela", "")), str(row.get("schema", "")), diff_int)
            )
        except (ValueError, TypeError):
            pass

    if divergentes_list:
        divergentes_list.sort(key=lambda x: x[2], reverse=True)
        top5 = divergentes_list[:5]
        print("  Top 5 maiores discrepâncias:")
        print(f"  {'Tabela':<55} {'Schema':<28} {'Dias':<8}")
        print(f"  {'-'*55} {'-'*28} {'-'*8}")
        for tabela, schema, dias in top5:
            print(f"  {tabela:<55} {schema:<28} {dias:<8}")
    else:
        print("  Nenhuma divergência com diferença mensurável.")

    print(f"\n  Relatório completo: {output_path}")


if __name__ == "__main__":
    main()
