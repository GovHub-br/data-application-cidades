#!/usr/bin/env python3
"""Gera queries SQL de amostragem para validação de conteúdo (Fase 2).

Lê o CSV de tabelas relacionadas, seleciona pares representativos e gera
um arquivo .sql com SELECT LIMIT 5 para execução no DBeaver.

Uso:
    uv run python scripts/sftp/gerar_queries_amostragem.py
    uv run python scripts/sftp/gerar_queries_amostragem.py -n 15 -o /tmp/queries.sql
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


def escape_tabela(nome: str) -> str:
    """Escapa nome de tabela para SQL se necessário."""
    nome = str(nome)
    if re.search(r"[ .\-()]", nome) or nome[0].isdigit() or nome.startswith("_"):
        return f'"{nome}"'
    return nome


def selecionar_pares(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Seleciona N pares representativos para amostragem.

    Prioridade: hash_exato > stem_canonico > jaccard_colunas.
    Dentro de cada categoria, prioriza maior score_similaridade.
    """
    selecionados: list[pd.DataFrame] = []

    # 1. Hash exato: 1 por hash distinto
    df_hash = df[df["metodo"] == "hash_exato"]
    if not df_hash.empty:
        # Agrupar por tabela_dump como proxy de grupo de hash
        grupos = df_hash.groupby("tabela_dump", sort=False)
        for _, grupo in grupos:
            if len(selecionados) < n:
                selecionados.append(grupo.nlargest(1, "qtd_campos_comum"))

    # 2. Stem canônico: 1 por família (agrupando por prefixo de nome)
    if len(selecionados) < n:
        df_stem = df[df["metodo"] == "stem_canonico"]
        if not df_stem.empty:
            # Usar os primeiros 3 tokens do nome da tabela SFTP como família
            df_stem = df_stem.copy()
            df_stem["familia"] = df_stem["tabela_sftp"].apply(
                lambda x: "_".join(str(x).split("_")[:3])
            )
            for _, grupo in df_stem.groupby("familia", sort=False):
                if len(selecionados) < n:
                    ja_selecionadas = {
                        str(row["tabela_sftp"])
                        for s in selecionados
                        for _, row in s.iterrows()
                    }
                    grupo = grupo[~grupo["tabela_sftp"].isin(ja_selecionadas)]
                    if not grupo.empty:
                        selecionados.append(grupo.nlargest(1, "score_similaridade"))

    # 3. Completar com Jaccard
    if len(selecionados) < n:
        df_jac = df[df["metodo"] == "jaccard_colunas"].nlargest(
            n - len(selecionados), "score_similaridade"
        )
        if not df_jac.empty:
            selecionados.append(df_jac)

    if not selecionados:
        return pd.DataFrame(columns=df.columns)

    result = pd.concat(selecionados, ignore_index=True)
    return result.head(n)


def gerar_sql(pares: pd.DataFrame, output: Path) -> None:
    """Gera arquivo SQL com queries de amostragem."""
    with open(output, "w", encoding="utf-8") as f:
        f.write("-- Queries de Amostragem — Batimento Dump × SFTP\n")
        f.write(f"-- Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- Total de pares selecionados: {len(pares)}\n")
        f.write("-- Critério: prioriza confiança alta/média, diversidade de famílias\n")
        f.write("-- Execute estas queries no DBeaver e salve cada resultado como CSV\n")
        f.write("-- em data/sftp/artefatos/amostra_<schema>_<tabela>.csv\n\n")

        for i, (_, row) in enumerate(pares.iterrows(), 1):
            ts = escape_tabela(row["tabela_sftp"])
            td = escape_tabela(row["tabela_dump"])
            f.write(f"-- {'=' * 60}\n")
            f.write(
                f"-- Par {i}/{len(pares)}: {row['tabela_sftp']} ↔ {row['tabela_dump']}\n"
            )
            f.write(
                f"-- Método: {row['metodo']}, Confiança: {row['confianca']}, Score: {row['score_similaridade']}\n"
            )
            f.write(f"-- {'=' * 60}\n")
            f.write(f"SELECT * FROM sftp.{ts} LIMIT 5;\n")
            f.write(f"SELECT * FROM dados_historicos.{td} LIMIT 5;\n\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera queries SQL de amostragem")
    parser.add_argument(
        "--input", "-i", default="data/sftp/analise/tabelas_relacionadas.csv"
    )
    parser.add_argument("--output", "-o", default=None, help="Arquivo .sql de saída")
    parser.add_argument(
        "--num-pares", "-n", type=int, default=10, help="Número de pares"
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Erro: arquivo não encontrado: {input_path}")
        print(
            "Execute primeiro o batimento estrutural:"
            " uv run python scripts/sftp/batimento_estrutura.py"
        )
        sys.exit(1)

    if args.output:
        output = Path(args.output)
    else:
        ts = datetime.now().strftime("%Y%m%d%H%M")
        output = Path("data/sftp/artefatos") / f"{ts}_queries_amostragem.sql"

    df = pd.read_csv(input_path, dtype=str)
    if "score_similaridade" in df.columns:
        df["score_similaridade"] = pd.to_numeric(
            df["score_similaridade"], errors="coerce"
        )
    if "qtd_campos_comum" in df.columns:
        df["qtd_campos_comum"] = pd.to_numeric(df["qtd_campos_comum"], errors="coerce")

    pares = selecionar_pares(df, args.num_pares)
    if pares.empty:
        print("Nenhum par de tabelas encontrado para amostragem.")
        sys.exit(0)

    gerar_sql(pares, output)
    print(f"Queries geradas: {output}")
    print(f"Total de pares selecionados: {len(pares)}")
    print("\nResumo por método:")
    for metodo in pares["metodo"].unique():
        print(f"  {metodo}: {len(pares[pares['metodo'] == metodo])} pares")


if __name__ == "__main__":
    main()
