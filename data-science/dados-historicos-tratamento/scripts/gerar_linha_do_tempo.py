"""Script: gera linha do tempo (data/linha_do_tempo.png) a partir do inventário.

Lê ``data/inventario_dados.csv`` (tab-separated) e produz 3 painéis verticais:

1. Barras consolidadas — contagem de tabelas por mês.
2. Linhas por frente — contagem por frente (FAR, PF, …).
3. Heatmap por tabela — tabelas Alta/Média com cobertura mês a mês.

Uso::

    uv run python scripts/gerar_linha_do_tempo.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# ── Caminhos ──────────────────────────────────────────────────────────────
INVENTARIO_PATH = Path("data/inventario_dados.csv")
OUTPUT_PATH = Path("data/linha_do_tempo.png")


def _carregar_inventario(path: Path) -> pd.DataFrame:
    """Carrega o CSV de inventário (tab-separated)."""
    if not path.exists():
        print(f"ERRO: Arquivo não encontrado: {path}", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(
        path,
        sep="\t",
        dtype=str,
        keep_default_na=False,
    )
    # Normaliza colunas: remove whitespace dos nomes
    df.columns = df.columns.str.strip()
    return df


def _parse_ano_mes(series: pd.Series) -> pd.Series:
    """Converte strings YYYY-MM-DD para Period[M] (ano-mês).

    Valores vazios ou inválidos retornam NaT.
    """
    return pd.to_datetime(series, format="%Y-%m-%d", errors="coerce").dt.to_period("M")


def _gerar_eixo_meses(df: pd.DataFrame) -> pd.PeriodIndex:
    """Gera um PeriodIndex contínuo de ano-mês sem buracos."""
    inicio = df["inicio_periodo"].min()
    fim = df["fim_periodo"].max()
    if pd.isna(inicio) or pd.isna(fim):
        return pd.PeriodIndex([], freq="M")
    # Gera todos os meses entre início e fim
    meses = pd.period_range(start=inicio, end=fim, freq="M")
    return meses


def _build_coverage_matrix(df: pd.DataFrame, meses: pd.PeriodIndex) -> pd.DataFrame:
    """Constrói matriz (tabela x mês) booleana de cobertura.

    Retorna DataFrame indexado por table_name, colunas = meses.
    """
    coverage = pd.DataFrame(False, index=df["table_name"], columns=meses)
    for idx, row in df.iterrows():
        table = row["table_name"]
        ini = row["inicio_periodo"]
        fim = row["fim_periodo"]
        if pd.isna(ini) or pd.isna(fim):
            continue
        # Slice dos meses cobertos
        mask = (meses >= ini) & (meses <= fim)
        coverage.loc[table, mask] = True
    return coverage


def _painel_barras(ax, meses: pd.PeriodIndex, counts: pd.Series) -> None:
    """Painel 1: barras consolidadas."""
    x_num = list(range(len(meses)))
    ax.bar(x_num, counts.values, width=0.8, color="#2c7bb6", edgecolor="none")
    ax.set_title("Cobertura temporal consolidada", fontsize=14, fontweight="bold")
    ax.set_ylabel("Quantidade de tabelas")
    ax.set_xlim(-0.5, len(meses) - 0.5)

    # Rótulos a cada 6 meses, rotacionados
    step = 6
    tick_pos = list(range(0, len(meses), step))
    tick_labels = [str(m)[:7] for m in meses[tick_pos]]
    ax.set_xticks(tick_pos)
    ax.set_xticklabels(tick_labels, rotation=45, ha="right", fontsize=8)
    ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax.grid(axis="y", alpha=0.3)


def _painel_linhas_frentes(
    ax, meses: pd.PeriodIndex, frente_counts: dict[str, pd.Series]
) -> None:
    """Painel 2: linhas por frente."""
    cores = [
        "#1f78b4",
        "#33a02c",
        "#e31a1c",
        "#ff7f00",
        "#6a3d9a",
        "#b15928",
        "#a6cee3",
        "#b2df8a",
        "#fb9a99",
        "#fdbf6f",
        "#cab2d6",
        "#8dd3c7",
    ]
    x_num = list(range(len(meses)))

    for i, (frente, series) in enumerate(sorted(frente_counts.items())):
        cor = cores[i % len(cores)]
        ax.plot(x_num, series.values, color=cor, linewidth=1.5, label=frente)

    ax.set_title("Cobertura por frente", fontsize=14, fontweight="bold")
    ax.set_ylabel("Quantidade de tabelas")
    ax.set_xlim(-0.5, len(meses) - 0.5)

    step = 6
    tick_pos = list(range(0, len(meses), step))
    tick_labels = [str(m)[:7] for m in meses[tick_pos]]
    ax.set_xticks(tick_pos)
    ax.set_xticklabels(tick_labels, rotation=45, ha="right", fontsize=8)
    ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax.grid(axis="y", alpha=0.3)
    ax.legend(fontsize=7, loc="upper left", ncol=2)


def _painel_heatmap(ax, meses: pd.PeriodIndex, coverage: pd.DataFrame) -> None:
    """Painel 3: heatmap por tabela."""
    if coverage.empty:
        ax.text(
            0.5,
            0.5,
            "Nenhuma tabela com Alta/Média utilidade",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=12,
        )
        ax.set_title("Cobertura por tabela (heatmap)", fontsize=14, fontweight="bold")
        return

    # Matriz numérica: 1 = cobertura, 0 = sem cobertura
    data = coverage.astype(int).values  # shape (n_tabelas, n_meses)

    ax.imshow(
        data,
        aspect="auto",
        cmap=plt.cm.Blues,
        interpolation="nearest",
        vmin=0,
        vmax=1,
    )
    ax.set_title("Cobertura por tabela (heatmap)", fontsize=14, fontweight="bold")
    ax.set_ylabel("Tabela")
    ax.set_xlabel("Ano-mês")

    # Abrevia nomes das tabelas (30 chars)
    nomes_curtos = [n[:30] for n in coverage.index]

    n_rows = len(nomes_curtos)
    # Mostra no máximo ~20 labels no eixo Y
    max_labels = 20
    if n_rows > max_labels:
        step_y = max(1, n_rows // max_labels)
        tick_y = list(range(0, n_rows, step_y))
        ax.set_yticks(tick_y)
        ax.set_yticklabels([nomes_curtos[i] for i in tick_y], fontsize=6)
    else:
        ax.set_yticks(range(n_rows))
        ax.set_yticklabels(nomes_curtos, fontsize=6)

    # Rótulos X a cada 6 meses
    step_x = 6
    tick_x = list(range(0, len(meses), step_x))
    ax.set_xticks(tick_x)
    ax.set_xticklabels(
        [str(m)[:7] for m in meses[tick_x]], rotation=45, ha="right", fontsize=7
    )


def main() -> None:
    # ── 1. Carregar inventário ──────────────────────────────────────────
    df = _carregar_inventario(INVENTARIO_PATH)

    col_inicio = "periodo_dados_inicio"
    col_fim = "periodo_dados_fim"
    col_frentes = "frentes_cobertas"
    col_utilidade = "classificacao_utilidade"
    col_tabela = "table_name"

    # Valida colunas essenciais
    obrigatorias = {col_tabela, col_inicio, col_fim, col_frentes, col_utilidade}
    if not obrigatorias.issubset(df.columns):
        faltantes = obrigatorias - set(df.columns)
        print(
            f"ERRO: Colunas faltantes no inventário: {faltantes}",
            file=sys.stderr,
        )
        sys.exit(1)

    # ── 2. Parse das datas ──────────────────────────────────────────────
    df["inicio_periodo"] = _parse_ano_mes(df[col_inicio])
    df["fim_periodo"] = _parse_ano_mes(df[col_fim])

    # Filtra tabelas com data válida
    df_com_data = df.dropna(subset=["inicio_periodo", "fim_periodo"]).copy()

    if df_com_data.empty:
        # ── Saída: figura vazia ────────────────────────────────────────
        fig, ax = plt.subplots(figsize=(20, 6))
        ax.text(
            0.5,
            0.5,
            "Sem dados temporais disponíveis",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=18,
            style="italic",
        )
        ax.set_title("Linha do Tempo — Inventário de Dados", fontsize=16)
        ax.axis("off")
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(str(OUTPUT_PATH), dpi=150, bbox_inches="tight")
        plt.close()
        print(f"Figura salva: {OUTPUT_PATH} (sem dados temporais)")
        return

    # ── 3. Eixo contínuo de meses ──────────────────────────────────────
    meses = _gerar_eixo_meses(df_com_data)
    if len(meses) == 0:
        fig, ax = plt.subplots(figsize=(20, 6))
        ax.text(
            0.5,
            0.5,
            "Sem dados temporais disponíveis",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=18,
            style="italic",
        )
        ax.set_title("Linha do Tempo — Inventário de Dados", fontsize=16)
        ax.axis("off")
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(str(OUTPUT_PATH), dpi=150, bbox_inches="tight")
        plt.close()
        print(f"Figura salva: {OUTPUT_PATH} (sem dados temporais)")
        return

    # ── 4. Matriz de cobertura (tabela x mês) ──────────────────────────
    coverage = _build_coverage_matrix(df_com_data, meses)

    # ── 5. Contagem consolidada (Painel 1) ──────────────────────────────
    consolidadas = coverage.sum(axis=0)  # total de tabelas por mês

    # ── 6. Contagem por frente (Painel 2) ──────────────────────────────
    # Para cada frente, conta quantas tabelas daquela frente cobrem cada mês
    frentes: dict[str, pd.Series] = {}
    for _, row in df_com_data.iterrows():
        raw = str(row[col_frentes]).strip()
        if not raw or raw == "Não identificada":
            continue
        for f in raw.split(","):
            f = f.strip()
            if not f:
                continue
            if f not in frentes:
                # Inicia com zeros
                frentes[f] = pd.Series(0, index=meses)
            # Marca cobertura para esta tabela
            ini = row["inicio_periodo"]
            fim = row["fim_periodo"]
            if pd.notna(ini) and pd.notna(fim):
                mask = (meses >= ini) & (meses <= fim)
                frentes[f][mask] += 1

    # ── 7. Heatmap (Painel 3) — apenas Alta/Média ──────────────────────
    utilidades_validas = {"Alta", "Média"}
    mask_util = df_com_data[col_utilidade].isin(utilidades_validas)
    df_heat = df_com_data[mask_util].copy()
    coverage_heat = (
        coverage.loc[df_heat["table_name"]] if not df_heat.empty else pd.DataFrame()
    )

    # ── 8. Plotagem ─────────────────────────────────────────────────────
    fig, axes = plt.subplots(3, 1, figsize=(20, 24))

    # Painel 1
    _painel_barras(axes[0], meses, consolidadas)

    # Painel 2
    _painel_linhas_frentes(axes[1], meses, frentes)

    # Painel 3
    _painel_heatmap(axes[2], meses, coverage_heat)

    plt.tight_layout()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(str(OUTPUT_PATH), dpi=150)
    plt.close()

    print(f"Figura salva: {OUTPUT_PATH}")
    print(f"  Tabelas com data: {len(df_com_data)}")
    print(f"  Meses no eixo: {len(meses)}")
    print(f"  Frentes distintas: {len(frentes)}")
    print(f"  Tabelas no heatmap (Alta/Média): {len(coverage_heat)}")


if __name__ == "__main__":
    main()
