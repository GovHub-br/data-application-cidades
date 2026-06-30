"""Entry point: classifica e trata todas as amostras/tabelas.

Modos de execução:
1. **CSV (padrão):** Classifica e trata amostras de ``data/table_samples/``.
2. **DB (``--db``):** Conecta ao PostgreSQL, classifica tabelas completas do
   schema ``dados_historicos`` e grava o resultado em ``dados_historicos_formatados``.

Pipeline (comum aos dois modos):
1. Classificação → comparação com referência → relatório.
2. Deduplicação por hash de conteúdo.
3. Tratamento de tabelas canônicas → validação.
4. Relatório de qualidade.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from classificacao.classificador import (
    classificar_todas,
    comparar_referencia,
    gerar_relatorio_divergencias,
    sumarizar,
)
from classificacao.saida import escrever_classificacao
from classificacao.carregamento import carregar_amostra
from classificacao.deduplicacao import (
    agrupar_duplicatas,
    eleger_canonicas,
    gerar_mapping,
)
from classificacao.tratamento import tratar_tabela
from classificacao.saida_tratamento import (
    escrever_saida,
    gerar_relatorio_qualidade as gerar_relatorio_qualidade_tratamento,
)

OUTPUT_PATH = "data/classificacao_formacao.csv"
RELATORIO_PATH = "data/relatorio_divergencias.md"
TREATED_DIR = Path("data/treated_tables")
DEDUP_MAP_PATH = TREATED_DIR / "_dedup_map.csv"
QUALITY_REPORT_PATH = TREATED_DIR / "_quality_report.csv"
TABLE_SAMPLES_DIR = Path("data/table_samples")


# ═══════════════════════════════════════════════════════════════════════════
# Helpers de orquestração (extraídos para reutilização)
# ═══════════════════════════════════════════════════════════════════════════


def _report_classificacao(resultados: pd.DataFrame) -> None:
    """Exibe sumário de classificação (comum CSV/DB)."""
    print("\nSumário por categoria:")
    for categoria, n in sumarizar(resultados).items():
        print(f"  {categoria:35s} {n}")

    n_baixa = int((resultados["confidence"] == "low").sum())
    print(f"\nTotal: {len(resultados)} tabelas | confidence=low: {n_baixa}")


def _processar_tratamento(
    df_tratar: pd.DataFrame,
    classificacao_map: dict[str, str],
    carregar_fn: Any,
    escrever_fn: Any,
) -> list[dict]:
    """Executa o loop de tratamento genérico (CSV ou DB).

    Args:
        df_tratar: DataFrame com ``source_table``, ``content_hash``,
            ``is_duplicate`` (apenas canônicas).
        classificacao_map: ``{table_name: formacao}``.
        carregar_fn: ``(table_name) -> pd.DataFrame`` — carrega a tabela bruta.
        escrever_fn: ``(df, table_name) -> None`` — persiste a saída.

    Returns:
        Lista de info_dicts com métricas de cada tabela processada.
    """
    from classificacao.validacao import validar_tabela

    resultados_tratamento: list[dict] = []
    n_treated = 0
    n_discarded = 0
    n_errors = 0

    for _, row in df_tratar.iterrows():
        table_name = str(row["source_table"])
        content_hash = str(row["content_hash"])
        formacao = classificacao_map.get(table_name, "indeterminada")

        try:
            df_raw = carregar_fn(table_name)
            df_t, info = tratar_tabela(
                table_name=table_name,
                formacao=formacao,
                df=df_raw,
                content_hash=content_hash,
                data_dir=Path("data"),
            )

            if df_t is not None and len(df_t) > 0:
                escrever_fn(df_t, table_name)
                n_treated += 1
            else:
                n_discarded += 1

            if df_t is not None:
                val = validar_tabela(df_t)
                info["missing_pct"] = val["missing_pct"]
                info["n_rows"] = val["n_rows"]
                info["n_cols"] = val["n_cols"]

            info["table_name"] = table_name
            resultados_tratamento.append(info)

        except Exception as exc:  # noqa: BLE001
            n_errors += 1
            error_info: dict[str, Any] = {
                "table_name": table_name,
                "status": "error",
                "profile": formacao,
                "n_rows": 0,
                "n_cols": 0,
                "missing_pct": 0.0,
                "error": str(exc),
            }
            resultados_tratamento.append(error_info)
            print(f"  ERRO: {table_name}: {exc}")

    print(f"\n  Tratadas: {n_treated}")
    print(f"  Descartadas: {n_discarded}")
    print(f"  Erros: {n_errors}")

    return resultados_tratamento


def _build_classificacao_map(resultados: pd.DataFrame) -> dict[str, str]:
    """Constrói ``{table_name: formacao}`` a partir dos resultados de classificação."""
    return dict(
        zip(resultados["table_name"].astype(str), resultados["formacao"].astype(str))
    )


def _executar_classificacao_csv() -> pd.DataFrame:
    """Executa a etapa de classificação no modo CSV.

    Classifica amostras, compara com referência, gera relatório de
    divergências e salva ``data/classificacao_formacao.csv``.

    Returns:
        DataFrame com colunas ``table_name, formacao, confidence, notes``.
    """
    print("Classificando amostras...")
    resultados = classificar_todas()

    print("Comparando com a referência manual...")
    resultados = comparar_referencia(resultados)

    path = escrever_classificacao(resultados, OUTPUT_PATH)
    print(f"Tabela de classificação escrita: {path}")

    relatorio = gerar_relatorio_divergencias(resultados)
    Path(RELATORIO_PATH).write_text(relatorio, encoding="utf-8")
    print(f"Relatório de divergências escrito: {RELATORIO_PATH}")

    _report_classificacao(resultados)

    return resultados


def _carregar_classificacao_csv(path: str) -> pd.DataFrame:
    """Carrega classificação de arquivo CSV e valida colunas obrigatórias.

    Args:
        path: Caminho para o arquivo CSV de classificação.

    Returns:
        DataFrame com colunas ``table_name, formacao, confidence, notes``.

    Raises:
        SystemExit(1): Se o arquivo não existir ou colunas obrigatórias
            estiverem ausentes.
    """
    p = Path(path)
    if not p.exists():
        print(
            f"Erro: Arquivo de classificação não encontrado: {p}",
            file=sys.stderr,
        )
        print("Execute sem --skip-classify para gerar.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(p)
    colunas_obrigatorias = {"table_name", "formacao", "confidence", "notes"}
    faltando = colunas_obrigatorias - set(df.columns)
    if faltando:
        print(
            f"Erro: Colunas obrigatórias ausentes no arquivo "
            f"de classificação: {faltando}",
            file=sys.stderr,
        )
        sys.exit(1)

    return df.astype(str)


# ═══════════════════════════════════════════════════════════════════════════
# Fluxo CSV (existente, mantido inalterado)
# ═══════════════════════════════════════════════════════════════════════════


def main(
    skip_classify: bool = False,
    classify_only: bool = False,
    run_inventario: bool = False,
) -> None:
    """Pipeline completo — modo CSV (amostras de ``data/table_samples/``).

    Args:
        skip_classify: Se True, carrega classificação de
            ``data/classificacao_formacao.csv`` em vez de reclassificar.
        classify_only: Se True, executa apenas a classificação.
        run_inventario: Se True, executa inventário após tratamento
            (ignorado com ``--classify-only``).
    """
    # ── Stage 1: Classificação ──────────────────────────────────────
    if classify_only:
        _executar_classificacao_csv()
        return

    if skip_classify:
        print("Carregando classificação existente...")
        resultados = _carregar_classificacao_csv(OUTPUT_PATH)
        classificacao_map = _build_classificacao_map(resultados)
    else:
        resultados = _executar_classificacao_csv()
        classificacao_map = _build_classificacao_map(resultados)

    # ── Stage 3: Tratamento ─────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Stage 3: Tratamento de dados")
    print("=" * 60)

    # Passo 0: Deduplicação
    print("\n[Passo 0] Deduplicação por hash MD5...")
    df_hashes = agrupar_duplicatas(TABLE_SAMPLES_DIR)
    df_canonicas = eleger_canonicas(df_hashes)
    gerar_mapping(df_canonicas, DEDUP_MAP_PATH)
    n_total = len(df_canonicas)
    n_canonicas = int((~df_canonicas["is_duplicate"]).sum())
    n_duplicadas = int(df_canonicas["is_duplicate"].sum())
    print(f"  {n_total} tabelas → {n_canonicas} canônicas + {n_duplicadas} duplicadas")
    print(f"  Mapping escrito: {DEDUP_MAP_PATH}")

    # Passo 1: Tratamento de tabelas canônicas
    print(f"\n[Passo 1] Tratando {n_canonicas} tabelas canônicas...")

    df_tratar = df_canonicas[~df_canonicas["is_duplicate"]].copy()

    def _carregar_csv(table_name: str) -> pd.DataFrame:
        return carregar_amostra(table_name)

    def _escrever_csv(df: pd.DataFrame, table_name: str) -> None:
        escrever_saida(df, table_name, TREATED_DIR)

    resultados_tratamento = _processar_tratamento(
        df_tratar, classificacao_map, _carregar_csv, _escrever_csv
    )

    # Gera relatório de qualidade
    print("\n[Passo 2] Gerando relatório de qualidade...")
    df_qualidade = gerar_relatorio_qualidade_tratamento(
        resultados_tratamento, QUALITY_REPORT_PATH
    )
    print(f"  Relatório escrito: {QUALITY_REPORT_PATH}")
    print(f"  Total de registros: {len(df_qualidade)}")

    print("\n" + "=" * 60)
    print("Pipeline completo!")
    print(f"  Classificação: {OUTPUT_PATH}")
    print(f"  Dedup mapping: {DEDUP_MAP_PATH}")
    print(f"  Tabelas tratadas: {TREATED_DIR}/")
    print(f"  Relatório de qualidade: {QUALITY_REPORT_PATH}")
    print("=" * 60)

    # ── Stage 4: Inventário ─────────────────────────────────────────
    if run_inventario:
        _executar_inventario_csv()


def _executar_inventario_csv() -> None:
    """Stage 4: Inventário de dados (modo CSV)."""
    print("\n" + "=" * 60)
    print("Stage 4: Inventário de dados")
    print("=" * 60)

    from classificacao.inventario import (
        gerar_inventario,
        gerar_relatorio_qualidade as gerar_relatorio_qualidade_dados,
    )

    INVENTARIO_PATH = "data/inventario_dados.csv"
    RELATORIO_QUALIDADE_PATH = "data/relatorio_qualidade_dados.md"

    print("\nGerando inventário de dados...")
    df_inventario = gerar_inventario(
        quality_path=str(QUALITY_REPORT_PATH),
        dedup_path=str(DEDUP_MAP_PATH),
        classificacao_path=OUTPUT_PATH,
        treated_dir=str(TREATED_DIR),
        output_path=INVENTARIO_PATH,
    )
    print(f"  Inventário escrito: {INVENTARIO_PATH}")
    print(f"  Total de tabelas: {len(df_inventario)}")

    print("\nGerando relatório de qualidade...")
    gerar_relatorio_qualidade_dados(
        inventario_path=INVENTARIO_PATH,
        quality_path=str(QUALITY_REPORT_PATH),
        dedup_path=str(DEDUP_MAP_PATH),
        output_path=RELATORIO_QUALIDADE_PATH,
    )
    print(f"  Relatório escrito: {RELATORIO_QUALIDADE_PATH}")

    import subprocess

    print("\nGerando linha do tempo...")
    subprocess.run(
        [sys.executable, "scripts/gerar_linha_do_tempo.py"],
        check=False,
    )
    print("  Linha do tempo: data/linha_do_tempo.png")

    print("\n" + "=" * 60)
    print("Pipeline com inventário completo!")
    print(f"  Inventário: {INVENTARIO_PATH}")
    print(f"  Relatório de qualidade: {RELATORIO_QUALIDADE_PATH}")
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
# Fluxo DB (novo, ativado via --db)
# ═══════════════════════════════════════════════════════════════════════════


def _executar_classificacao_db(engine: Any) -> pd.DataFrame:
    """Executa a etapa de classificação no modo DB.

    Classifica tabelas via PostgreSQL, compara com referência, gera
    relatório de divergências e persiste resultados em tabela PG e CSV
    local.

    Args:
        engine: SQLAlchemy Engine conectado ao banco.

    Returns:
        DataFrame com colunas ``table_name, formacao, confidence, notes``.
    """
    from classificacao.classificador import (
        classificar_todas_db,
        gerar_relatorio_divergencias_db,
    )

    print("Classificando tabelas via banco de dados...")
    resultados = classificar_todas_db(engine)

    print("Comparando com a referência manual...")
    resultados = comparar_referencia(resultados)

    _report_classificacao(resultados)

    gerar_relatorio_divergencias_db(resultados)
    print("Relatório de divergências (DB) escrito: data/relatorio_divergencias_db.md")

    # Salva CSV local para fallback do --skip-classify
    path_csv = escrever_classificacao(resultados, "data/classificacao_formacao_db.csv")
    print(f"Classificação DB salva em CSV local: {path_csv}")

    return resultados


def _carregar_classificacao_db(engine: Any) -> pd.DataFrame:
    """Carrega classificação no modo DB com fallback PG → CSV → erro.

    Ordem de tentativa:
    1. Tabela ``_classificacao`` no schema target do PostgreSQL.
    2. Arquivo ``data/classificacao_formacao_db.csv`` no sistema local.
    3. Se nenhum disponível, ``sys.exit(1)``.

    Args:
        engine: SQLAlchemy Engine conectado ao banco.

    Returns:
        DataFrame com colunas ``table_name, formacao, confidence, notes``.
    """
    from classificacao.db.reader import (
        ler_classificacao_db,
        ClassificationNotFoundError,
    )

    # Tenta PostgreSQL
    try:
        print(
            "Tentando carregar classificação da tabela _classificacao no PostgreSQL..."
        )
        df = ler_classificacao_db(engine)
        print("Classificação carregada do PostgreSQL.")
        return df
    except ClassificationNotFoundError:
        print(
            "Tabela _classificacao não encontrada no PostgreSQL. Tentando CSV local..."
        )

    # Tenta CSV local
    csv_path = Path("data/classificacao_formacao_db.csv")
    if csv_path.exists():
        print(f"Carregando classificação do CSV local: {csv_path}")
        df = pd.read_csv(csv_path)
        colunas_obrigatorias = {"table_name", "formacao", "confidence", "notes"}
        faltando = colunas_obrigatorias - set(df.columns)
        if faltando:
            print(
                f"Erro: Colunas obrigatórias ausentes no CSV: {faltando}",
                file=sys.stderr,
            )
            sys.exit(1)
        return df.astype(str)

    # Ambos falharam
    print(
        "Erro: Nenhuma fonte de classificação encontrada.",
        file=sys.stderr,
    )
    print("Execute sem --skip-classify para gerar.", file=sys.stderr)
    sys.exit(1)


def main_db(
    skip_classify: bool = False,
    classify_only: bool = False,
) -> None:
    """Pipeline completo — modo DB (PostgreSQL).

    Lê tabelas completas do schema ``dados_historicos`` e grava o
    resultado no schema ``dados_historicos_formatados``.

    Args:
        skip_classify: Se True, carrega classificação do PostgreSQL/CSV
            em vez de reclassificar.
        classify_only: Se True, executa apenas a classificação.
    """
    from classificacao.db.connection import get_engine
    from classificacao.db.reader import ler_tabela
    from classificacao.db.writer import (
        criar_schema_target,
        escrever_classificacao as escrever_classificacao_db,
        escrever_qualidade as escrever_qualidade_db,
        escrever_tabela,
    )
    from classificacao.deduplicacao import (
        agrupar_duplicatas_db,
        gerar_mapping_db,
    )

    engine = get_engine()

    # ── Stage 1: Classificação ──────────────────────────────────────
    if classify_only:
        resultados = _executar_classificacao_db(engine)

        # Persiste no PostgreSQL
        print("\nRecriando schema target...")
        criar_schema_target(engine, confirm=True)
        escrever_classificacao_db(resultados, engine)
        print("Tabela de classificação escrita no schema target.")
        return

    if skip_classify:
        print("Carregando classificação existente...")
        resultados = _carregar_classificacao_db(engine)
        classificacao_map = _build_classificacao_map(resultados)
    else:
        resultados = _executar_classificacao_db(engine)
        classificacao_map = _build_classificacao_map(resultados)

    # ── Stage 2: Preparar schema target ─────────────────────────────
    print("\nRecriando schema target...")
    criar_schema_target(engine, confirm=True)

    escrever_classificacao_db(resultados, engine)
    print("Tabela de classificação escrita no schema target.")

    # ── Stage 3: Deduplicação ───────────────────────────────────────
    print("\n" + "=" * 60)
    print("Stage 3: Tratamento de dados (DB)")
    print("=" * 60)

    print("\n[Passo 0] Deduplicação por hash de conteúdo...")
    df_hashes = agrupar_duplicatas_db(engine)
    # Renomeia row_count → file_size (esperado por eleger_canonicas)
    df_hashes = df_hashes.rename(columns={"row_count": "file_size"})
    df_canonicas = eleger_canonicas(df_hashes)
    gerar_mapping_db(df_canonicas, engine)
    n_total = len(df_canonicas)
    n_canonicas = int((~df_canonicas["is_duplicate"]).sum())
    n_duplicadas = int(df_canonicas["is_duplicate"].sum())
    print(f"  {n_total} tabelas → {n_canonicas} canônicas + {n_duplicadas} duplicadas")

    # ── Stage 4: Tratamento ─────────────────────────────────────────
    print(f"\n[Passo 1] Tratando {n_canonicas} tabelas canônicas...")

    df_tratar = df_canonicas[~df_canonicas["is_duplicate"]].copy()

    def _carregar_db(table_name: str) -> pd.DataFrame:
        return ler_tabela(engine, table_name)

    def _escrever_db(df: pd.DataFrame, table_name: str) -> None:
        escrever_tabela(df, table_name, engine)

    resultados_tratamento = _processar_tratamento(
        df_tratar, classificacao_map, _carregar_db, _escrever_db
    )

    # ── Stage 5: Relatório de qualidade ─────────────────────────────
    print("\n[Passo 2] Gerando relatório de qualidade...")
    df_qualidade = gerar_relatorio_qualidade_tratamento(
        resultados_tratamento, QUALITY_REPORT_PATH
    )
    escrever_qualidade_db(df_qualidade, engine)
    print("  Relatório de qualidade escrito no schema target.")
    print(f"  Total de registros: {len(df_qualidade)}")

    print("\n" + "=" * 60)
    print("Pipeline DB completo!")
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline MCMV: classificação, tratamento e inventário"
    )
    parser.add_argument(
        "--inventario",
        action="store_true",
        help="Executa etapa de inventário após tratamento (modo CSV)",
    )
    parser.add_argument(
        "--db",
        action="store_true",
        help="Executa pipeline no modo DB (PostgreSQL) em vez do modo CSV",
    )
    parser.add_argument(
        "--skip-classify",
        action="store_true",
        help="Pula a etapa de classificação e carrega resultados de fonte existente",
    )
    parser.add_argument(
        "--classify-only",
        action="store_true",
        help="Executa apenas a etapa de classificação, sem dedup nem tratamento",
    )
    args = parser.parse_args()

    if args.skip_classify and args.classify_only:
        print(
            "Erro: --skip-classify e --classify-only são mutuamente exclusivos.",
            file=sys.stderr,
        )
        sys.exit(2)

    if args.classify_only and args.inventario:
        print(
            "Aviso: --inventario não é executado com --classify-only.",
            file=sys.stderr,
        )

    if args.db:
        main_db(skip_classify=args.skip_classify, classify_only=args.classify_only)
    else:
        main(
            skip_classify=args.skip_classify,
            classify_only=args.classify_only,
            run_inventario=args.inventario,
        )
