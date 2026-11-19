#!/usr/bin/env python3
"""Classifica a formação estrutural de todas as tabelas do schema sftp.

Aplica a taxonomia R1–R8 (de ``src/classificacao/regras.py``) a cada tabela
do schema ``sftp`` no PostgreSQL, complementando com heurísticas específicas
de padrões SFTP/BB/GEFUS:

* Coluna 0 com nome snake_case longo (>30 chars) — pipe-concatenated header.
* Colunas ``dt_ingest`` / ``arquivo_origem`` — metadados de ingestão GEFUS.
* Híbrido tab+pipe — col0 pipe-like + ingestion cols nas últimas posições.
* Colunas nomeadas como datas (YYYYMMDD, YYYYMMDD_HHMMSS) — headerless.
* Estrutura multi-bloco — sequências repetidas de linhas vazias.

Uso:
    uv run python scripts/sftp/classificar_formacao_sftp.py
"""

from __future__ import annotations

import logging
import re
import sys
import time
from pathlib import Path

# Adiciona src/ ao path para importar pacotes do projeto
_SRC = str(Path(__file__).resolve().parent.parent.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pandas as pd
from sqlalchemy import Engine, text

from classificacao.db.connection import get_engine
from classificacao.db.reader import listar_tabelas
from classificacao.profiling import profile_estrutural
from classificacao.regras import (
    classificar_formacao,
    SEPARADOR_PIPE,
    SEM_CABECALHO,
)

logger = logging.getLogger("classificar_formacao_sftp")

# ── Constants ──────────────────────────────────────────────────────────────────

SFTP_SCHEMA = "sftp"
SAMPLE_LIMIT = 200
OUTPUT_CSV = Path("data/sftp/analise/classificacao_formacao_sftp.csv")

OUTPUT_COLUMNS = [
    "table_name",
    "formacao",
    "confidence",
    "notes",
    "n_rows",
    "n_cols",
    "n_data_cols",
    "pipe_detected",
    "has_ingestion_cols",
    "unique_column_patterns",
]

# Colunas de metadados de ingestão GEFUS
_INGESTION_COLS = {"dt_ingest", "arquivo_origem"}

# Padrão de nomes de coluna no formato data (headerless)
_DATE_COL_RE = re.compile(r"^\d{8}$|^\d{8}_\d{6}$")


# ── DB helpers ─────────────────────────────────────────────────────────────────


def _ler_tabela_com_limite(
    engine: Engine,
    table_name: str,
    schema: str,
    limit: int = SAMPLE_LIMIT,
) -> pd.DataFrame:
    """Lê até *limit* linhas de uma tabela como DataFrame de strings.

    Faz o quoting do nome da tabela para suportar caracteres especiais.
    Remove a coluna ``unnamed_0`` se ela parecer um índice sequencial
    (compatibilidade com o comportamento de ``ler_tabela`` em reader.py).
    """
    query = text(f'SELECT * FROM "{schema}"."{table_name}" LIMIT :lim')
    df = pd.read_sql_query(query, engine, params={"lim": limit})
    df = df.astype(str)

    # Drop unnamed_0 se parecer índice sequencial
    if "unnamed_0" in df.columns and len(df) >= 3:
        if df["unnamed_0"].iloc[:3].tolist() == ["0", "1", "2"]:
            del df["unnamed_0"]

    return df


# ── SFTP-specific pattern detectors ────────────────────────────────────────────


def _pipe_coluna_0(df: pd.DataFrame) -> bool:
    """Detecta col0 com nome snake_case longo (>30 chars) ou com ≥4 underscores.

    Em tabelas pipe-delimited de coluna única do SFTP/BB, o nome da única
    coluna costuma ser uma concatenação de identificadores separados por
    ``_``, frequentemente >30 caracteres.
    """
    if df.empty or len(df.columns) == 0:
        return False
    first = str(df.columns[0])
    # Nome muito longo com underscores
    if len(first) >= 30 and "_" in first:
        return True
    # Múltiplos grupos de underscore (pelo menos 4)
    if first.count("_") >= 4 and len(first) >= 20:
        return True
    return False


def _has_ingestion_cols(df: pd.DataFrame) -> bool:
    """True se a tabela tem ambas as colunas ``dt_ingest`` e ``arquivo_origem``."""
    lower = {c.lower() for c in df.columns}
    return "dt_ingest" in lower and "arquivo_origem" in lower


def _hibrido_tab_pipe(df: pd.DataFrame) -> bool:
    """Híbrido tab+pipe: col0 pipe-like + dt_ingest/arquivo_origem no final.

    Este é o formato típico das tabelas GEFUS onde o dado real está na
    primeira coluna (pipe-delimited) e as duas últimas colunas são
    metadados de ingestão.
    """
    if len(df.columns) < 3:
        return False
    cols = list(df.columns)
    last_two = {str(c).lower() for c in cols[-2:]}
    if "dt_ingest" in last_two and "arquivo_origem" in last_two:
        return _pipe_coluna_0(df)
    return False


def _nomes_data_headerless(df: pd.DataFrame) -> bool:
    """True se colunas seguem padrão de data (YYYYMMDD ou YYYYMMDD_HHMMSS).

    Sugere uma tabela headerless onde os nomes de coluna são, na verdade,
    timestamps de dados.
    """
    if df.empty or len(df.columns) == 0:
        return False
    n_date = sum(1 for c in df.columns if bool(_DATE_COL_RE.match(str(c).strip())))
    return n_date >= 2 or (n_date / len(df.columns) >= 0.3)


def _multi_bloco(df: pd.DataFrame) -> tuple[bool, int]:
    """Detecta estrutura multi-bloco contando sequências de linhas vazias.

    Returns
    -------
    tuple[bool, int]
        ``(is_multi_block, n_blank_sequences)``
    """
    if df.empty:
        return False, 0

    empty = df.isna().all(axis=1)

    # Ignora linhas vazias iniciais
    if not empty.any():
        return False, 0
    first_data = empty.argmin() if empty.any() else 0
    tail = empty.iloc[first_data:]
    if len(tail) == 0:
        return False, 0

    in_blank = False
    seqs = 0
    for v in tail:
        if v:
            if not in_blank:
                seqs += 1
                in_blank = True
        else:
            in_blank = False
    return seqs >= 2, seqs


def _padroes_colunas(df: pd.DataFrame) -> str:
    """Gera um descritor curto dos padrões de nome de coluna."""
    if df.empty:
        return "empty"

    cols = [str(c).strip() for c in df.columns]
    n = len(cols)
    if n == 0:
        return "no_columns"

    parts: list[str] = []
    unnamed = sum(1 for c in cols if c.lower().startswith("unnamed"))
    date_names = sum(1 for c in cols if bool(_DATE_COL_RE.match(c)))
    long_names = sum(1 for c in cols if len(c) >= 30)
    ingest = sum(1 for c in cols if c.lower() in _INGESTION_COLS)

    if unnamed > n / 2:
        parts.append(f"unnamed:{unnamed}/{n}")
    if date_names:
        parts.append(f"date_names:{date_names}")
    if long_names:
        parts.append(f"long_names:{long_names}")
    if ingest:
        parts.append(f"ingestion:{ingest}")
    if not parts:
        parts.append(f"mixed:{n}")

    return "; ".join(parts)


# ── Classification orchestrator ────────────────────────────────────────────────


def _classificar_tabela(
    table_name: str, df: pd.DataFrame
) -> tuple[str, str, str, bool, bool, str]:
    """Aplica a taxonomia R1–R8 + heurísticas SFTP a uma tabela.

    Returns
    -------
    tuple[str, str, str, bool, bool, str]
        ``(formacao, confidence, notes, pipe_detected, has_ingestion, column_patterns)``
    """
    # --- SFTP-specific pre-flags ---
    pipe_col0 = _pipe_coluna_0(df)
    has_ingestion = _has_ingestion_cols(df)
    is_hybrid = _hibrido_tab_pipe(df)
    is_headerless_dates = _nomes_data_headerless(df)
    is_multi_block, blank_seqs = _multi_bloco(df)
    col_patterns = _padroes_colunas(df)

    # ── Casos específicos SFTP que a árvore R1–R8 padrão não captura ──

    # 1. Híbrido tab+pipe: col0 pipe-like + dt_ingest/arquivo_origem
    if is_hybrid:
        return (
            SEPARADOR_PIPE,
            "high",
            "SFTP: col0 pipe-like + dt_ingest/arquivo_origem (híbrido tab+pipe)",
            pipe_col0,
            has_ingestion,
            col_patterns,
        )

    # 2. Coluna 0 pipe-like isolada (sem ingestion cols)
    if pipe_col0 and not has_ingestion and len(df.columns) == 1:
        return (
            SEPARADOR_PIPE,
            "high",
            "SFTP: coluna única com nome pipe-like (pipe-delimited)",
            pipe_col0,
            has_ingestion,
            col_patterns,
        )

    # 3. Colunas nomeadas como datas → headerless (quando não há ingestion cols)
    if is_headerless_dates and not has_ingestion:
        return (
            SEM_CABECALHO,
            "high",
            "SFTP: colunas nomeadas como datas (headerless)",
            pipe_col0,
            has_ingestion,
            col_patterns,
        )

    # ── Árvore de decisão padrão R1–R8 ──
    profile = profile_estrutural(df, file_size=len(df))
    formacao, confidence, notes = classificar_formacao(table_name, df, profile)

    # ── Anotações SFTP adicionais ──
    extras: list[str] = []
    if pipe_col0:
        extras.append("col0_pipe_like")
    if has_ingestion:
        extras.append("ingestion_cols_present")
    if is_multi_block:
        extras.append(f"multi_bloco:{blank_seqs}_seqs")

    if extras:
        suffix = "SFTP: " + ", ".join(extras)
        notes = f"{notes}; {suffix}" if notes else suffix

    return formacao, confidence, notes, pipe_col0, has_ingestion, col_patterns


# ── Main ───────────────────────────────────────────────────────────────────────


def _configurar_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main() -> None:
    _configurar_logging()
    inicio = time.time()

    # 1. Engine
    logger.info("Conectando ao banco de dados...")
    engine = get_engine()

    # 2. Listar tabelas
    logger.info("Listando tabelas no schema '%s'...", SFTP_SCHEMA)
    tabelas = listar_tabelas(engine, schema=SFTP_SCHEMA)
    logger.info("Encontradas %d tabelas.", len(tabelas))

    if not tabelas:
        logger.warning("Nenhuma tabela encontrada. Saindo.")
        return

    # 3. Classificar cada tabela
    registros: list[dict] = []
    erros: list[str] = []

    for i, table_name in enumerate(tabelas, 1):
        logger.info("[%d/%d] %s", i, len(tabelas), table_name)
        try:
            df = _ler_tabela_com_limite(engine, table_name, SFTP_SCHEMA)
            formacao, confidence, notes, pipe_detected, has_ingestion, col_patterns = (
                _classificar_tabela(table_name, df)
            )

            registros.append(
                {
                    "table_name": table_name,
                    "formacao": formacao,
                    "confidence": confidence,
                    "notes": notes,
                    "n_rows": int(len(df)),
                    "n_cols": int(len(df.columns)),
                    "n_data_cols": int(len(df.columns)),
                    "pipe_detected": pipe_detected,
                    "has_ingestion_cols": has_ingestion,
                    "unique_column_patterns": col_patterns,
                }
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Erro ao classificar '%s': %s", table_name, exc)
            erros.append(table_name)
            registros.append(
                {
                    "table_name": table_name,
                    "formacao": "indeterminada",
                    "confidence": "low",
                    "notes": f"erro na leitura/classificação: {exc}",
                    "n_rows": 0,
                    "n_cols": 0,
                    "n_data_cols": 0,
                    "pipe_detected": False,
                    "has_ingestion_cols": False,
                    "unique_column_patterns": "erro_leitura",
                }
            )

    # 4. Salvar CSV
    df_resultado = pd.DataFrame(registros, columns=OUTPUT_COLUMNS)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df_resultado.to_csv(OUTPUT_CSV, index=False)
    logger.info("Resultados salvos em: %s (%d tabelas)", OUTPUT_CSV, len(df_resultado))

    # 5. Sumário
    duracao = time.time() - inicio
    _imprimir_sumario(df_resultado, erros, duracao)


def _imprimir_sumario(
    df_resultado: pd.DataFrame,
    erros: list[str],
    duracao: float,
) -> None:
    """Imprime sumário com distribuição por categoria e flags de revisão."""
    n_total = len(df_resultado)

    print()
    print("=" * 72)
    print("  RESUMO — CLASSIFICAÇÃO DE FORMAÇÃO SFTP")
    print("=" * 72)
    print(f"  Total de tabelas classificadas:  {n_total}")
    print(f"  Erros de leitura:                {len(erros)}")
    print(f"  Tempo total:                     {duracao:.1f}s")
    print()

    # Distribuição por categoria
    if not df_resultado.empty:
        print("  Distribuição por categoria de formação:")
        cat_counts = df_resultado["formacao"].value_counts().sort_index()
        max_cat = max(len(c) for c in cat_counts.index)
        for cat, cnt in cat_counts.items():
            pct = cnt / n_total * 100
            print(f"    {cat:<{max_cat + 2}} {cnt:>4}  ({pct:5.1f}%)")
        print()

    # Confiança
    print("  Nível de confiança:")
    for conf in ["high", "medium", "low"]:
        cnt = int((df_resultado["confidence"] == conf).sum())
        pct = cnt / n_total * 100
        print(f"    {conf:<8} {cnt:>4}  ({pct:5.1f}%)")
    print()

    # Flags SFTP
    n_pipe = int(df_resultado["pipe_detected"].sum())
    n_ingest = int(df_resultado["has_ingestion_cols"].sum())
    print(f"  Tabelas com col0 pipe-like:      {n_pipe}")
    print(f"  Tabelas com cols de ingestão:    {n_ingest}")
    print()

    # Tabelas para revisão manual (confidence=low)
    baixas = df_resultado[df_resultado["confidence"] == "low"]
    if not baixas.empty:
        n_review = len(baixas)
        print(f"  Tabelas sinalizadas para revisão manual ({n_review}):")
        for _, row in baixas.iterrows():
            nome = row["table_name"]
            form = row["formacao"]
            nota = row["notes"]
            print(f"    - {nome:<50} {form:<25} {nota}")
        print()

    # Erros
    if erros:
        print(f"  Tabelas com erro de leitura ({len(erros)}):")
        for nome in erros:
            print(f"    - {nome}")
        print()

    print("=" * 72)


if __name__ == "__main__":
    main()
