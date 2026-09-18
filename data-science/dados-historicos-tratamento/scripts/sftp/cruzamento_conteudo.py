#!/usr/bin/env python3
"""Cruzamento de conteúdo entre tabelas SFTP e dados_historicos_formatados (DHF).

Para cada família de tabelas (por stem canônico) que exista em ambos os
diretórios, extrai os valores distintos de APF e calcula o overlap de
conteúdo. Gera um CSV de análise e um resumo no stdout.

Uso:
    uv run python scripts/sftp/cruzamento_conteudo.py
    uv run python scripts/sftp/cruzamento_conteudo.py \\
        --sftp-dir data/sftp/table_samples/ \\
        --dhf-dir data/dados_historicos_formatados/table_samples/ \\
        --output data/sftp/analise/cruzamento_conteudo.csv \\
        --min-overlap 0.5 --min-apfs 5
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# Adicionar src/ ao path para importar o pacote sftp
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pandas as pd

from sftp.normalizacao import canonicalizar_stem

logger = logging.getLogger("cruzamento_conteudo")

# Colunas de saída do CSV
COLUNAS_SAIDA = [
    "familia",
    "tabela_sftp",
    "tabela_dhf",
    "qtd_apfs_sftp",
    "qtd_apfs_dhf",
    "overlap_apfs",
    "overlap_pct",
    "match_estrutural",
    "confianca",
]

# Nomes de coluna que representam APF
COLUNAS_APF = {"apf", "nu_apf"}


def _configurar_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _carregar_tabelas(diretorio: Path) -> dict[str, pd.DataFrame]:
    """Carrega todos os CSVs de um diretório.

    Retorna um dict {nome_arquivo_sem_ext: DataFrame}.
    """
    tabelas: dict[str, pd.DataFrame] = {}
    if not diretorio.exists():
        logger.warning("Diretório não encontrado: %s", diretorio)
        return tabelas

    for caminho in sorted(diretorio.glob("*.csv")):
        nome = caminho.stem
        try:
            df = pd.read_csv(
                caminho,
                sep="\t",
                index_col=0,
                dtype=str,
                on_bad_lines="skip",
            )
            tabelas[nome] = df
            logger.debug(
                "Carregada: %s (%d linhas, %d colunas)", nome, len(df), len(df.columns)
            )
        except Exception as exc:
            logger.warning("Erro ao carregar %s: %s", caminho.name, exc)

    logger.info("Carregadas %d tabelas de %s", len(tabelas), diretorio)
    return tabelas


def _extrair_apfs(df: pd.DataFrame) -> set[str]:
    """Extrai valores distintos de APF do DataFrame.

    Procura por colunas com nome ``apf`` ou ``nu_apf`` (case-insensitive).
    Retorna um conjunto vazio se nenhuma coluna de APF for encontrada.
    """
    col_apf = None
    for col in df.columns:
        if col.strip().lower() in COLUNAS_APF:
            col_apf = col
            break

    if col_apf is None:
        return set()

    valores = df[col_apf].dropna().str.strip()
    # Filtrar valores vazios ou que são apenas cabeçalho repetido
    valores = valores[valores != ""]
    return set(valores.unique())


def _agrupar_por_familia(tabelas: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    """Agrupa nomes de tabela pelo stem canônico (família).

    Retorna {familia: [nome_tabela, ...]}.
    """
    familias: dict[str, list[str]] = {}
    for nome in tabelas:
        familia = canonicalizar_stem(nome)
        familias.setdefault(familia, []).append(nome)
    return familias


def _carregar_match_estrutural(
    caminho: Path,
) -> set[tuple[str, str]]:
    """Carrega pares (tabela_sftp, tabela_dump) do CSV de matching estrutural.

    Retorna um conjunto de tuplas (tabela_sftp, tabela_dump) para consulta O(1).
    Se o arquivo não existir, retorna conjunto vazio.
    """
    if not caminho.exists():
        logger.info("Arquivo de matching estrutural não encontrado: %s", caminho)
        return set()

    try:
        df = pd.read_csv(caminho, dtype=str)
        required = {"tabela_sftp", "tabela_dump"}
        if not required.issubset(df.columns):
            logger.warning(
                "Colunas esperadas %s não encontradas em %s. Ignorando.",
                required,
                caminho,
            )
            return set()
        pares = set(zip(df["tabela_sftp"], df["tabela_dump"], strict=False))
        logger.info("Carregados %d pares de matching estrutural", len(pares))
        return pares
    except Exception as exc:
        logger.warning("Erro ao ler %s: %s", caminho, exc)
        return set()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cruzamento de conteúdo (APF) entre tabelas SFTP e DHF"
    )
    parser.add_argument(
        "--sftp-dir",
        default="data/sftp/table_samples/",
        help="Diretório com amostras CSV do SFTP",
    )
    parser.add_argument(
        "--dhf-dir",
        default="data/dados_historicos_formatados/table_samples/",
        help="Diretório com amostras CSV do DHF",
    )
    parser.add_argument(
        "--output",
        default="data/sftp/analise/cruzamento_conteudo.csv",
        help="Caminho do CSV de saída",
    )
    parser.add_argument(
        "--min-overlap",
        type=float,
        default=0.5,
        help="Overlap mínimo (Jaccard / fração) para considerar match alto",
    )
    parser.add_argument(
        "--min-apfs",
        type=int,
        default=5,
        help="Número mínimo de APFs em comum para considerar match alto",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Habilita logging DEBUG",
    )
    args = parser.parse_args()

    _configurar_logging(args.verbose)
    inicio = time.time()

    min_overlap = args.min_overlap
    min_apfs = args.min_apfs

    # --- 1. Carregar tabelas SFTP e DHF ---
    sftp_dir = Path(args.sftp_dir)
    dhf_dir = Path(args.dhf_dir)

    logger.info("Carregando tabelas SFTP de: %s", sftp_dir)
    tabelas_sftp = _carregar_tabelas(sftp_dir)

    logger.info("Carregando tabelas DHF de: %s", dhf_dir)
    tabelas_dhf = _carregar_tabelas(dhf_dir)

    if not tabelas_sftp:
        logger.error("Nenhuma tabela SFTP carregada. Abortando.")
        sys.exit(1)
    if not tabelas_dhf:
        logger.error("Nenhuma tabela DHF carregada. Abortando.")
        sys.exit(1)

    # --- 2. Agrupar por família ---
    familias_sftp = _agrupar_por_familia(tabelas_sftp)
    familias_dhf = _agrupar_por_familia(tabelas_dhf)

    familias_comuns = sorted(set(familias_sftp) & set(familias_dhf))
    logger.info(
        "Famílias: %d SFTP, %d DHF, %d em comum",
        len(familias_sftp),
        len(familias_dhf),
        len(familias_comuns),
    )

    # --- 3. Carregar matching estrutural (se existir) ---
    caminho_estrutural = Path("data/sftp/analise/tabelas_relacionadas.csv")
    pares_estruturais = _carregar_match_estrutural(caminho_estrutural)

    # --- 4. Cruzar conteúdo por família ---
    registros: list[dict] = []

    for familia in familias_comuns:
        nomes_sftp = familias_sftp[familia]
        nomes_dhf = familias_dhf[familia]

        for nome_sftp in nomes_sftp:
            df_sftp = tabelas_sftp[nome_sftp]
            apfs_sftp = _extrair_apfs(df_sftp)

            for nome_dhf in nomes_dhf:
                df_dhf = tabelas_dhf[nome_dhf]
                apfs_dhf = _extrair_apfs(df_dhf)

                qtd_sftp = len(apfs_sftp)
                qtd_dhf = len(apfs_dhf)

                if qtd_sftp == 0 or qtd_dhf == 0:
                    overlap = 0
                    overlap_pct = 0.0
                else:
                    overlap = len(apfs_sftp & apfs_dhf)
                    overlap_pct = overlap / min(qtd_sftp, qtd_dhf)

                # Determinar confiança
                if overlap >= min_apfs and overlap_pct >= min_overlap:
                    confianca = "alta"
                else:
                    confianca = "baixa"

                # Verificar matching estrutural
                # O arquivo usa 'tabela_dump' como nome da coluna DHF
                match_estrutural = (nome_sftp, nome_dhf) in pares_estruturais

                registros.append(
                    {
                        "familia": familia,
                        "tabela_sftp": nome_sftp,
                        "tabela_dhf": nome_dhf,
                        "qtd_apfs_sftp": qtd_sftp,
                        "qtd_apfs_dhf": qtd_dhf,
                        "overlap_apfs": overlap,
                        "overlap_pct": round(overlap_pct, 4),
                        "match_estrutural": match_estrutural,
                        "confianca": confianca,
                    }
                )

    if not registros:
        logger.warning("Nenhum par de tabelas com APF encontrado nas famílias comuns.")
    else:
        logger.info("Total de pares analisados: %d", len(registros))

    # --- 5. Gerar DataFrame e salvar CSV ---
    df_saida = pd.DataFrame(registros, columns=COLUNAS_SAIDA)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_saida.to_csv(output_path, index=False)
    logger.info("Resultado salvo em: %s (%d linhas)", output_path, len(df_saida))

    # --- 6. Resumo no stdout ---
    duracao = time.time() - inicio
    total_pares = len(df_saida)
    qtd_alta = len(df_saida[df_saida["confianca"] == "alta"]) if total_pares > 0 else 0
    qtd_familias = df_saida["familia"].nunique() if total_pares > 0 else 0

    logger.info("=" * 60)
    logger.info("RESUMO DO CRUZAMENTO DE CONTEÚDO")
    logger.info("  Total de pares: %d", total_pares)
    logger.info("  Pares com confiança alta: %d", qtd_alta)
    logger.info(
        "  Pares com match estrutural: %d",
        df_saida["match_estrutural"].sum() if total_pares > 0 else 0,
    )
    logger.info("  Famílias cobertas: %d", qtd_familias)
    logger.info("  Tempo total: %.1f segundos", duracao)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
