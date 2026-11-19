#!/usr/bin/env python3
"""Batimento estrutural entre dump histórico MCMV e SFTP.

Script CLI que executa o matching em 3 camadas (hash exato → stem canônico
→ similaridade de colunas) e gera CSVs de análise em data/sftp/analise/.

Uso:
    uv run python scripts/sftp/batimento_estrutura.py
    uv run python scripts/sftp/batimento_estrutura.py -v -o /tmp/analise/
    uv run python scripts/sftp/batimento_estrutura.py --schema-dump dados_historicos_formatados
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# Adicionar src/ ao path para importar o pacote sftp
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

import pandas as pd

from sftp.leitura_artefatos import _carregar_todos_artefatos
from sftp.matching import executar_batimento

logger = logging.getLogger("batimento")


def _configurar_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _resumir_por_camada(df_relacionadas: "pd.DataFrame") -> dict[str, int]:
    """Agrupa pares relacionados por método de matching."""
    if df_relacionadas.empty:
        return {}
    return df_relacionadas["metodo"].value_counts().to_dict()


def _resumir_sem_match(
    df_relacionadas: "pd.DataFrame",
    inv_sftp: "pd.DataFrame",
    inv_dh: "pd.DataFrame",
) -> tuple[int, int]:
    """Conta tabelas SFTP e dump sem nenhum par relacionado."""
    from sftp.matching import _build_col_index

    col_sftp = _build_col_index(inv_sftp)
    col_dh = _build_col_index(inv_dh)
    todas_sftp = set(col_sftp.keys())
    todas_dh = set(col_dh.keys())

    if df_relacionadas.empty:
        return len(todas_sftp), len(todas_dh)

    matched_sftp = set(df_relacionadas["tabela_sftp"].unique())
    matched_dh = set(df_relacionadas["tabela_dump"].unique())
    return len(todas_sftp - matched_sftp), len(todas_dh - matched_dh)


def main() -> None:
    parser = argparse.ArgumentParser(description="Batimento estrutural dump × SFTP")
    parser.add_argument(
        "--output-dir",
        "-o",
        default="data/sftp/analise/",
        help="Diretório de saída dos CSVs de análise",
    )
    parser.add_argument(
        "--artefatos-dir",
        "-a",
        default="data/sftp/artefatos/",
        help="Diretório contendo os artefatos SQL exportados",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Habilita logging DEBUG",
    )
    parser.add_argument(
        "--schema-dump",
        choices=["dados_historicos", "dados_historicos_formatados"],
        default="dados_historicos",
        help="Schema alvo do dump para o batimento (default: dados_historicos)",
    )
    args = parser.parse_args()

    _configurar_logging(args.verbose)
    inicio = time.time()

    schema_dump = args.schema_dump
    is_formatados = schema_dump == "dados_historicos_formatados"

    # --- 1. Carregar artefatos ---
    artefatos_dir = Path(args.artefatos_dir)
    logger.info(
        "Carregando artefatos de: %s (schema_dump=%s)", artefatos_dir, schema_dump
    )

    inv_sftp, inv_dh, est_sftp, est_dh, comparacao, _ = _carregar_todos_artefatos(
        artefatos_dir, schema_dump=schema_dump
    )

    # --- 2. Executar batimento estrutural (3 camadas) ---
    logger.info("Executando batimento estrutural...")
    resultado = executar_batimento(
        inv_sftp, inv_dh, est_sftp, est_dh, comparacao, schema_dump=schema_dump
    )

    # --- 3. Criar diretório de saída ---
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Diretório de saída: %s", output_dir)

    # --- 4. Salvar CSVs ---
    # Ajustar nomes dos arquivos conforme schema alvo
    suffix = "_formatados" if is_formatados else ""
    caminhos: dict[str, Path] = {
        "tabelas_relacionadas.csv": output_dir / f"tabelas_relacionadas{suffix}.csv",
        "campos_em_comum.csv": output_dir / f"campos_em_comum{suffix}.csv",
        "chaves_cruzamento_candidatas.csv": output_dir
        / f"chaves_cruzamento_candidatas{suffix}.csv",
        "divergencias_estrutura.csv": output_dir
        / f"divergencias_estrutura{suffix}.csv",
    }

    # tabelas_relacionadas.csv
    cols_rel = [
        "tabela_sftp",
        "tabela_dump",
        "metodo",
        "confianca",
        "score_similaridade",
        "qtd_campos_comum",
    ]
    resultado.relacionadas[cols_rel].to_csv(
        caminhos["tabelas_relacionadas.csv"], index=False
    )
    logger.info(
        "Salvo: %s (%d linhas)",
        caminhos["tabelas_relacionadas.csv"].name,
        len(resultado.relacionadas),
    )

    # campos_em_comum.csv
    cols_campos = [
        "tabela_sftp",
        "tabela_dump",
        "campo",
        "tipo_sftp",
        "tipo_dump",
        "match_tipo",
    ]
    resultado.campos_em_comum[cols_campos].to_csv(
        caminhos["campos_em_comum.csv"], index=False
    )
    logger.info(
        "Salvo: %s (%d linhas)",
        caminhos["campos_em_comum.csv"].name,
        len(resultado.campos_em_comum),
    )

    # chaves_cruzamento_candidatas.csv
    cols_chaves = [
        "chave",
        "padrao",
        "qtd_tabelas_sftp",
        "qtd_tabelas_dump",
        "tabelas_exemplo",
    ]
    resultado.chaves[cols_chaves].to_csv(
        caminhos["chaves_cruzamento_candidatas.csv"], index=False
    )
    logger.info(
        "Salvo: %s (%d linhas)",
        caminhos["chaves_cruzamento_candidatas.csv"].name,
        len(resultado.chaves),
    )

    # divergencias_estrutura.csv
    cols_div = ["categoria", "tabela", "schema", "familia", "qtd_tabelas", "observacao"]
    resultado.divergencias[cols_div].to_csv(
        caminhos["divergencias_estrutura.csv"], index=False
    )
    logger.info(
        "Salvo: %s (%d linhas)",
        caminhos["divergencias_estrutura.csv"].name,
        len(resultado.divergencias),
    )

    # --- 5. Resumo final ---
    duracao = time.time() - inicio
    por_camada = _resumir_por_camada(resultado.relacionadas)
    sem_sftp, sem_dh = _resumir_sem_match(resultado.relacionadas, inv_sftp, inv_dh)

    logger.info("=" * 60)
    logger.info("RESUMO DO BATIMENTO ESTRUTURAL")
    logger.info("  Pares encontrados por camada:")
    for metodo in ["hash_exato", "stem_canonico", "jaccard_colunas"]:
        qtd = por_camada.get(metodo, 0)
        logger.info("    - %s: %d", metodo, qtd)
    logger.info("  Tabelas sem match: %d SFTP, %d dump", sem_sftp, sem_dh)
    logger.info("  Total de pares: %d", len(resultado.relacionadas))
    logger.info("  Campos em comum registrados: %d", len(resultado.campos_em_comum))
    logger.info("  Chaves candidatas identificadas: %d", len(resultado.chaves))
    logger.info("  Divergências registradas: %d", len(resultado.divergencias))
    logger.info("  Tempo total: %.1f segundos", duracao)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
