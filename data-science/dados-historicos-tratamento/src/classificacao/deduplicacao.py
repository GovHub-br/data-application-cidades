"""Deduplicação de amostras CSV por hash MD5 do conteúdo binário.

O módulo implementa o pipeline de deduplicação do tratamento de dados:

1. ``calcular_hash`` — calcula MD5 do conteúdo binário de um arquivo CSV.
2. ``agrupar_duplicatas`` — varre o diretório de amostras e retorna DataFrame
   com hashes e tamanhos de arquivo.
3. ``eleger_canonicas`` — para cada grupo de hash, elege a primeira tabela
   em ordem alfabética como canônica; as demais são marcadas como duplicatas.
4. ``gerar_mapping`` — escreve o arquivo ``_dedup_map.csv`` com o mapeamento
   de source_table → canonical_table.

Regras de negócio:
- Agrupamento por MD5 do conteúdo binário (byte-a-byte).
- Em caso de colisão de hash com tamanhos diferentes, emite warning e trata
  como grupos separados (file_size como fator secundário).
- Apenas tabelas canônicas (``is_duplicate=False``) seguem para downstream.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


def calcular_hash(arquivo: Path) -> str:
    """Calcula MD5 do conteúdo binário do arquivo CSV.

    Lê o arquivo em modo binário e retorna o hash MD5 como string hex.

    Args:
        arquivo: Caminho para o arquivo CSV.

    Returns:
        Hash MD5 em hexadecimal (string de 32 caracteres).

    Raises:
        FileNotFoundError: Se o arquivo não existir.
        OSError: Se houver erro de leitura.
    """
    hasher = hashlib.md5()
    with open(arquivo, "rb") as f:
        # Leitura em blocos de 64KB para arquivos grandes
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def agrupar_duplicatas(table_samples_dir: Path) -> pd.DataFrame:
    """Varre o diretório de amostras e retorna DataFrame com hashes.

    Para cada arquivo ``.csv`` no diretório, calcula o hash MD5 do conteúdo
    binário e registra o tamanho do arquivo. Arquivos que geram erro de
    leitura são pulados com warning.

    Args:
        table_samples_dir: Diretório contendo os arquivos CSV de amostra.

    Returns:
        DataFrame com colunas:
        - ``source_table``: nome do arquivo sem extensão (``Path.stem``).
        - ``content_hash``: MD5 do conteúdo binário.
        - ``file_size``: tamanho do arquivo em bytes.
        Ordenado por ``source_table`` (ordem alfabética, reproduzível).
        Retorna DataFrame vazio se o diretório não existir ou estiver vazio.
    """
    data_dir = Path(table_samples_dir)
    if not data_dir.is_dir():
        logger.warning("Diretório não encontrado: %s", data_dir)
        return pd.DataFrame(columns=["source_table", "content_hash", "file_size"])

    # Lista apenas arquivos .csv, ordenados para reprodutibilidade
    csv_files = sorted([f for f in data_dir.iterdir() if f.suffix.lower() == ".csv"])
    if not csv_files:
        return pd.DataFrame(columns=["source_table", "content_hash", "file_size"])

    records: list[dict[str, object]] = []
    for fpath in csv_files:
        try:
            content_hash = calcular_hash(fpath)
            file_size = fpath.stat().st_size
            records.append(
                {
                    "source_table": fpath.stem,
                    "content_hash": content_hash,
                    "file_size": file_size,
                }
            )
        except (OSError, PermissionError) as exc:
            logger.warning("Erro ao ler %s: %s", fpath, exc)
            continue

    if not records:
        return pd.DataFrame(columns=["source_table", "content_hash", "file_size"])

    df = pd.DataFrame(records)
    # Garante ordenação alfabética por source_table
    df = df.sort_values("source_table").reset_index(drop=True)
    return df


def eleger_canonicas(df_hashes: pd.DataFrame) -> pd.DataFrame:
    """Elege tabela canônica por grupo de hash.

    Para cada ``content_hash`` distinto, a primeira tabela em ordem alfabética
    é eleita ``canonical_table`` (``is_duplicate=False``). As demais são marcadas
    ``is_duplicate=True`` e apontam para a ``canonical_table`` do grupo.

    Args:
        df_hashes: DataFrame com colunas ``source_table``, ``content_hash``,
            ``file_size`` (gerado por ``agrupar_duplicatas``).

    Returns:
        DataFrame com colunas:
        - ``source_table``
        - ``content_hash``
        - ``file_size``
        - ``canonical_table``: nome da tabela canônica do grupo.
        - ``is_duplicate``: ``True`` se a tabela é duplicata, ``False`` se é canônica.

        Se o DataFrame de entrada for vazio, retorna DataFrame vazio com as
        mesmas colunas.

    Verificação de integridade:
        Se duas tabelas têm o mesmo hash mas ``file_size`` diferente (colisão),
        emite warning via ``logging.warning`` e trata como grupos separados.
    """
    if df_hashes.empty:
        return pd.DataFrame(
            columns=[
                "source_table",
                "content_hash",
                "file_size",
                "canonical_table",
                "is_duplicate",
            ]
        )

    # Faz cópia para não modificar o original
    df = df_hashes.copy()
    df["canonical_table"] = ""
    df["is_duplicate"] = False

    # Detecta colisões: mesmo hash, file_size diferente
    # Agrupa por hash e verifica se há mais de um file_size distinto no grupo
    hash_groups = df.groupby("content_hash")
    collision_hashes: set[str] = set()
    for hash_val, group in hash_groups:
        unique_sizes = group["file_size"].dropna().unique()
        if len(unique_sizes) > 1:
            logger.warning(
                "Colisão de hash MD5 detectada: hash=%s com tamanhos %s. "
                "Tratando como grupos separados por (hash, file_size).",
                hash_val,
                sorted(unique_sizes),
            )
            collision_hashes.add(str(hash_val))

    # Se há colisões, agrupa por (hash, file_size) em vez de apenas hash
    if collision_hashes:
        # Cria chave composta: hash + tamanho
        # Tabelas sem colisão usam hash puro; colididas usam hash+size
        df["_agrupador"] = df.apply(
            lambda r: (
                f"{r['content_hash']}_{r['file_size']}"
                if r["content_hash"] in collision_hashes
                else r["content_hash"]
            ),
            axis=1,
        )
        grupos = df.groupby("_agrupador", sort=False)
    else:
        grupos = df.groupby("content_hash", sort=False)

    # Para cada grupo, elege a primeira em ordem alfabética como canônica
    for _, group in grupos:
        # O DataFrame já está ordenado por source_table (herdado de
        # agrupar_duplicatas), mas garantimos com sort dentro do grupo
        group_sorted = group.sort_values("source_table")
        canonical = group_sorted.iloc[0]["source_table"]
        idx = group_sorted.index
        df.loc[idx, "canonical_table"] = canonical
        # Marca como duplicata tudo exceto a primeira
        df.loc[idx[1:], "is_duplicate"] = True

    # Remove coluna auxiliar de agrupamento se existir
    df = df.drop(columns=["_agrupador"], errors="ignore")

    return df


def gerar_mapping(df_canonicas: pd.DataFrame, output_path: Path) -> pd.DataFrame:
    """Escreve ``_dedup_map.csv`` e retorna o DataFrame.

    O arquivo CSV é comma-separated com colunas:
    ``source_table``, ``content_hash``, ``canonical_table``, ``is_duplicate``.

    Args:
        df_canonicas: DataFrame com colunas ``source_table``, ``content_hash``,
            ``canonical_table``, ``is_duplicate`` (gerado por ``eleger_canonicas``).
        output_path: Caminho para escrever o CSV de mapping (ex.:
            ``data/treated_tables/_dedup_map.csv``).

    Returns:
        O mesmo DataFrame (para uso encadeado).

    Nota:
        O diretório pai de ``output_path`` deve existir. A função não o cria
        automaticamente para manter separação de responsabilidades (I/O mínimo).
    """
    output = Path(output_path)
    # Garante que o diretório pai existe
    output.parent.mkdir(parents=True, exist_ok=True)

    cols = ["source_table", "content_hash", "canonical_table", "is_duplicate"]
    # Seleciona apenas as colunas de interesse na ordem especificada
    df_out = (
        df_canonicas[cols].copy()
        if not df_canonicas.empty
        else pd.DataFrame(columns=cols)
    )

    df_out.to_csv(output, sep=",", index=False)
    logger.info("Mapping de deduplicação escrito em: %s", output)
    return df_canonicas  # Retorna o DataFrame original para encadeamento


def agrupar_duplicatas_db(engine, tabelas: list[str] | None = None) -> pd.DataFrame:
    """Varre as tabelas do banco e retorna DataFrame com hashes de conteúdo.

    Para cada tabela, chama ``_ler_e_hash_tabela()`` que lê a tabela
    uma única vez, obtendo hash MD5 e row count na mesma passagem.
    Tabelas que geram erro de leitura são puladas com warning.

    Args:
        engine: SQLAlchemy Engine.
        tabelas: Lista de nomes de tabelas. Se None, usa ``listar_tabelas()``.

    Returns:
        DataFrame com colunas: ``source_table``, ``content_hash``, ``row_count``.
    """
    from classificacao.db.reader import _ler_e_hash_tabela, listar_tabelas
    from classificacao.db.connection import get_schema_source

    if tabelas is None:
        tabelas = listar_tabelas(engine)

    schema = get_schema_source()
    records: list[dict] = []
    for table_name in tabelas:
        try:
            content_hash, row_count = _ler_e_hash_tabela(
                engine, table_name, schema=schema
            )
        except Exception as exc:
            logger.warning("Erro ao processar %s: %s", table_name, exc)
            continue

        records.append(
            {
                "source_table": table_name,
                "content_hash": content_hash,
                "row_count": row_count,
            }
        )

    if not records:
        return pd.DataFrame(columns=["source_table", "content_hash", "row_count"])

    df = pd.DataFrame(records)
    df = df.sort_values("source_table").reset_index(drop=True)
    return df


def gerar_mapping_db(df_canonicas: pd.DataFrame, engine) -> pd.DataFrame:
    """Escreve ``_dedup_map`` no schema target via ``escrever_dedup_map``.

    Adaptação de ``gerar_mapping`` para o modo DB.

    Args:
        df_canonicas: DataFrame com colunas ``source_table``, ``content_hash``,
            ``canonical_table``, ``is_duplicate`` (gerado por ``eleger_canonicas``).
        engine: SQLAlchemy Engine.

    Returns:
        O mesmo DataFrame (para uso encadeado).
    """
    from classificacao.db.writer import escrever_dedup_map

    escrever_dedup_map(df_canonicas, engine)
    return df_canonicas
