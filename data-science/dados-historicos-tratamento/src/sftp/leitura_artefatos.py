"""Leitura dos CSVs de artefatos extraídos do PostgreSQL.

Os artefatos são gerados manualmente via DBeaver (queries SQL) e salvos em
``data/sftp/artefatos/`` com prefixo de data no nome. Este módulo localiza
o arquivo mais recente de cada tipo e o carrega como ``pd.DataFrame``.

Tipos de artefato reconhecidos
-------------------------------
inventario_sftp
    Colunas do schema ``sftp`` (``table_name, column_name, data_type``).
inventario_dh
    Colunas do schema ``dados_historicos_formatados``.
estrutura_sftp
    Metadados por tabela do schema ``sftp`` (hash MD5, tamanho, colunas,
    tuplas vivas/mortas, timestamps de vacuum/analyze).
estrutura_dh
    Metadados por tabela do schema ``dados_historicos``.
comparacao_sftp_dh
    Comparação de hash estrutural entre ``sftp`` e ``dados_historicos``
    (``hash_estrutura, quantidade_tabelas, tabelas``).
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

ARTEFATOS_DIR = Path("data/sftp/artefatos")

# Patterns para identificar o tipo de artefato pelo nome do arquivo.
# Ordem importa: patterns mais específicos primeiro.
_PATTERNS: dict[str, str] = {
    "inventario_sftp": r"inventario_tabelas_colunas_sftp\.csv$",
    "inventario_dh": r"inventario_tabelas_colunas_dados_historicos_formatados\.csv$",
    "inventario_dhf": r"inventario_tabelas_colunas_dados_historicos_formatados\.csv$",
    "estrutura_sftp": r"estrutura_sftp\.csv$",
    "estrutura_dh": r"estrutura_dados_historicos\.csv$",
    "comparacao_sftp_dh": r"estrutura_sftp_vs_dados_historicos\.csv$",
}

_TIPOS_VALIDOS = frozenset(_PATTERNS.keys())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def listar_artefatos(tipo: str, diretorio: Path | None = None) -> Path:
    """Encontra o arquivo mais recente de um tipo de artefato.

    Parameters
    ----------
    tipo : str
        Tipo de artefato. Deve ser um de: inventario_sftp, inventario_dh,
        inventario_dhf, estrutura_sftp, estrutura_dh, comparacao_sftp_dh.
    diretorio : Path, optional
        Diretório onde buscar. Default: ``data/sftp/artefatos/``.

    Returns
    -------
    Path
        Caminho para o arquivo mais recente.

    Raises
    ------
    ValueError
        Se o tipo não for reconhecido.
    FileNotFoundError
        Se nenhum artefato do tipo for encontrado.
    """
    if tipo not in _TIPOS_VALIDOS:
        raise ValueError(
            f"Tipo de artefato desconhecido: {tipo!r}. "
            f"Tipos válidos: {sorted(_TIPOS_VALIDOS)}"
        )

    base = diretorio or ARTEFATOS_DIR
    pattern = re.compile(_PATTERNS[tipo])

    candidatos: list[Path] = []
    for f in base.iterdir():
        if f.is_file() and pattern.search(f.name):
            candidatos.append(f)

    if not candidatos:
        raise FileNotFoundError(
            f"Nenhum artefato do tipo {tipo!r} encontrado em {base}. "
            f"Padrão esperado: {_PATTERNS[tipo]}"
        )

    if len(candidatos) > 1:
        logger.warning(
            "Múltiplos artefatos do tipo %r encontrados (%d). "
            "Usando o mais recente por nome: %s",
            tipo,
            len(candidatos),
            max(candidatos, key=lambda p: p.name).name,
        )

    return max(candidatos, key=lambda p: p.name)


def ler_inventario(caminho: Path) -> pd.DataFrame:
    """Lê um CSV de inventário de colunas.

    Espera colunas: ``table_name, column_name, data_type``.

    Parameters
    ----------
    caminho : Path
        Caminho para o CSV de inventário.

    Returns
    -------
    pd.DataFrame
        DataFrame com colunas ``table_name``, ``column_name``, ``data_type``.
    """
    df = pd.read_csv(caminho, dtype=str)
    # Normalizar nomes de colunas (CSVs podem ter aspas no header)
    df.columns = [c.strip().strip('"') for c in df.columns]
    return df


def ler_estrutura(caminho: Path) -> pd.DataFrame:
    """Lê um CSV de estrutura (metadados por tabela).

    Colunas retornadas são as originais do CSV de estrutura SQL:
    ``tabela, hash_estrutura_md5, linhas_estimadas, tamanho_total,
    tamanho_tabela, tamanho_indices, tamanho_toast, qtd_colunas,
    comentario, tuplas_vivas, tuplas_mortas, last_vacuum,
    last_autovacuum, last_analyze, last_autoanalyze``.

    Parameters
    ----------
    caminho : Path
        Caminho para o CSV de estrutura.

    Returns
    -------
    pd.DataFrame
    """
    df = pd.read_csv(caminho, dtype=str)
    df.columns = [c.strip().strip('"') for c in df.columns]
    # Converter colunas numéricas
    for col in ("linhas_estimadas", "qtd_colunas", "tuplas_vivas", "tuplas_mortas"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def ler_comparacao(caminho: Path) -> pd.DataFrame:
    """Lê um CSV de comparação de hash entre schemas.

    Colunas: ``hash_estrutura, quantidade_tabelas, tabelas``.
    A coluna ``tabelas`` contém nomes no formato ``schema.tabela``
    separados por ``;``.

    Parameters
    ----------
    caminho : Path
        Caminho para o CSV de comparação.

    Returns
    -------
    pd.DataFrame
    """
    df = pd.read_csv(caminho, dtype=str)
    df.columns = [c.strip().strip('"') for c in df.columns]
    if "quantidade_tabelas" in df.columns:
        df["quantidade_tabelas"] = pd.to_numeric(
            df["quantidade_tabelas"], errors="coerce"
        )
    return df


def indexar_por_hash(df_comparacao: pd.DataFrame) -> dict[str, set[str]]:
    """Constrói índice hash → conjunto de nomes de tabela.

    Cada nome está no formato ``schema.tabela``.

    Parameters
    ----------
    df_comparacao : pd.DataFrame
        DataFrame retornado por :func:`ler_comparacao`.

    Returns
    -------
    dict[str, set[str]]
        Mapeamento hash MD5 → conjunto de ``schema.tabela``.
    """
    indice: dict[str, set[str]] = {}
    for _, row in df_comparacao.iterrows():
        h = row["hash_estrutura"]
        tabelas_raw = row.get("tabelas", "")
        if pd.isna(tabelas_raw) or not tabelas_raw:
            continue
        tabelas = {t.strip() for t in str(tabelas_raw).split(";") if t.strip()}
        if h in indice:
            indice[h].update(tabelas)
        else:
            indice[h] = tabelas
    return indice


def _carregar_todos_artefatos(
    artefatos_dir: Path | None = None,
    schema_dump: str = "dados_historicos",
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    dict[str, set[str]],
]:
    """Carrega todos os artefatos de uma vez.

    Utilitário interno usado por ``executar_batimento``.

    Parameters
    ----------
    artefatos_dir : Path, optional
        Diretório dos artefatos. Default: ``data/sftp/artefatos/``.
    schema_dump : str
        Schema alvo do dump. Quando ``"dados_historicos_formatados"``, usa o
        artefato ``inventario_dhf`` no lugar de ``inventario_dh`` para o
        inventário de colunas do dump. Default: ``"dados_historicos"``.

    Returns
    -------
    tuple
        (inventario_sftp, inventario_dh, estrutura_sftp, estrutura_dh,
         comparacao, index_hash)
    """
    base = artefatos_dir or ARTEFATOS_DIR

    inv_sftp = ler_inventario(listar_artefatos("inventario_sftp", base))
    tipo_inv_dump = (
        "inventario_dhf"
        if schema_dump == "dados_historicos_formatados"
        else "inventario_dh"
    )
    inv_dh = ler_inventario(listar_artefatos(tipo_inv_dump, base))
    est_sftp = ler_estrutura(listar_artefatos("estrutura_sftp", base))
    est_dh = ler_estrutura(listar_artefatos("estrutura_dh", base))
    comp = ler_comparacao(listar_artefatos("comparacao_sftp_dh", base))
    idx = indexar_por_hash(comp)

    logger.info(
        "Artefatos carregados: sftp=%d cols, dh=%d cols, "
        "estrutura sftp=%d tabs, estrutura dh=%d tabs, "
        "comparacao=%d grupos",
        len(inv_sftp),
        len(inv_dh),
        len(est_sftp),
        len(est_dh),
        len(comp),
    )
    return inv_sftp, inv_dh, est_sftp, est_dh, comp, idx
