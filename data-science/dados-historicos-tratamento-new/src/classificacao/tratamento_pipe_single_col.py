"""Tratamento de tabelas BB 2013-06 carregadas como coluna única com pipe.

Este módulo detecta e trata 5 tabelas do lote ``bb_2013_06_junho_pmcmv_18062013_``
que foram carregadas no PostgreSQL como uma única coluna, onde os dados
originais são separados por ``|`` (pipe).

Funções
-------
detectar_tabelas_pipe
    Identifica tabelas no schema source com exatamente 1 coluna e que
    contenham ao menos um ``|`` nos dados.
tratar_tabela_pipe
    Lê, expande, limpa e tipa uma tabela pipe, retornando um DataFrame.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from sqlalchemy import Engine, text

from .db.connection import get_schema_source
from .db.reader import ler_tabela

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────

_PREFIXO_BB_2013_06 = "bb_2013_06_junho_pmcmv_18062013_"

#: Mapeamento das 5 tabelas que precisam de expansão pipe.
#: Cada entrada contém:
#: - n_cols: número esperado de campos separados por pipe
#: - colunas: lista de dicts com ``nome`` e ``tipo`` (int, date, decimal, bool, text)
#: - drop_cols: índices 0-based das colunas a descartar
MAPEAMENTO_PIPE: dict[str, dict[str, Any]] = {
    f"{_PREFIXO_BB_2013_06}tab_contratos_pj": {
        "n_cols": 4,
        "colunas": [
            {"nome": "cod_contrato_pj", "tipo": "int"},
            {"nome": "cod_operacao", "tipo": "int"},
            {"nome": "cod_sit_contrato_pj", "tipo": "int"},
            {"nome": "dte_assin_contrato", "tipo": "date"},
        ],
        "drop_cols": [],
    },
    f"{_PREFIXO_BB_2013_06}tab_proponentes": {
        "n_cols": 4,
        "colunas": [
            {"nome": "cod_cnpj_proponente", "tipo": "text"},
            {"nome": "cod_porte_construtor", "tipo": "int"},
            {"nome": "cod_nat_proponente", "tipo": "int"},
            {"nome": "txt_razao_social", "tipo": "text"},
        ],
        "drop_cols": [],
    },
    f"{_PREFIXO_BB_2013_06}tab_unidades_concluidas": {
        "n_cols": 7,
        "colunas": [
            {"nome": "cod_operacao", "tipo": "int"},
            {"nome": "dte_legalizacao", "tipo": "date"},
            {"nome": "cod_motivo_ociosidade", "tipo": "int"},
            {"nome": "dte_primeira_entrega", "tipo": "date"},
            {"nome": "dte_02", "tipo": "date"},
            {"nome": "cod_01", "tipo": "int"},
            {"nome": "cod_02", "tipo": "int"},
        ],
        "drop_cols": [],
    },
    f"{_PREFIXO_BB_2013_06}tab_caracterizacoes_entornos": {
        "n_cols": 16,
        "colunas": [
            {"nome": "cod_operacao", "tipo": "int"},
            {"nome": "bln_empreendimento_his", "tipo": "bool"},
            {"nome": "bln_rede_abast_agua", "tipo": "bool"},
            {"nome": "bln_coleta_esgoto", "tipo": "bool"},
            {"nome": "bln_01", "tipo": "bool"},
            {"nome": "bln_02", "tipo": "bool"},
            {"nome": "bln_03", "tipo": "bool"},
            {"nome": "bln_04", "tipo": "bool"},
            {"nome": "bln_05", "tipo": "bool"},
            {"nome": "bln_06", "tipo": "bool"},
            {"nome": "bln_07", "tipo": "bool"},
            {"nome": "bln_08", "tipo": "bool"},
            {"nome": "bln_09", "tipo": "bool"},
            {"nome": "bln_10", "tipo": "bool"},
            {"nome": "bln_11", "tipo": "bool"},
            {"nome": "bln_12", "tipo": "bool"},
        ],
        "drop_cols": [],
    },
    f"{_PREFIXO_BB_2013_06}tab_andamento_obras": {
        "n_cols": 17,
        "colunas": [
            {"nome": "cod_operacao", "tipo": "int"},
            {"nome": "cod_sit_obra", "tipo": "int"},
            {"nome": "cod_regime_construcao", "tipo": "int"},
            {"nome": "cod_pendencia_obra", "tipo": "int"},
            {"nome": "vazia_04", "tipo": "text"},  # dropped
            {"nome": "vazia_05", "tipo": "text"},  # dropped
            {"nome": "vazia_06", "tipo": "text"},  # dropped
            {"nome": "vazia_07", "tipo": "text"},  # dropped
            {"nome": "vazia_08", "tipo": "text"},  # dropped
            {"nome": "dte_01", "tipo": "date"},
            {"nome": "dte_02", "tipo": "date"},
            {"nome": "dte_03", "tipo": "date"},
            {"nome": "vlr_01", "tipo": "decimal"},
            {"nome": "vlr_02", "tipo": "decimal"},
            {"nome": "vlr_03", "tipo": "decimal"},
            {"nome": "dte_04", "tipo": "date"},
            {"nome": "dte_05", "tipo": "date"},
        ],
        "drop_cols": [4, 5, 6, 7, 8],
    },
}

__all__ = [
    "MAPEAMENTO_PIPE",
    "detectar_tabelas_pipe",
    "tratar_tabela_pipe",
]


# ── Funções públicas ──────────────────────────────────────────────────


def detectar_tabelas_pipe(engine: Engine) -> list[str]:
    """Detecta tabelas no schema source com 1 coluna e dados pipe.

    Consulta ``information_schema.columns`` para encontrar tabelas com
    exatamente 1 coluna. Para cada uma, verifica se ao menos uma linha
    não-nula contém o caractere ``|``.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Conexão com o banco PostgreSQL.

    Returns
    -------
    list[str]
        Lista de nomes de tabelas que se encaixam no padrão pipe.
    """
    schema = get_schema_source()
    tabelas_pipe: list[str] = []

    # 1. Descobrir tabelas com exatamente 1 coluna
    stmt = text(
        "SELECT table_name "
        "FROM information_schema.columns "
        "WHERE table_schema = :schema "
        "GROUP BY table_name "
        "HAVING COUNT(*) = 1 "
        "ORDER BY table_name"
    )

    with engine.connect() as conn:
        result = conn.execute(stmt, {"schema": schema})
        tabelas_uma_coluna = [row[0] for row in result]

    if not tabelas_uma_coluna:
        logger.info("Nenhuma tabela com 1 coluna encontrada no schema '%s'.", schema)
        return tabelas_pipe

    # 2. Para cada uma, checar se contém pipe nos dados
    for tbl in tabelas_uma_coluna:
        # Descobrir o nome da coluna única
        col_stmt = text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table_name "
            "ORDER BY ordinal_position"
        )
        with engine.connect() as conn:
            col_name = conn.scalar(col_stmt, {"schema": schema, "table_name": tbl})

        if col_name is None:
            continue

        # Verificar se ao menos 1 linha não-nula contém pipe
        check_stmt = text(
            f'SELECT COUNT(*) FROM "{schema}"."{tbl}" '
            f'WHERE "{col_name}" IS NOT NULL '
            f"AND \"{col_name}\" LIKE '%|%'"
        )
        with engine.connect() as conn:
            count = conn.scalar(check_stmt)

        if count is not None and count > 0:
            tabelas_pipe.append(tbl)
            logger.debug(
                "Tabela '%s' detectada como pipe (coluna única, %d linhas com |).",
                tbl,
                count,
            )

    if tabelas_pipe:
        logger.info(
            "Detectadas %d tabela(s) pipe: %s.", len(tabelas_pipe), tabelas_pipe
        )
    else:
        logger.info(
            "Nenhuma tabela com 1 coluna contém dados pipe no schema '%s'.", schema
        )

    return tabelas_pipe


def tratar_tabela_pipe(
    engine: Engine,
    table_name: str,
) -> pd.DataFrame | None:
    """Expande e trata uma tabela pipe, retornando um DataFrame tipado.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Conexão com o banco PostgreSQL.
    table_name : str
        Nome completo da tabela (com prefixo ``bb_2013_06_...``).

    Returns
    -------
    pd.DataFrame | None
        DataFrame tratado, ou ``None`` se a tabela não estiver mapeada.
    """
    # 1. Verificar se a tabela está no mapeamento
    if table_name not in MAPEAMENTO_PIPE:
        logger.warning(
            "Tabela '%s' detectada como pipe, mas não possui mapeamento "
            "em MAPEAMENTO_PIPE. Ignorando.",
            table_name,
        )
        return None

    mapping = MAPEAMENTO_PIPE[table_name]

    # 2. Ler a tabela do banco
    df = ler_tabela(engine, table_name)

    # 3. Obter o nome da coluna única
    col_unica = df.columns[0]

    # 4. Expandir pela barra vertical
    #    O resultado é uma lista de listas com o mesmo número de colunas
    expandido = df[col_unica].str.split("|", expand=True)

    #    Se o split não gerou o número esperado de colunas, avisar
    if expandido.shape[1] != mapping["n_cols"]:
        logger.warning(
            "Tabela '%s': esperadas %d colunas após split, "
            "mas obtidas %d. O resultado pode estar incorreto.",
            table_name,
            mapping["n_cols"],
            expandido.shape[1],
        )

    # 5. Renomear colunas com nomes temporários
    expandido.columns = list(range(expandido.shape[1]))

    # 6. Dropar colunas indesejadas
    drop_cols = mapping["drop_cols"]
    if drop_cols:
        # Só dropar índices que realmente existem
        indices_validos = [i for i in drop_cols if i < expandido.shape[1]]
        if indices_validos:
            expandido = expandido.drop(columns=indices_validos)

    # 7. Renomear colunas para os nomes definitivos
    colunas_mantidas: list[dict[str, Any]] = []
    indices_originais: list[int] = []
    for i, c in enumerate(mapping["colunas"]):
        if i not in drop_cols:
            colunas_mantidas.append(c)
            indices_originais.append(i)
    novos_nomes = {
        orig: col["nome"] for orig, col in zip(indices_originais, colunas_mantidas)
    }
    expandido = expandido.rename(columns=novos_nomes)

    # 8. Coerção de tipos
    for col_def in colunas_mantidas:
        nome = col_def["nome"]
        tipo = col_def["tipo"]

        if tipo == "int":
            expandido[nome] = pd.to_numeric(expandido[nome], errors="coerce")
        elif tipo == "date":
            expandido[nome] = pd.to_datetime(expandido[nome], errors="coerce")
        elif tipo == "decimal":
            expandido[nome] = pd.to_numeric(expandido[nome], errors="coerce")
        elif tipo == "bool":
            expandido[nome] = expandido[nome].replace({"T": True, "F": False})
        elif tipo == "text":
            expandido[nome] = expandido[nome].astype(str)
        else:
            logger.warning(
                "Tipo desconhecido '%s' para coluna '%s' na tabela '%s'. "
                "Mantendo como string.",
                tipo,
                nome,
                table_name,
            )
            expandido[nome] = expandido[nome].astype(str)

    logger.info(
        "Tabela pipe '%s' tratada com sucesso: %d linhas, %d colunas.",
        table_name,
        len(expandido),
        len(expandido.columns),
    )

    return expandido
