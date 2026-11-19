"""Tratamento de tabelas SFTP: canonicalização GEFUS e Pipe Padrão B.

Este módulo implementa dois grupos de funcionalidade para o schema ``sftp``:

1. **Canonicalização GEFUS** — Detecta pares de tabelas (não-gefus / gefus) no
   schema ``sftp``, verifica equivalência via row count, e elege a versão GEFUS
   como canônica para cópia ao schema target.

2. **Tratamento Pipe Padrão B** — Detecta tabelas no schema ``sftp`` cuja
   primeira coluna possui um nome longo (>40 chars) com underscores (header
   pipe-concatenado) e que NÃO possuem versão GEFUS. Aplica split por ``|``
   na coluna 0 e escreve a tabela tratada no schema target.
"""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Engine, text

from .db.connection import get_schema_sftp_target

logger = logging.getLogger(__name__)

__all__ = [
    # Grupo 1
    "normalizar_nome_sftp",
    "agrupar_pares_gefus",
    "verificar_row_count",
    "eleger_canonicas_gefus",
    "copiar_tabela_gefus",
    "gerar_mapeamento_gefus",
    "escrever_canonicas",
    "escrever_classificacao_sftp",
    # Grupo 2
    "detectar_padrao_b",
    "extrair_nomes_colunas",
    "split_pipe_col0",
    "tratar_tabela_padrao_b",
    "detectar_anomalias_pipe",
    # Utilitários
    "_detectar_int055_sem_pipe",
]

# ── Constantes ──────────────────────────────────────────────────────────────

_PREFIXO_GEFUS_ANTERIORES = "gefus_anteriores_"
_PREFIXO_GEFUS = "gefus_"

# Padrão conhecido: coluna 0 da família caixa_af_gehis_andamento_obra
_PADRAO_ANDAMENTO_OBRA = "anomes_nu_apf_dt_prevista_conclusao"
_COLUNAS_ANDAMENTO_OBRA = [
    "anomes",
    "nu_apf",
    "dt_prevista_conclusao",
    "dt_prevista_inauguracao",
    "situacao_obra",
]

# Padrões conhecidos: famílias tab_validacao
_PADRAO_VALIDACAO_PJ = "cod_validacao_dsc_validacao_name_file_nu_apf_data_log"
_COLUNAS_VALIDACAO_PJ = [
    "cod_validacao",
    "dsc_validacao",
    "name_file",
    "nu_apf",
    "data_log",
]

_PADRAO_VALIDACAO_PF = "cod_validacao_dsc_validacao_name_file_txt_cpf"
_COLUNAS_VALIDACAO_PF = [
    "cod_validacao",
    "dsc_validacao",
    "name_file_txt",
    "cpf",
]

# Padrão conhecido: caixa_af_gehis_operacao_desenquadrada
# Header: _YYYYMM_NNNNNNNN_proponente_entidade_peblica_tipologia
_PADRAO_OPERACAO_DESENQUADRADA = "_proponente_entidade"
_COLUNAS_OPERACAO_DESENQUADRADA = [
    "ano_mes",
    "nu_apf",
    "txt_informacoes",
]

# Prefixos que indicam início de novo campo no header concatenado
_PREFIXOS_CONHECIDOS: frozenset[str] = frozenset(
    {
        "nu",
        "dt",
        "co",
        "vr",
        "no",
        "sg",
        "cd",
        "vl",
    }
)

# ═════════════════════════════════════════════════════════════════════════════
# Grupo 1: GEFUS Canonicalization
# ═════════════════════════════════════════════════════════════════════════════


def normalizar_nome_sftp(table_name: str) -> str:
    """Remove prefixo ``gefus_anteriores_`` / ``gefus_`` do nome da tabela.

    Aplica as remoções em ordem:
    1. ``gefus_anteriores_`` (se presente)
    2. ``gefus_`` (se presente após o passo 1)

    Parameters
    ----------
    table_name : str
        Nome original da tabela no schema ``sftp``.

    Returns
    -------
    str
        Nome base sem os prefixos GEFUS.
    """
    name = table_name
    if name.startswith(_PREFIXO_GEFUS_ANTERIORES):
        name = name[len(_PREFIXO_GEFUS_ANTERIORES) :]
    elif name.startswith(_PREFIXO_GEFUS):
        name = name[len(_PREFIXO_GEFUS) :]
    return name


def agrupar_pares_gefus(
    engine: Engine,
    schema: str = "sftp",
) -> dict[str, dict[str, list[str]]]:
    """Lista tabelas do schema *schema* e agrupa por nome normalizado.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Conexão com o banco PostgreSQL.
    schema : str, default ``"sftp"``
        Schema onde as tabelas SFTP estão armazenadas.

    Returns
    -------
    dict[str, dict[str, list[str]]]
        Estrutura ``{base_name: {'nao_gefus': [...], 'gefus': [...]}}``.
        Tabelas **sem** prefixo ``gefus_``/``gefus_anteriores_`` vão para a
        chave ``'nao_gefus'``; tabelas **com** prefixo vão para ``'gefus'``.
    """
    tabelas = _listar_tabelas_sftp(engine, schema)
    pares: dict[str, dict[str, list[str]]] = {}

    for tbl in tabelas:
        base = normalizar_nome_sftp(tbl)
        if base not in pares:
            pares[base] = {"nao_gefus": [], "gefus": []}

        if tbl.startswith(_PREFIXO_GEFUS_ANTERIORES) or tbl.startswith(_PREFIXO_GEFUS):
            pares[base]["gefus"].append(tbl)
        else:
            pares[base]["nao_gefus"].append(tbl)

    logger.info(
        "Agrupamento GEFUS concluído: %d grupos, %d com par completo.",
        len(pares),
        sum(1 for v in pares.values() if v["nao_gefus"] and v["gefus"]),
    )
    return pares


def verificar_row_count(
    engine: Engine,
    table_nao_gefus: str,
    table_gefus: str,
    schema: str = "sftp",
) -> dict[str, Any]:
    """Compara ``COUNT(*)`` das duas tabelas do par.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Conexão com o banco PostgreSQL.
    table_nao_gefus : str
        Nome da tabela sem prefixo GEFUS.
    table_gefus : str
        Nome da tabela com prefixo GEFUS.
    schema : str, default ``"sftp"``
        Schema onde as tabelas residem.

    Returns
    -------
    dict[str, Any]
        Dicionário com as chaves:
        - ``nao_gefus_count`` (int): linhas na tabela não-gefus.
        - ``gefus_count`` (int): linhas na tabela GEFUS.
        - ``match`` (bool): ``True`` se os counts forem iguais.
    """
    nao_gefus_count = _contar_linhas(engine, table_nao_gefus, schema)
    gefus_count = _contar_linhas(engine, table_gefus, schema)

    match = nao_gefus_count == gefus_count

    logger.debug(
        "Row counts: %s = %d | %s = %d | match=%s",
        table_nao_gefus,
        nao_gefus_count,
        table_gefus,
        gefus_count,
        match,
    )

    return {
        "nao_gefus_count": nao_gefus_count,
        "gefus_count": gefus_count,
        "match": match,
    }


def eleger_canonicas_gefus(
    pares: dict[str, dict[str, list[str]]],
    engine: Engine,
    schema: str = "sftp",
) -> pd.DataFrame:
    """Para cada par, elege a tabela canônica com base em row count.

    Regras de eleição:
    - Se ambos os lados existem **e** os row counts coincidem → a GEFUS é eleita.
    - Se os row counts divergem → a tabela **com mais linhas** é eleita.
    - Se apenas o lado GEFUS existe (standalone) → a GEFUS é a canônica.

    Parameters
    ----------
    pares : dict
        Estrutura retornada por :func:`agrupar_pares_gefus`.
    engine : sqlalchemy.engine.Engine
        Conexão com o banco PostgreSQL.
    schema : str, default ``"sftp"``
        Schema onde as tabelas residem.

    Returns
    -------
    pd.DataFrame
        DataFrame com as colunas:
        - ``table_name``: nome original da tabela não-gefus (ou GEFUS se
          standalone).
        - ``canonical_table``: nome da tabela eleita como canônica.
        - ``n_rows_orig``: row count da tabela original.
        - ``n_rows_canonical``: row count da tabela canônica.
        - ``is_pipe``: ``True`` se a tabela canônica tem col0 com pipe (nota
          preliminar).
        - ``row_count_match``: ``True`` se os counts coincidirem (``None``
          para standalone).
    """
    registos: list[dict[str, Any]] = []

    for base_name, grupos in sorted(pares.items()):
        nao_gefus_list = grupos["nao_gefus"]
        gefus_list = grupos["gefus"]

        if not gefus_list:
            # Sem contraparte GEFUS — não entra como canônica GEFUS.
            continue

        # Usar sempre o primeiro elemento de cada lista (nomes únicos esperados).
        table_nao_gefus = nao_gefus_list[0] if nao_gefus_list else None
        table_gefus = gefus_list[0]

        if table_nao_gefus and table_gefus:
            # Par completo: comparar row counts.
            counts = verificar_row_count(engine, table_nao_gefus, table_gefus, schema)
            n_nao = counts["nao_gefus_count"]
            n_gefus = counts["gefus_count"]
            match = counts["match"]

            if match or n_gefus >= n_nao:
                canonical = table_gefus
                n_orig = n_nao
                n_canonical = n_gefus
            else:
                canonical = table_nao_gefus
                n_orig = n_nao
                n_canonical = n_nao

            registos.append(
                {
                    "table_name": table_nao_gefus,
                    "canonical_table": canonical,
                    "n_rows_orig": n_orig,
                    "n_rows_canonical": n_canonical,
                    "is_pipe": False,  # preenchido externamente se necessário
                    "row_count_match": match,
                }
            )

        else:
            # Standalone GEFUS.
            n_gefus = _contar_linhas(engine, table_gefus, schema)
            registos.append(
                {
                    "table_name": table_gefus,
                    "canonical_table": table_gefus,
                    "n_rows_orig": n_gefus,
                    "n_rows_canonical": n_gefus,
                    "is_pipe": False,
                    "row_count_match": None,
                }
            )

    df = pd.DataFrame(registos, dtype=str)
    # Ajustar tipos numéricos
    for col in ("n_rows_orig", "n_rows_canonical"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    logger.info(
        "Canônicas eleitas: %d tabelas (%d pares, %d standalone GEFUS).",
        len(df),
        sum(df["row_count_match"].notna()) if "row_count_match" in df.columns else 0,
        sum(df["row_count_match"].isna()) if "row_count_match" in df.columns else 0,
    )
    return df


def _canonical_tem_pipe(
    engine: Engine,
    table_name: str,
    schema: str = "sftp",
) -> bool:
    """Check if the first column of a GEFUS canonical has pipe-concatenated header.

    The heuristic mirrors ``detectar_padrao_b``: col0 name > 40 chars AND
    contains at least one underscore.

    Returns True if the canonical is likely broken (pipe in col0).
    """
    stmt = text(
        "SELECT column_name "
        "FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table_name "
        "  AND ordinal_position = 1"
    )
    with engine.connect() as conn:
        col0 = conn.scalar(stmt, {"schema": schema, "table_name": table_name})

    if col0 is None:
        return False

    col0_str = str(col0)
    return len(col0_str) > 40 and "_" in col0_str


def copiar_tabela_gefus(
    engine: Engine,
    source_table: str,
    target_schema: str | None = None,
    source_schema: str = "sftp",
) -> tuple[int, bool]:
    """Copy a GEFUS table to target_schema ONLY if pipe is detected.

    If the canonical is healthy (no pipe in col0), it is NOT copied —
    the table remains in the source schema and is only referenced in metadata.

    Returns
    -------
    tuple[int, bool]
        ``(n_rows, has_pipe)`` where ``has_pipe`` indicates if split was applied.
        ``n_rows=0`` with ``has_pipe=False`` means "healthy canonical, not copied".
    """
    if target_schema is None:
        target_schema = get_schema_sftp_target()

    tem_pipe = _canonical_tem_pipe(engine, source_table, source_schema)

    # Verificar se é andamento_obra com 1 coluna e dados sem pipe (split posicional).
    # Esta verificação ocorre ANTES do split normal porque _canonical_tem_pipe
    # pode retornar True para tabelas cujo header é longo mas os dados não têm '|'.
    _COL0_STMT = text(
        "SELECT column_name "
        "FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table_name "
        "  AND ordinal_position = 1"
    )
    _NCOLS_STMT = text(
        "SELECT COUNT(*) "
        "FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table_name"
    )
    with engine.connect() as conn:
        col0_name = conn.scalar(
            _COL0_STMT, {"schema": source_schema, "table_name": source_table}
        )
        n_cols = (
            conn.scalar(
                _NCOLS_STMT, {"schema": source_schema, "table_name": source_table}
            )
            or 0
        )

    col0_str = str(col0_name).lower() if col0_name else ""

    # Detecção de andamento_obra: header começa com "anomes" e contém marcadores
    # da família. O prefixo pode ser concatenado (anomesnu_apf...) ou com undersc.
    _eh_andamento_obra = (
        col0_str.startswith("anomes")
        and "apf" in col0_str
        and ("prevista" in col0_str or "situac" in col0_str)
    )

    if _eh_andamento_obra and n_cols == 1:
        # Verificar se os dados contêm pipe. Se não, usar split posicional.
        stmt_sample = text(
            f'SELECT "{col0_name}" FROM "{source_schema}"."{source_table}" LIMIT 1'
        )
        with engine.connect() as conn:
            sample = conn.scalar(stmt_sample)
        has_pipe_char = sample is not None and "|" in str(sample)

        if not has_pipe_char:
            logger.info(
                "Andamento_obra posicional detectada em '%s' — "
                "aplicando split posicional.",
                source_table,
            )
            try:
                df_full = pd.read_sql_table(source_table, engine, schema=source_schema)
                df_split = _split_posicional_andamento_obra(df_full, str(col0_name))
                # Adicionar colunas de metadado se existirem
                meta_cols = [
                    c
                    for c in df_full.columns
                    if c.lower() in ("dt_ingest", "arquivo_origem")
                ]
                for mc in meta_cols:
                    df_split[mc] = df_full[mc]

                df_split["_profile"] = "gefus_canonica_posicional_split"

                _escrever_tabela_tratada(df_split, source_table, engine, target_schema)
                n_rows = len(df_split)
                logger.info(
                    "Andamento_obra posicional '%s' tratada: %d linhas, %d colunas.",
                    source_table,
                    n_rows,
                    len(df_split.columns),
                )
                return (n_rows, False)
            except Exception as exc:
                logger.warning(
                    "Erro no split posicional de '%s': %s. "
                    "Considerando como saudável e não copiando.",
                    source_table,
                    exc,
                )
                return (0, False)

    if not tem_pipe:
        # Canonical saudável: não copiar
        logger.info(
            "Canonical '%s' saudável — não copiada (já está limpa no '%s').",
            source_table,
            source_schema,
        )
        return (0, False)

    # Canonical com pipe: extrair nomes e fazer split
    stmt = text(
        "SELECT column_name "
        "FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table_name "
        "  AND ordinal_position = 1"
    )
    with engine.connect() as conn:
        col0_name = conn.scalar(
            stmt, {"schema": source_schema, "table_name": source_table}
        )

    col_names = extrair_nomes_colunas(str(col0_name) if col0_name else "")
    if not col_names:
        logger.warning(
            "Canonical '%s' tem pipe mas nao foi possivel extrair nomes. "
            "Fazendo copia normal.",
            source_table,
        )
        tem_pipe = False
    else:
        try:
            df = split_pipe_col0(engine, source_table, col_names, source_schema)
            if df.empty:
                logger.warning(
                    "Split de '%s' produziu DataFrame vazio. Fazendo copia normal.",
                    source_table,
                )
                tem_pipe = False
            else:
                _escrever_tabela_tratada(df, source_table, engine, target_schema)
                n_rows = len(df)
                logger.info(
                    "Canonical quebrada '%s' tratada via split: %d linhas, %d colunas.",
                    source_table,
                    n_rows,
                    len(df.columns),
                )
                return (n_rows, True)
        except Exception as exc:
            logger.warning(
                "Erro no split de '%s': %s. Fazendo copia normal.",
                source_table,
                exc,
            )
            tem_pipe = False

    # Fallback: copia normal (split falhou, mas canonical tem pipe)
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))
        conn.execute(
            text(
                f'CREATE TABLE "{target_schema}"."{source_table}" AS '
                f'SELECT * FROM "{source_schema}"."{source_table}"'
            )
        )
        conn.commit()

    n_rows = _contar_linhas(engine, source_table, target_schema)
    logger.info(
        "Tabela '%s.%s' copiada para '%s.%s' (%d linhas) — fallback de pipe.",
        source_schema,
        source_table,
        target_schema,
        source_table,
        n_rows,
    )
    return (n_rows, False)


# ── Regex para split posicional de andamento_obra ──────────────────────────

_POS_REGEX = re.compile(
    r"^(\d{6})(\d{8})(\d{2}/\d{2}/\d{4})?(\d{2}/\d{2}/\d{4})?(.+)?$"
)


def _split_posicional_andamento_obra(df: pd.DataFrame, col0_name: str) -> pd.DataFrame:
    """Split posicional da coluna concatenada de ``andamento_obra``.

    Aplica a regex ``_POS_REGEX`` para extrair 5 campos posicionais:

        anomes (6 dígitos)
        nu_apf (8 dígitos)
        dt_prevista_conclusao (data opcional, dd/mm/aaaa)
        dt_prevista_inauguracao (data opcional, dd/mm/aaaa)
        situacao_obra (restante opcional)

    Linhas que **não** casam com a regex têm o valor original preservado
    na coluna ``anomes`` e ``None`` nas demais, com um aviso em log.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame com exatamente uma coluna (a concatenada).
    col0_name : str
        Nome da coluna única no DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame com 5 colunas: anomes, nu_apf, dt_prevista_conclusao,
        dt_prevista_inauguracao, situacao_obra.
    """
    series = df[col0_name].astype(str)
    extracted = series.str.extract(_POS_REGEX)
    extracted.columns = [
        "anomes",
        "nu_apf",
        "dt_prevista_conclusao",
        "dt_prevista_inauguracao",
        "situacao_obra",
    ]

    # Identificar linhas que não casaram
    na_mask = extracted["anomes"].isna()
    n_na = na_mask.sum()
    if n_na > 0:
        logger.warning(
            "%d linha(s) não casaram com o padrão posicional andamento_obra. "
            "Valor original preservado em 'anomes'.",
            n_na,
        )
        # Preservar o valor original na coluna anomes para linhas sem match
        extracted.loc[na_mask, "anomes"] = series[na_mask]

    logger.info(
        "Split posicional andamento_obra: %d linhas, %d sem match.",
        len(df),
        n_na,
    )
    return extracted


def gerar_mapeamento_gefus(
    df_pares: pd.DataFrame,
    output_path: str = "data/sftp/analise/mapeamento_gefus.csv",
) -> str:
    """Escreve CSV de mapeamento da canonicalização GEFUS.

    Parameters
    ----------
    df_pares : pd.DataFrame
        DataFrame produzido por :func:`eleger_canonicas_gefus`.
    output_path : str, default ``"data/sftp/analise/mapeamento_gefus.csv"``
        Caminho para o arquivo CSV de saída.

    Returns
    -------
    str
        Caminho absoluto do arquivo escrito.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df_pares.to_csv(path, index=False, quoting=csv.QUOTE_NONNUMERIC)

    logger.info(
        "Mapeamento GEFUS escrito em '%s' (%d linhas).",
        path.resolve(),
        len(df_pares),
    )
    return str(path.resolve())


def escrever_canonicas(
    df_canonicas: pd.DataFrame,
    engine: Engine,
    target_schema: str,
) -> None:
    """Write the canonicalization mapping as ``_canonicas`` table in target_schema.

    Parameters
    ----------
    df_canonicas : pd.DataFrame
        DataFrame from ``eleger_canonicas_gefus``.
    engine : sqlalchemy.engine.Engine
    target_schema : str
        Target schema name.
    """
    if df_canonicas.empty:
        logger.warning("df_canonicas vazio — _canonicas nao escrita.")
        return

    # Selecionar e renomear colunas
    cols_out = {
        "table_name": "table_name",
        "canonical_table": "canonical_table",
        "n_rows_orig": "n_rows_orig",
        "n_rows_canonical": "n_rows_canonical",
        "row_count_match": "row_count_match",
    }
    available = [k for k in cols_out if k in df_canonicas.columns]
    df_out = df_canonicas[available].copy()
    df_out.rename(columns={k: cols_out[k] for k in available}, inplace=True)

    # Adicionar coluna canonical_has_pipe (False por padrao; sera atualizado
    # se o pipeline detectar pipe no passo de copia)
    df_out["canonical_has_pipe"] = False

    df_out.to_sql(
        "_canonicas",
        con=engine,
        schema=target_schema,
        if_exists="replace",
        index=False,
    )
    logger.info(
        "_canonicas escrita em '%s': %d linhas.",
        target_schema,
        len(df_out),
    )


def escrever_classificacao_sftp(
    engine: Engine,
    results: list[dict],
    sftp_schema: str,
    target_schema: str,
    dump_equivalents: dict[str, str] | None = None,
    treated_at: str = "",
) -> None:
    """Write ``_classificacao`` table with one row per SFTP table.

    Covers ALL tables in the SFTP schema, not just treated ones.
    Uses ``results`` for treated tables and queries ``information_schema``
    for remaining (untreated) tables.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
    results : list[dict]
        Treatment results from ``main_sftp()``.
    sftp_schema : str
        Source SFTP schema name.
    target_schema : str
        Target schema name.
    dump_equivalents : dict, optional
        Mapping of SFTP table -> dump table for identical tables.
    treated_at : str
        ISO timestamp for the ``treated_at`` column.
    """
    import pandas as pd

    if dump_equivalents is None:
        dump_equivalents = {}

    # 1. Construir lookup rapido dos resultados
    treated: dict[str, dict] = {}
    for r in results:
        tname = str(r.get("table_name", ""))
        if tname:
            treated[tname] = r

    # 2. Listar todas as tabelas do schema SFTP
    stmt = text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = :schema AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    )
    with engine.connect() as conn:
        all_tables = [row[0] for row in conn.execute(stmt, {"schema": sftp_schema})]

    rows = []
    for t in all_tables:
        t_str = str(t)
        if t_str in treated:
            info = treated[t_str]
            nrows = int(info.get("n_rows", 0))
            status = str(info.get("status", ""))
            foi_copiada = nrows > 0 and status != "error"
            rows.append(
                {
                    "table_name": t_str,
                    "category": str(info.get("profile", "")),
                    "treatment": str(info.get("profile", "")),
                    "has_pipe": False,
                    "n_cols_original": int(info.get("n_cols", 0)),
                    "n_rows": nrows,
                    "destination_schema": target_schema if foi_copiada else "",
                    "destination_table": t_str if foi_copiada else "",
                    "dump_equivalent": dump_equivalents.get(t_str),
                    "treated_at": treated_at,
                }
            )
        else:
            # Tabela nao tratada — obter metadados basicos
            n_rows = 0
            n_cols = 0
            try:
                n_rows = _contar_linhas(engine, t_str, sftp_schema)
            except Exception:
                pass
            try:
                col_stmt = text(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = :tbl"
                )
                with engine.connect() as conn:
                    n_cols = (
                        conn.scalar(col_stmt, {"schema": sftp_schema, "tbl": t_str})
                        or 0
                    )
            except Exception:
                pass

            rows.append(
                {
                    "table_name": t_str,
                    "category": "",
                    "treatment": "nao_requer",
                    "has_pipe": False,
                    "n_cols_original": int(n_cols),
                    "n_rows": int(n_rows),
                    "destination_schema": "",
                    "destination_table": "",
                    "dump_equivalent": dump_equivalents.get(t_str),
                    "treated_at": treated_at,
                }
            )

    df = pd.DataFrame(rows)
    df.to_sql(
        "_classificacao",
        con=engine,
        schema=target_schema,
        if_exists="replace",
        index=False,
    )
    logger.info(
        "_classificacao escrita em '%s': %d linhas.",
        target_schema,
        len(df),
    )


# ═════════════════════════════════════════════════════════════════════════════
# Grupo 2: Pipe Padrão B
# ═════════════════════════════════════════════════════════════════════════════


def detectar_padrao_b(
    engine: Engine,
    df_canonicas: pd.DataFrame,
    schema: str = "sftp",
) -> list[str]:
    """Identifica tabelas Padrão B sem contraparte GEFUS.

    Critérios:
    1. A primeira coluna (``ordinal_position = 1``) tem nome com mais de 40
       caracteres **e** contém pelo menos um ``_``.
    2. A tabela **não** está coberta pela canonicalização GEFUS (nem como
       ``canonical_table`` nem como ``table_name`` em ``df_canonicas``).

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Conexão com o banco PostgreSQL.
    df_canonicas : pd.DataFrame
        DataFrame com as colunas ``table_name`` e ``canonical_table``,
        produzido por :func:`eleger_canonicas_gefus`.
    schema : str, default ``"sftp"``
        Schema onde as tabelas SFTP estão armazenadas.

    Returns
    -------
    list[str]
        Lista de nomes de tabelas candidatas ao tratamento Padrão B.
    """
    # 1. Construir conjunto de tabelas cobertas pela GEFUS.
    tabelas_cobertas: set[str] = set()
    if not df_canonicas.empty:
        if "canonical_table" in df_canonicas.columns:
            tabelas_cobertas.update(df_canonicas["canonical_table"].dropna().tolist())
        if "table_name" in df_canonicas.columns:
            tabelas_cobertas.update(df_canonicas["table_name"].dropna().tolist())

    # 2. Buscar tabelas com col0 de nome longo contendo underscore.
    stmt = text(
        "SELECT c.table_name, c.column_name "
        "FROM information_schema.columns c "
        "INNER JOIN ("
        "  SELECT table_name, MIN(ordinal_position) AS min_ord "
        "  FROM information_schema.columns "
        "  WHERE table_schema = :schema "
        "  GROUP BY table_name"
        ") first_col "
        "  ON c.table_name = first_col.table_name "
        "  AND c.ordinal_position = first_col.min_ord "
        "WHERE c.table_schema = :schema "
        "  AND LENGTH(c.column_name) > 40 "
        "  AND c.column_name LIKE '%_%' "
        "ORDER BY c.table_name"
    )

    candidatas: list[str] = []
    with engine.connect() as conn:
        rows = conn.execute(stmt, {"schema": schema})
        for table_name, _col_name in rows:
            if table_name not in tabelas_cobertas:
                candidatas.append(table_name)

    logger.info(
        "Detecção Padrão B: %d tabelas candidatas (de %d com col0 >40 chars).",
        len(candidatas),
        rows.rowcount if hasattr(rows, "rowcount") else 0,
    )
    return candidatas


def extrair_nomes_colunas(col0_name: str) -> list[str]:
    """Extrai nomes lógicos de colunas do header pipe-concatenado.

    Aplica heurística de split em fronteiras de snake_case:

    - **Família ``caixa_af_gehis_andamento_obra``:** se o nome começa com
      ``anomes_nu_apf_dt_prevista_conclusao``, retorna o mapeamento fixo
      de 5 campos.
    - **Demais famílias:** itera pelas partes separadas por ``_``; quando uma
      parte é um prefixo conhecido (``nu_``, ``dt_``, ``co_``, ``vr_``,
      ``no_``, ``sg_``, ``cd_``, ``vl_``), marca o início de um novo campo.

    Parameters
    ----------
    col0_name : str
        Nome da primeira coluna da tabela (header concatenado).

    Returns
    -------
    list[str]
        Lista de nomes de colunas inferidos.
    """
    if not col0_name:
        logger.warning("extrair_nomes_colunas: nome vazio.")
        return []

    # Detecção da família conhecida via prefixo.
    if col0_name.startswith(_PADRAO_VALIDACAO_PJ):
        logger.debug(
            "Família validacao_pj detectada em '%s...'. "
            "Usando mapeamento fixo de %d campos.",
            col0_name[:50],
            len(_COLUNAS_VALIDACAO_PJ),
        )
        return list(_COLUNAS_VALIDACAO_PJ)

    if col0_name.startswith(_PADRAO_VALIDACAO_PF):
        logger.debug(
            "Família validacao_pf detectada em '%s...'. "
            "Usando mapeamento fixo de %d campos.",
            col0_name[:50],
            len(_COLUNAS_VALIDACAO_PF),
        )
        return list(_COLUNAS_VALIDACAO_PF)

    if col0_name.startswith(_PADRAO_ANDAMENTO_OBRA):
        logger.debug(
            "Família andamento_obra detectada em '%s...'. "
            "Usando mapeamento fixo de %d campos.",
            col0_name[:50],
            len(_COLUNAS_ANDAMENTO_OBRA),
        )
        return list(_COLUNAS_ANDAMENTO_OBRA)

    # Detecção por marcador no meio do nome (header começa com _YYYYMM_...)
    if _PADRAO_OPERACAO_DESENQUADRADA in col0_name:
        logger.debug(
            "Família operacao_desenquadrada detectada em '%s...'. "
            "Usando mapeamento fixo de %d campos.",
            col0_name[:50],
            len(_COLUNAS_OPERACAO_DESENQUADRADA),
        )
        return list(_COLUNAS_OPERACAO_DESENQUADRADA)

    # Heurística geral: split por underscore, agrupando por prefixo.
    partes = col0_name.split("_")
    campos: list[str] = []
    buffer: list[str] = []

    for parte in partes:
        if not parte:
            continue

        if parte in _PREFIXOS_CONHECIDOS:
            # Prefixo conhecido: finaliza campo anterior (se houver) e
            # começa novo campo com este prefixo.
            if buffer:
                campos.append("_".join(buffer))
            buffer = [parte]
        else:
            buffer.append(parte)

    # Finalizar último campo
    if buffer:
        campos.append("_".join(buffer))

    if not campos:
        # Fallback: nome inteiro como campo único
        campos = [col0_name]

    # Validação: se menos de 50% dos campos começam com prefixo conhecido,
    # emitir warning (possível ambiguidade na extração).
    total = len(campos)
    com_prefixo = sum(1 for c in campos if c.split("_")[0] in _PREFIXOS_CONHECIDOS)
    if total > 1 and (com_prefixo / total) < 0.5:
        logger.warning(
            "Apenas %d/%d campos (%.0f%%) começam com prefixo conhecido "
            "em '%s'. Revisão manual recomendada.",
            com_prefixo,
            total,
            (com_prefixo / total) * 100,
            col0_name[:80],
        )

    logger.debug(
        "extrair_nomes_colunas: %d campos extraídos de '%s...'.",
        len(campos),
        col0_name[:50],
    )
    return campos


def split_pipe_col0(
    engine: Engine,
    table_name: str,
    col_names: list[str],
    schema: str = "sftp",
) -> pd.DataFrame:
    """Executa split da coluna 0 por ``|`` via PostgreSQL.

    Aplica ``regexp_split_to_array(col0, E'\\\\|')`` no PostgreSQL e retorna
    um DataFrame com as colunas splitadas, além de ``dt_ingest`` e
    ``arquivo_origem`` se existirem no schema.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Conexão com o banco PostgreSQL.
    table_name : str
        Nome da tabela no schema *schema*.
    col_names : list[str]
        Lista de nomes inferidos para as colunas resultantes do split.
    schema : str, default ``"sftp"``
        Schema onde a tabela reside.

    Returns
    -------
    pd.DataFrame
        DataFrame com as colunas splitadas + metadados preservados.
    """
    n_esperado = len(col_names)

    # 1. Descobrir o nome da coluna 0 e colunas de metadado.
    stmt_cols = text(
        "SELECT column_name "
        "FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table_name "
        "ORDER BY ordinal_position"
    )
    with engine.connect() as conn:
        colunas_info = conn.execute(
            stmt_cols, {"schema": schema, "table_name": table_name}
        )
        todas_colunas = [row[0] for row in colunas_info]

    if not todas_colunas:
        raise ValueError(f"Tabela '{schema}.{table_name}' não possui colunas.")

    col0 = todas_colunas[0]
    colunas_metadado = [
        c for c in todas_colunas if c.lower() in ("dt_ingest", "arquivo_origem")
    ]

    # 2. Construir query com regexp_split_to_array + indexação.
    #    Geramos uma coluna para cada índice do array.
    col_exprs: list[str] = []
    for i in range(n_esperado):
        idx_pg = i + 1  # PostgreSQL arrays são 1-based
        col_exprs.append(
            f'(regexp_split_to_array("{col0}", E\'\\\\|\'))[{idx_pg}] AS "col_{i}"'
        )

    select_list = ",\n      ".join(col_exprs)

    # Adicionar colunas de metadado (se existirem)
    if colunas_metadado:
        meta_select = ",\n      ".join(f'"{c}"' for c in colunas_metadado)
        select_list = f"{select_list},\n      {meta_select}"

    query = text(f'SELECT {select_list} FROM "{schema}"."{table_name}"')

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    # 3. Renomear col_0 … col_N para os nomes extraídos
    rename_map = {f"col_{i}": col_names[i] for i in range(n_esperado)}
    df.rename(columns=rename_map, inplace=True)

    logger.info(
        "Split pipe de '%s': %d linhas, %d colunas (+ %d metadado).",
        table_name,
        len(df),
        n_esperado,
        len(colunas_metadado),
    )
    return df


def tratar_tabela_padrao_b(
    engine: Engine,
    table_name: str,
    target_schema: str | None = None,
    source_schema: str = "sftp",
) -> dict[str, Any]:
    """Pipeline completo de tratamento de uma tabela Padrão B.

    Etapas:
    1. Extrair nomes de colunas do header concatenado (col0).
    2. Split da col0 por ``|`` no PostgreSQL.
    3. Validação de consistência (número de campos).
    4. Escrita no schema target.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Conexão com o banco PostgreSQL.
    table_name : str
        Nome da tabela no schema *source_schema*.
    target_schema : str, optional
        Schema de destino.  Padrão é o valor de
        :func:`get_schema_target`.
    source_schema : str, default ``"sftp"``
        Schema onde a tabela original reside.

    Returns
    -------
    dict[str, Any]
        Dicionário com status da operação:
        - ``table_name``: nome da tabela tratada.
        - ``status``: ``"ok"``, ``"erro"``, ou ``"ignorada"``.
        - ``n_rows``: número de linhas.
        - ``n_cols``: número de colunas.
        - ``warnings``: lista de avisos.
    """
    if target_schema is None:
        target_schema = get_schema_sftp_target()

    info: dict[str, Any] = {
        "table_name": table_name,
        "status": "ok",
        "n_rows": 0,
        "n_cols": 0,
        "warnings": [],
    }

    try:
        # 1. Descobrir nome da coluna 0 e extrair field names
        stmt = text(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table_name "
            "ORDER BY ordinal_position "
            "LIMIT 1"
        )
        with engine.connect() as conn:
            col0_name = conn.scalar(
                stmt, {"schema": source_schema, "table_name": table_name}
            )

        if not col0_name:
            info["status"] = "erro"
            info["warnings"].append("Coluna 0 não encontrada.")
            return info

        col_names = extrair_nomes_colunas(col0_name)
        if not col_names:
            info["status"] = "ignorada"
            info["warnings"].append(
                f"Não foi possível extrair nomes de colunas de '{col0_name}'."
            )
            return info

        n_esperado = len(col_names)

        # 2. Split da col0
        df = split_pipe_col0(engine, table_name, col_names, source_schema)

        if df.empty:
            info["status"] = "ignorada"
            info["warnings"].append("Tabela vazia após split.")
            return info

        info["n_rows"] = len(df)
        info["n_cols"] = len(df.columns)

        # 3. Validar consistência: detectar anomalias pipe
        anomalias = detectar_anomalias_pipe(df, n_esperado)
        if anomalias["linhas_divergentes"] > 0:
            info["warnings"].append(
                f"{anomalias['linhas_divergentes']} linha(s) com "
                f"número divergente de campos pipe "
                f"(esperado: {n_esperado})."
            )

        # 4. Escrever no schema target
        _escrever_tabela_tratada(df, table_name, engine, target_schema)

        logger.info(
            "Tabela Padrão B '%s' tratada: %d linhas, %d colunas, %d warning(s).",
            table_name,
            info["n_rows"],
            info["n_cols"],
            len(info["warnings"]),
        )

    except Exception as exc:
        logger.exception("Erro ao tratar tabela Padrão B '%s'.", table_name)
        info["status"] = "erro"
        info["warnings"].append(str(exc))

    return info


def detectar_anomalias_pipe(
    df: pd.DataFrame,
    n_esperado: int,
) -> dict[str, Any]:
    """Detecta linhas com número divergente de campos pipe.

    Conta valores nulos por linha nas colunas resultantes do split.
    Se uma linha tem todos os campos nulos ou menos campos que o esperado,
    é considerada divergente.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame resultante de :func:`split_pipe_col0` (já com nomes
        atribuídos). Assume-se que as primeiras *n_esperado* colunas são
        os campos pipe.
    n_esperado : int
        Número esperado de campos.

    Returns
    -------
    dict[str, Any]
        - ``total_linhas`` (int): total de linhas no DataFrame.
        - ``linhas_ok`` (int): linhas com todos os campos não-nulos.
        - ``linhas_divergentes`` (int): linhas com ao menos um campo nulo.
        - ``amostras_divergentes`` (list): até 5 exemplos de linhas com
          divergência (como dicts).
    """
    if df.empty:
        return {
            "total_linhas": 0,
            "linhas_ok": 0,
            "linhas_divergentes": 0,
            "amostras_divergentes": [],
        }

    # Considerar apenas as primeiras n_esperado colunas
    colunas_analise = df.columns[:n_esperado].tolist()
    subset = df[colunas_analise]

    # Uma linha é divergente se QUALQUER campo esperado for nulo
    nulos_por_linha = subset.isnull().sum(axis=1)
    divergente = nulos_por_linha > 0

    total = len(df)
    n_divergentes = int(divergente.sum())
    n_ok = total - n_divergentes

    # Amostras (até 5)
    amostras: list[dict[str, Any]] = []
    if n_divergentes > 0:
        indices_div = df.index[divergente].tolist()
        for idx in indices_div[:5]:
            linha = df.loc[idx].to_dict()
            linha["_indice"] = idx
            amostras.append(linha)

    return {
        "total_linhas": total,
        "linhas_ok": n_ok,
        "linhas_divergentes": n_divergentes,
        "amostras_divergentes": amostras,
    }


# ── Funções internas ────────────────────────────────────────────────────────


def _listar_tabelas_sftp(engine: Engine, schema: str) -> list[str]:
    """Lista todas as tabelas (BASE TABLE) no schema *schema*.

    Retorna lista ordenada alfabeticamente.
    """
    stmt = text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = :schema AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    )
    with engine.connect() as conn:
        result = conn.execute(stmt, {"schema": schema})
        return [row[0] for row in result]


def _contar_linhas(engine: Engine, table_name: str, schema: str) -> int:
    """Retorna ``COUNT(*)`` da tabela."""
    stmt = text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
    with engine.connect() as conn:
        count = conn.scalar(stmt)
    return count if count is not None else 0


def _escrever_tabela_tratada(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    target_schema: str,
) -> None:
    """Escreve DataFrame tratado no schema target.

    Preserva o nome original da tabela. Nomes de colunas são sanitizados
    automaticamente pelo ``to_sql`` do pandas.
    """
    # Sanitizar nome da tabela para PostgreSQL (max 63 chars, lowercase)
    import re as _re

    safe_name = _re.sub(r"[^a-zA-Z0-9_]", "_", table_name)
    safe_name = safe_name.lower()[:63]
    if safe_name and not _re.match(r"[a-z_]", safe_name[0]):
        safe_name = "_" + safe_name
    if not safe_name:
        safe_name = "_pipe_table"

    # Sanitizar colunas (truncar nomes muito longos)
    col_rename = {}
    for col in df.columns:
        col_str = str(col)
        if len(col_str) > 63:
            col_rename[col] = col_str[:60] + "..."
    if col_rename:
        df = df.rename(columns=col_rename)

    df.to_sql(
        name=safe_name,
        con=engine,
        schema=target_schema,
        if_exists="replace",
        index=False,
    )
    logger.debug(
        "Tabela tratada '%s.%s' escrita (%d linhas).",
        target_schema,
        safe_name,
        len(df),
    )


# ═════════════════════════════════════════════════════════════════════════════
# Grupo 3: Utilitários de detecção
# ═════════════════════════════════════════════════════════════════════════════


def _detectar_int055_sem_pipe(
    engine: Engine, table_name: str, schema: str = "sftp"
) -> bool:
    """Detecta tabelas ``int055`` cuja col0 não contém pipe e tem comprimento <= 2.

    Retorna ``True`` se mais de 90% das linhas têm col0 sem pipe e
    ``length(col0) <= 2``, indicando possivelmente dados ausentes ou
    placeholders.

    Parameters
    ----------
    engine : Engine
        Conexão com o banco PostgreSQL.
    table_name : str
        Nome da tabela no schema *schema*.
    schema : str, default ``"sftp"``
        Schema onde a tabela reside.

    Returns
    -------
    bool
        ``True`` se a maioria esmagadora das linhas (>90%) parecer vazia.
    """
    total = _contar_linhas(engine, table_name, schema)
    if total == 0:
        return False

    # Descobrir nome da primeira coluna (não é literalmente "col0")
    col_stmt = text(
        "SELECT column_name "
        "FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table_name "
        "ORDER BY ordinal_position "
        "LIMIT 1"
    )
    with engine.connect() as conn:
        col0_name = conn.scalar(col_stmt, {"schema": schema, "table_name": table_name})
    if not col0_name:
        return False

    stmt = text(
        f'SELECT COUNT(*) FROM "{schema}"."{table_name}" '
        f'WHERE "{col0_name}" NOT LIKE \'%|%\' AND LENGTH("{col0_name}") <= 2'
    )
    with engine.connect() as conn:
        sem_pipe_curtas = conn.scalar(stmt) or 0

    ratio = sem_pipe_curtas / total
    logger.debug(
        "int055 sem pipe: %d/%d linhas (%.1f%%) — limiar 90%% -> %s",
        sem_pipe_curtas,
        total,
        ratio * 100,
        "SIM" if ratio > 0.9 else "NÃO",
    )
    return ratio > 0.9
