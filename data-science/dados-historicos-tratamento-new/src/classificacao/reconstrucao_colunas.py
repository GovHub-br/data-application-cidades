"""Reconstrução de tabelas com colunas fragmentadas (Padrão A).

Tabelas no schema ``sftp`` onde o pipe ``|`` foi interpretado como
delimitador de coluna, fragmentando os dados originais em dezenas de
colunas estreitas com nomes truncados (ex: ``rigor_dt_m``, ``viment``).
Apenas 11–18 das 82–101 colunas contêm dados; o restante é ``NULL``.

Este módulo implementa a detecção, reconstrução e tratamento destas
tabelas (Padrão A), conforme especificação em
``openspec/changes/tratar-formacao-sftp/``.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Engine, text

logger = logging.getLogger(__name__)

__all__ = [
    "detectar_padrao_a",
    "reconstruir_linhas",
    "split_reconstruido",
    "atribuir_nomes_genericos",
    "enriquecer_nomes_tipos",
    "inferir_tipos_colunas",
    "gerar_referencia_colunas",
    "tratar_tabela_padrao_a",
    "estimar_truncamento",
]

# ── Constantes ──────────────────────────────────────────────────────────────

_LIMITE_AMOSTRA_DETECCAO = 1000  # linhas máximas para amostragem de detecção
_TAMANHO_AMOSTRA_TIPO = 100  # linhas para inferência de n_campos no split
_MAX_AMOSTRA_INFERENCIA_TIPO = 3  # valores de amostra no relatório de tipos
_CHARS_POR_COLUNA_ESPERADOS = 10.0  # heurística para estimativa de truncamento


# ── Funções públicas ────────────────────────────────────────────────────────


def detectar_padrao_a(
    engine: Engine,
    schema: str = "sftp",
    min_cols: int = 80,
    max_fill_pct: float = 0.25,
    df_canonicas: "pd.DataFrame | None" = None,
) -> list[str]:
    """Detecta tabelas no schema *schema* com colunas fragmentadas (Padrão A).

    A detecção ocorre em duas etapas:

    1. **Filtro estrutural:** consulta ``information_schema.columns`` para
       encontrar tabelas com mais de *min_cols* colunas.

    2. **Verificação de preenchimento:** para cada candidata, executa
       ``COUNT(*) FILTER (WHERE col IS NOT NULL)`` sobre uma amostra de
       até ``_LIMITE_AMOSTRA_DETECCAO`` linhas. Se a fração de colunas
       com ao menos um valor não-nulo for inferior a *max_fill_pct*, a
       tabela é classificada como Padrão A.

    Parameters
    ----------
    engine : Engine
        Conexão com o banco PostgreSQL.
    schema : str, optional
        Schema onde buscar as tabelas (padrão ``"sftp"``).
    min_cols : int, optional
        Número mínimo de colunas para considerar (padrão 80).
    max_fill_pct : float, optional
        Fração máxima de colunas com dados não-nulos (padrão 0.25).
        Tabelas com preenchimento abaixo deste valor são classificadas
        como Padrão A.

    Returns
    -------
    list[str]
        Lista de nomes de tabelas classificadas como Padrão A.
    """
    tabelas_padrao_a: list[str] = []

    # Etapa 1: encontrar tabelas com > min_cols colunas
    stmt_col_count = text(
        "SELECT table_name, COUNT(*) AS n_cols "
        "FROM information_schema.columns "
        "WHERE table_schema = :schema "
        "GROUP BY table_name "
        "HAVING COUNT(*) > :min_cols "
        "ORDER BY table_name"
    )

    with engine.connect() as conn:
        result = conn.execute(stmt_col_count, {"schema": schema, "min_cols": min_cols})
        candidatas: list[tuple[str, int]] = [(row[0], row[1]) for row in result]

    if not candidatas:
        logger.info(
            "Nenhuma tabela com mais de %d colunas encontrada no schema '%s'.",
            min_cols,
            schema,
        )
        return tabelas_padrao_a

    logger.info(
        "Encontradas %d tabela(s) candidata(s) com >%d colunas no schema '%s'.",
        len(candidatas),
        min_cols,
        schema,
    )

    # Etapa 2: para cada candidata, verificar % de colunas com dados
    for table_name, n_cols in candidatas:
        pct = _calcular_pct_preenchimento(engine, table_name, schema)

        if pct is None:
            logger.warning(
                "Não foi possível calcular preenchimento para '%s'. Ignorando.",
                table_name,
            )
            continue

        if pct < max_fill_pct:
            tabelas_padrao_a.append(table_name)
            logger.info(
                "Tabela '%s' detectada como Padrão A: "
                "%d colunas, apenas %.1f%% com dados (threshold < %.0f%%).",
                table_name,
                n_cols,
                pct * 100,
                max_fill_pct * 100,
            )
        else:
            logger.debug(
                "Tabela '%s' NÃO é Padrão A: %.1f%% das colunas têm dados "
                "(threshold >= %.0f%%).",
                table_name,
                pct * 100,
                max_fill_pct * 100,
            )

    if tabelas_padrao_a:
        logger.info(
            "Detectadas %d tabela(s) Padrão A: %s.",
            len(tabelas_padrao_a),
            tabelas_padrao_a,
        )
    else:
        logger.info("Nenhuma tabela Padrão A detectada no schema '%s'.", schema)

    # Excluir tabelas cobertas pela canonicalizacao GEFUS.
    if df_canonicas is not None and not df_canonicas.empty:
        tabelas_cobertas: set[str] = set()
        if "table_name" in df_canonicas.columns:
            tabelas_cobertas.update(df_canonicas["table_name"].dropna().tolist())
        if "canonical_table" in df_canonicas.columns:
            # Normalizar (remover prefixo gefus_) para comparar
            from .tratamento_sftp import normalizar_nome_sftp

            for t in df_canonicas["canonical_table"].dropna():
                tabelas_cobertas.add(normalizar_nome_sftp(str(t)))

        antes = len(tabelas_padrao_a)
        tabelas_padrao_a = [t for t in tabelas_padrao_a if t not in tabelas_cobertas]
        logger.info(
            "Padrao A: %d tabelas ignoradas por cobertura GEFUS (%d restantes).",
            antes - len(tabelas_padrao_a),
            len(tabelas_padrao_a),
        )
    elif df_canonicas is not None and df_canonicas.empty:
        logger.warning(
            "Padrao A: df_canonicas vazio — processando todas as tabelas "
            "como fallback (canonicalizacao pode ter falhado)."
        )

    return tabelas_padrao_a


def reconstruir_linhas(
    engine: Engine,
    table_name: str,
    schema: str = "sftp",
) -> pd.DataFrame:
    """Reconstrói registros pipe-delimitados originais de uma tabela fragmentada.

    Gera dinamicamente uma query SQL que concatena todas as colunas
    não-nulas de cada linha com ``|`` como separador:

    .. code-block:: sql

        SELECT array_to_string(array_remove(ARRAY["col1", "col2", ...], ''), '|')
        AS reconstructed
        FROM "schema"."table"

    A ordem das colunas é preservada conforme ``ordinal_position``.

    Parameters
    ----------
    engine : Engine
        Conexão com o banco PostgreSQL.
    table_name : str
        Nome da tabela a reconstruir.
    schema : str, optional
        Schema onde a tabela se encontra (padrão ``"sftp"``).

    Returns
    -------
    pd.DataFrame
        DataFrame com uma única coluna ``reconstructed`` contendo as
        strings pipe-delimitadas reconstruídas.

    Raises
    ------
    ValueError
        Se a tabela não existir ou não possuir colunas.
    """
    # 1. Obter colunas em ordem ordinal
    colunas = _listar_colunas(engine, table_name, schema)
    if not colunas:
        raise ValueError(
            f"Tabela '{table_name}' no schema '{schema}' não possui colunas."
        )

    # 2. Montar ARRAY com colunas devidamente quoteadas
    quoted_cols = [_quote_ident(c) for c in colunas]
    array_expr = f"ARRAY[{', '.join(quoted_cols)}]"

    # 3. Query de reconstrução
    query = text(
        f"SELECT array_to_string(array_remove({array_expr}, ''), '|') "
        f"AS reconstructed "
        f"FROM {_quote_ident(schema)}.{_quote_ident(table_name)}"
    )

    df = pd.read_sql_query(query, engine)

    logger.info(
        "Tabela '%s' reconstruída: %d linhas.",
        table_name,
        len(df),
    )

    return df


def split_reconstruido(
    df_reconstructed: pd.DataFrame,
    n_campos: int | None = None,
) -> pd.DataFrame:
    """Aplica split por ``|`` sobre a coluna ``reconstructed``.

    Se *n_campos* for ``None``, o número de campos é inferido a partir
    do máximo de campos pipe encontrados nas primeiras
    ``_TAMANHO_AMOSTRA_TIPO`` linhas.

    Linhas com mais campos que *n_campos* são truncadas (com warning).
    Linhas com menos campos recebem ``None`` nas colunas excedentes.

    Parameters
    ----------
    df_reconstructed : pd.DataFrame
        DataFrame com a coluna ``reconstructed`` (gerado por
        :func:`reconstruir_linhas`).
    n_campos : int, optional
        Número desejado de colunas de saída. Se ``None``, é inferido
        automaticamente.

    Returns
    -------
    pd.DataFrame
        DataFrame com colunas ``col_0``, ``col_1``, ..., ``col_N``.
        Linhas sem dados válidos são removidas.

    Raises
    ------
    ValueError
        Se o DataFrame de entrada não contiver a coluna ``reconstructed``.
    """
    if "reconstructed" not in df_reconstructed.columns:
        raise ValueError("DataFrame de entrada deve conter a coluna 'reconstructed'.")

    # Remover linhas onde reconstructed é NULL ou vazio
    mask = df_reconstructed["reconstructed"].notna() & (
        df_reconstructed["reconstructed"] != ""
    )
    df_valid = df_reconstructed[mask].copy()

    if df_valid.empty:
        logger.warning("Nenhuma linha com dados válidos para split.")
        return pd.DataFrame()

    # Inferir n_campos se não fornecido
    if n_campos is None:
        amostra = df_valid["reconstructed"].head(_TAMANHO_AMOSTRA_TIPO)
        if len(amostra) > 0:
            n_campos = int(amostra.str.count(r"\|").max()) + 1
        else:
            n_campos = 1
        logger.info("Número de campos inferido da amostra: %d.", n_campos)

    # Split
    expandido = df_valid["reconstructed"].str.split("|", expand=True)

    # Truncar se exceder n_campos
    if expandido.shape[1] > n_campos:
        logger.warning(
            "Split gerou %d colunas, truncando para %d. "
            "Algumas linhas têm campos excedentes que serão perdidos.",
            expandido.shape[1],
            n_campos,
        )
        expandido = expandido.iloc[:, :n_campos]
    elif expandido.shape[1] < n_campos:
        # Completar com colunas vazias
        for i in range(expandido.shape[1], n_campos):
            expandido[i] = None

    # Nomear colunas
    expandido.columns = atribuir_nomes_genericos(n_campos)

    logger.info(
        "Split concluído: %d linhas, %d colunas.",
        len(expandido),
        n_campos,
    )

    return expandido


def atribuir_nomes_genericos(n_campos: int) -> list[str]:
    """Gera uma lista de nomes genéricos para colunas reconstruídas.

    Os nomes originais das colunas foram perdidos no truncamento
    causado pela ingestão do pipe como delimitador. Usamos nomes
    genéricos ``col_0``, ``col_1``, ..., ``col_{n_campos-1}``.

    Parameters
    ----------
    n_campos : int
        Número de colunas.

    Returns
    -------
    list[str]
        Lista com ``n_campos`` nomes de coluna.
    """
    return [f"col_{i}" for i in range(n_campos)]


def enriquecer_nomes_tipos(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Enriquece nomes de colunas com prefixo de tipo baseado em arquivo de referência.

    Procura por ``data/sftp/analise/referencia_colunas_<prefixo>*.csv`` onde
    o prefixo é extraído do nome da tabela (ex: ``int040_`` de
    ``int040_ministeriocidades_far_caixa_empreendimentos_20231228``).

    Se encontrado, renomeia ``col_N`` para ``tipo_N`` usando o campo
    ``tipo_inferido``:

    - ``date`` → ``data_N``
    - ``number`` → ``numero_N``
    - ``text`` → ``texto_N``
    - outros → mantém ``col_N``

    Se o arquivo não existir, retorna o DataFrame inalterado (*fallback*).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame com colunas nomeadas ``col_0``, ``col_1``, …
    table_name : str
        Nome da tabela (usado para extrair o prefixo).

    Returns
    -------
    pd.DataFrame
        DataFrame com colunas renomeadas ou inalterado se não houver
        arquivo de referência.
    """
    # 1. Extrair prefixo do nome da tabela
    match = re.match(r"^(int\d{3}_)", table_name)
    if not match:
        logger.debug(
            "Nome '%s' não possui prefixo 'intNNN_'. Pulando enriquecimento.",
            table_name,
        )
        return df

    prefix = match.group(1)

    # 2. Procurar arquivo de referência
    ref_dir = Path("data/sftp/analise")
    if not ref_dir.is_dir():
        logger.warning(
            "Diretório de referência não encontrado: '%s'. Pulando enriquecimento.",
            ref_dir,
        )
        return df

    files = sorted(ref_dir.glob(f"referencia_colunas_{prefix}*.csv"))
    if not files:
        logger.debug(
            "Arquivo de referência não encontrado para prefixo '%s'. "
            "Pulando enriquecimento.",
            prefix,
        )
        return df

    ref_path = files[0]

    # 3. Ler CSV de referência
    try:
        df_ref = pd.read_csv(ref_path)
    except Exception as exc:
        logger.warning(
            "Erro ao ler arquivo de referência '%s': %s. Pulando enriquecimento.",
            ref_path,
            exc,
        )
        return df

    # 4. Validar colunas necessárias
    required_cols: set[str] = {"coluna", "tipo_inferido"}
    if not required_cols.issubset(df_ref.columns):
        logger.warning(
            "Arquivo de referência '%s' não contém as colunas necessárias: %s. "
            "Pulando enriquecimento.",
            ref_path,
            required_cols - set(df_ref.columns),
        )
        return df

    # 5. Construir mapeamento de renomeio
    tipo_prefix: dict[str, str] = {
        "date": "data",
        "number": "numero",
        "text": "texto",
    }

    rename_map: dict[str, str] = {}
    for _, row in df_ref.iterrows():
        coluna = str(row.get("coluna", ""))
        tipo = str(row.get("tipo_inferido", "")).strip().lower()

        # Pular se a coluna não existe no DataFrame atual
        if coluna not in df.columns:
            continue

        # Extrair índice numérico de col_N
        m = re.match(r"^col_(\d+)$", coluna)
        if not m:
            continue

        idx = m.group(1)
        prefixo_tipo = tipo_prefix.get(tipo)

        if prefixo_tipo is not None:
            rename_map[coluna] = f"{prefixo_tipo}_{idx}"
        # else: mantém col_N (não adiciona ao mapa)

    if not rename_map:
        logger.debug(
            "Nenhuma coluna para renomear na tabela '%s' via referência '%s'.",
            table_name,
            ref_path.name,
        )
        return df

    # 6. Aplicar renomeio
    logger.info(
        "Renomeadas %d colunas da tabela '%s' usando referência '%s': %s.",
        len(rename_map),
        table_name,
        ref_path.name,
        {k: v for k, v in rename_map.items() if k != v},
    )

    return df.rename(columns=rename_map)


def inferir_tipos_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Infere o tipo de dados de cada coluna reconstruída.

    Para cada coluna, analisa uma amostra de até
    ``_MAX_AMOSTRA_INFERENCIA_TIPO`` valores não-nulos e classifica o
    tipo como:

    - ``date`` — a maioria dos valores parece data
    - ``number`` — a maioria dos valores parece número
    - ``text`` — fallback (inclui colunas totalmente vazias)

    Também coleta até 3 valores de amostra para inspeção manual.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame com colunas nomeadas (gerado por :func:`split_reconstruido`).

    Returns
    -------
    pd.DataFrame
        DataFrame com as colunas:
        - ``coluna``: nome da coluna
        - ``tipo_inferido``: ``date``, ``number``, ou ``text``
        - ``amostra_1``, ``amostra_2``, ``amostra_3``: valores de amostra
    """
    registros: list[dict[str, Any]] = []

    for col in df.columns:
        valores = df[col].dropna().head(_MAX_AMOSTRA_INFERENCIA_TIPO).tolist()
        amostra_1 = str(valores[0]) if len(valores) > 0 else ""
        amostra_2 = str(valores[1]) if len(valores) > 1 else ""
        amostra_3 = str(valores[2]) if len(valores) > 2 else ""

        tipo = _inferir_tipo_coluna(valores)

        registros.append(
            {
                "coluna": col,
                "tipo_inferido": tipo,
                "amostra_1": amostra_1,
                "amostra_2": amostra_2,
                "amostra_3": amostra_3,
            }
        )

    df_tipos = pd.DataFrame(registros)
    logger.info("Tipos inferidos para %d colunas.", len(df_tipos))
    return df_tipos


def gerar_referencia_colunas(
    df_tipos: pd.DataFrame,
    table_name: str,
    output_dir: str = "data/sftp/analise",
) -> str:
    """Gera um arquivo CSV com a referência de colunas para a tabela.

    O arquivo contém, para cada coluna reconstruída, o tipo inferido
    e uma amostra de valores, auxiliando na identificação manual
    posterior dos campos.

    Nome do arquivo: ``referencia_colunas_{table_name}.csv``

    Parameters
    ----------
    df_tipos : pd.DataFrame
        DataFrame com tipos inferidos (gerado por
        :func:`inferir_tipos_colunas`).
    table_name : str
        Nome da tabela (usado no nome do arquivo).
    output_dir : str, optional
        Diretório de saída (padrão ``"data/sftp/analise"``).

    Returns
    -------
    str
        Caminho absoluto do arquivo gerado.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / f"referencia_colunas_{table_name}.csv"
    df_tipos.to_csv(file_path, index=False)

    logger.info("Referência de colunas salva em: %s", file_path)
    return str(file_path.resolve())


def tratar_tabela_padrao_a(
    engine: Engine,
    table_name: str,
    target_schema: str,
    source_schema: str = "sftp",
) -> dict[str, Any]:
    """Executa o pipeline completo de reconstrução Padrão A.

    Etapas executadas:
    1. Obtém métricas da tabela original (nº colunas, % preenchimento)
    2. Reconstrói linhas via ``array_to_string`` no PostgreSQL
    3. Aplica split por ``|`` com inferência de número de campos
    4. Atribui nomes genéricos ``col_0``…``col_N``
    5. Infere tipos das colunas a partir de amostras
    6. Gera arquivo de referência de colunas (CSV)
    7. Estima perda por truncamento
    8. Escreve a tabela tratada no schema target

    Parameters
    ----------
    engine : Engine
        Conexão com o banco PostgreSQL.
    table_name : str
        Nome da tabela a tratar.
    target_schema : str
        Schema de destino onde a tabela tratada será escrita.
    source_schema : str, optional
        Schema de origem (padrão ``"sftp"``).

    Returns
    -------
    dict
        Dicionário com métricas da operação:

        - ``status``: ``"ok"`` ou ``"erro"``
        - ``table_name``: nome da tabela
        - ``n_rows``: número de linhas resultantes
        - ``n_cols``: número de colunas resultantes
        - ``n_cols_originais``: número de colunas na tabela original
        - ``pct_preenchimento``: fração de colunas originais com dados (0–1)
        - ``truncation_estimate``: estimativa de perda por truncamento (0–1)
        - ``error``: mensagem de erro (presente apenas se ``status == "erro"``)
    """
    info: dict[str, Any] = {
        "table_name": table_name,
        "status": "ok",
        "n_rows": 0,
        "n_cols": 0,
        "n_cols_originais": 0,
        "pct_preenchimento": 0.0,
        "truncation_estimate": 0.0,
    }

    try:
        # 1. Obter contagem de colunas originais
        col_count_stmt = text(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table_name"
        )
        with engine.connect() as conn:
            n_cols_originais = (
                conn.scalar(
                    col_count_stmt,
                    {"schema": source_schema, "table_name": table_name},
                )
                or 0
            )
        info["n_cols_originais"] = n_cols_originais

        # 2. Calcular % de preenchimento
        pct = _calcular_pct_preenchimento(engine, table_name, source_schema)
        info["pct_preenchimento"] = round(pct, 4) if pct is not None else 0.0

        # 3. Reconstruir linhas via concatenação SQL
        df_reconstructed = reconstruir_linhas(engine, table_name, schema=source_schema)

        if df_reconstructed.empty:
            logger.warning(
                "Tabela '%s' reconstruída vazia. Nenhuma linha para processar.",
                table_name,
            )
            info["status"] = "ok"
            return info

        # 4. Split por pipe
        df_split = split_reconstruido(df_reconstructed)

        if df_split.empty:
            logger.warning("Split da tabela '%s' resultou vazio.", table_name)
            return info

        info["n_rows"] = len(df_split)
        info["n_cols"] = len(df_split.columns)

        # 4b. Enriquece nomes de colunas com prefixo de tipo via referência
        df_split = enriquecer_nomes_tipos(df_split, table_name)

        # 5. Inferir tipos e gerar referência de colunas
        df_tipos = inferir_tipos_colunas(df_split)
        gerar_referencia_colunas(df_tipos, table_name)

        # 6. Estimar truncamento
        n_cols_com_dados = max(1, round(n_cols_originais * (pct or 0)))
        total_chars = int(
            df_reconstructed["reconstructed"].dropna().astype(str).str.len().sum()
        )
        estimate = estimar_truncamento(n_cols_com_dados, total_chars)
        info["truncation_estimate"] = round(estimate, 4)

        # 7. Escrever no schema target
        _escrever_tabela_target(df_split, table_name, engine, target_schema)

        logger.info(
            "Tabela Padrão A '%s' tratada com sucesso: "
            "%d linhas, %d colunas → schema '%s'.",
            table_name,
            info["n_rows"],
            info["n_cols"],
            target_schema,
        )

    except Exception as exc:
        logger.exception(
            "Erro ao tratar tabela Padrão A '%s': %s",
            table_name,
            exc,
        )
        info["status"] = "erro"
        info["error"] = str(exc)

    return info


def estimar_truncamento(
    n_cols_com_dados: int,
    total_chars_reconstructed: int,
) -> float:
    """Estima a perda por truncamento na reconstrução Padrão A.

    A estimativa usa uma heurística que assume um comprimento médio
    mínimo esperado de ``_CHARS_POR_COLUNA_ESPERADOS`` caracteres por
    coluna com dados. Se o total de caracteres reconstruídos for
    inferior ao esperado, a diferença é reportada como fração de perda.

    Esta é uma **estimativa conservadora**. O truncamento real só
    poderia ser medido comparando com os dados originais pré-ingestão.

    Parameters
    ----------
    n_cols_com_dados : int
        Número de colunas originais que continham dados não-nulos.
    total_chars_reconstructed : int
        Total de caracteres na string reconstruída (soma sobre todas
        as linhas).

    Returns
    -------
    float
        Fração estimada de perda, entre 0.0 (sem perda) e 1.0 (perda
        total). Retorna ``1.0`` se não houver dados para comparar.
    """
    if n_cols_com_dados <= 0 or total_chars_reconstructed <= 0:
        return 1.0

    total_esperado = n_cols_com_dados * _CHARS_POR_COLUNA_ESPERADOS

    if total_chars_reconstructed >= total_esperado:
        return 0.0

    perda = 1.0 - (total_chars_reconstructed / total_esperado)
    return max(0.0, min(1.0, perda))


# ── Funções internas ────────────────────────────────────────────────────────


def _quote_ident(name: str) -> str:
    """Envolve um identificador em aspas duplas, escapando aspas internas.

    Segue a regra do PostgreSQL: aspas duplas internas são duplicadas.
    """
    # Usar variável intermediária para evitar backslash em f-string (inválido
    # em Python < 3.12 dentro de expressões de f-string).
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _listar_colunas(
    engine: Engine,
    table_name: str,
    schema: str,
) -> list[str]:
    """Retorna a lista de nomes de colunas de uma tabela, ordenadas."""
    stmt = text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table_name "
        "ORDER BY ordinal_position"
    )
    with engine.connect() as conn:
        return [
            row[0]
            for row in conn.execute(stmt, {"schema": schema, "table_name": table_name})
        ]


def _calcular_pct_preenchimento(
    engine: Engine,
    table_name: str,
    schema: str,
) -> float | None:
    """Calcula a fração de colunas com ao menos um valor não-nulo.

    Executa ``COUNT(*) FILTER (WHERE col IS NOT NULL)`` para cada
    coluna em uma amostra de até ``_LIMITE_AMOSTRA_DETECCAO`` linhas.
    Uma coluna é considerada "com dados" se a contagem for > 0.

    Returns
    -------
    float | None
        Fração de colunas com dados (0–1), ou ``None`` se não foi
        possível determinar.
    """
    colunas = _listar_colunas(engine, table_name, schema)
    if not colunas:
        logger.warning("Tabela '%s' não possui colunas.", table_name)
        return None

    clauses = []
    for col in colunas:
        quoted = _quote_ident(col)
        clauses.append(f"COUNT(*) FILTER (WHERE {quoted} IS NOT NULL)")

    # Amostra limitada para performance
    query = text(
        f"SELECT {', '.join(clauses)} "
        f"FROM (SELECT * FROM {_quote_ident(schema)}.{_quote_ident(table_name)} "
        f"LIMIT {_LIMITE_AMOSTRA_DETECCAO}) sub"
    )

    with engine.connect() as conn:
        row = conn.execute(query).fetchone()

    if row is None:
        return None

    colunas_com_dados = sum(1 for v in row if v is not None and v > 0)
    return colunas_com_dados / len(colunas) if colunas else 0.0


def _inferir_tipo_coluna(valores: list[Any]) -> str:
    """Infere o tipo de dados de uma lista de valores.

    Estratégia:
    1. Tenta interpretar cada valor como data (formatos ISO, BR, etc.)
    2. Se não, tenta como número (inteiro ou decimal)
    3. Se a maioria for data → ``"date"``
    4. Se a maioria for número → ``"number"``
    5. Caso contrário → ``"text"``

    Parameters
    ----------
    valores : list[Any]
        Amostra de valores não-nulos da coluna.

    Returns
    -------
    str
        ``"date"``, ``"number"``, ou ``"text"``.
    """
    if not valores:
        return "text"

    datas = 0
    numeros = 0
    total = 0

    for v in valores:
        if v is None or (isinstance(v, str) and v.strip() == ""):
            continue
        total += 1
        s = str(v).strip()

        if _parece_data(s):
            datas += 1
        elif _parece_numero(s):
            numeros += 1

    if total == 0:
        return "text"

    # Se pelo menos metade parece data
    if datas >= total / 2:
        return "date"
    # Se pelo menos metade parece número
    if numeros >= total / 2:
        return "number"
    return "text"


def _parece_data(valor: str) -> bool:
    """Verifica se a string se parece com uma data em formato comum.

    Formatos reconhecidos:
    - ISO 8601: ``YYYY-MM-DD``
    - Brasileiro: ``DD/MM/YYYY``
    - Americano: ``MM/DD/YYYY`` ou ``YYYY/MM/DD``
    - Compacto: ``YYYYMMDD``
    - Hífen: ``DD-MM-YYYY``
    """
    # Apenas verificar padrões; não faz parsing completo
    if re.match(r"^\d{4}-\d{2}-\d{2}$", valor):
        return True
    if re.match(r"^\d{2}/\d{2}/\d{4}$", valor):
        return True
    if re.match(r"^\d{4}/\d{2}/\d{2}$", valor):
        return True
    if re.match(r"^\d{8}$", valor):
        return True
    if re.match(r"^\d{2}-\d{2}-\d{4}$", valor):
        return True
    return False


def _parece_numero(valor: str) -> bool:
    """Verifica se a string se parece com um número (int ou decimal).

    Aceita separador decimal ``,`` ou ``.`` e sinal negativo opcional.
    """
    return bool(re.match(r"^-?\d+(?:[.,]\d+)?$", valor))


def _escrever_tabela_target(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    target_schema: str,
) -> None:
    """Escreve o DataFrame tratado no schema target.

    Usa ``pandas.DataFrame.to_sql`` com ``if_exists="replace"``.
    O nome da tabela é sanitizado via :func:`sanitize_table_name`.
    """
    # Import relativo para consistência com o restante do pacote
    from .db.writer import sanitize_table_name

    sanitized = sanitize_table_name(table_name)

    df.to_sql(
        name=sanitized,
        con=engine,
        schema=target_schema,
        if_exists="replace",
        index=False,
    )

    logger.info(
        "Tabela '%s.%s' escrita (%d linhas, %d colunas).",
        target_schema,
        sanitized,
        len(df),
        len(df.columns),
    )
