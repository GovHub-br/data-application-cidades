"""Orquestração da classificação por formação.

* ``classificar_todas`` — itera sobre as amostras de ``data/table_samples/``,
  aplica ``classificar_formacao`` e coleta resultados em um ``DataFrame``.
* ``comparar_referencia`` — compara com a classificação manual em
  ``categorias_classificacao.md`` e sinaliza divergências (``confidence=low``).
* ``sumarizar`` — contagem por categoria (para relatório de divergências).
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .carregamento import _DATA_DIR, carregar_amostra
from .profiling import profile_estrutural
from .regras import classificar_formacao

# Colunas da tabela de classificação
COLUNAS = ["table_name", "formacao", "confidence", "notes"]

# Arquivo de referência autoritativa (raiz do projeto) — spec.
_AUTORITATIVO_DEFAULT = (
    _DATA_DIR.parent / "classificacao_formacao_revisado_autoritativo.md"
)

# Arquivo de referência operacional antigo (mantido para legado).
_OPERACIONAL_DEFAULT = _DATA_DIR.parent / "categorias_classificacao.md"


def _parse_referencia_autoritativa(path: Path) -> dict[str, str]:
    """Extrai ``{table_name: category}`` do doc autoritativo.

    O formato é ``### \`categoria\`` seguido de texto descritivo e uma seção
    ``**Exemplos:**`` com nomes de tabela entre backticks. Tabelas de exemplo
    também podem aparecer no texto descritivo — qualquer token entre backticks
    que contenha dígitos é considerado nome de tabela.
    """
    text = path.read_text(encoding="utf-8")
    exemplos: dict[str, str] = {}
    # Divide por "### `categoria`" — nomes podem conter dígitos (ex.: sub_tabelas_1).
    secoes = re.split(r"^###\s+`([a-z0-9_|]+)`", text, flags=re.MULTILINE)
    for i in range(1, len(secoes), 2):
        categoria = secoes[i]
        corpo = secoes[i + 1]
        # Coleta todos os tokens entre backticks que parecem nomes de tabela.
        for token in re.findall(r"`([^`]+)`", corpo):
            # Descarta prefixo de schema (ex.: "dados_historicos.bb_...").
            nome = token.rsplit(".", 1)[-1]
            # Nomes de tabela contêm dígitos (datas como "2012_01").
            if any(ch.isdigit() for ch in nome):
                exemplos[nome] = categoria
    return exemplos


def _parse_referencia(
    path: Path,
) -> tuple[dict[str, str], dict[str, int]]:
    """Extrai de ``categorias_classificacao.md``:

    * ``{table_name: category}`` — exemplos listados sob cada categoria.
    * ``{category: count}`` — contagem declarada no cabeçalho da seção.
    """
    text = path.read_text(encoding="utf-8")
    exemplos: dict[str, str] = {}
    contagens: dict[str, int] = {}
    # Divide por seções "### `categoria`" (nomes podem conter dígitos, ex.:
    # nao_colunares_tipo1).
    secoes = re.split(r"^###\s+`([a-z0-9_]+)`", text, flags=re.MULTILINE)
    # secoes[0] é o pré-texto; depois pares (categoria, corpo)
    for i in range(1, len(secoes), 2):
        categoria = secoes[i]
        corpo = secoes[i + 1]
        # Contagem no cabeçalho: "(N tabelas — X%)" ou "(1 tabela — 0.1%)"
        m = re.search(r"\((\d+)\s+tabelas?", corpo)
        if m:
            contagens[categoria] = int(m.group(1))
        # Tokens em backticks que contêm dígitos = nomes de tabelas exemplo
        # (nomes de categorias não contêm dígitos).
        for token in re.findall(r"`([^`]+)`", corpo):
            nome = token.split(".")[
                -1
            ]  # descarta prefixo de schema "dados_historicos."
            if any(ch.isdigit() for ch in nome):
                exemplos[nome] = categoria
    return exemplos, contagens


def classificar_todas(data_dir: Path | str | None = None) -> pd.DataFrame:
    """Classifica todas as amostras de ``data/table_samples/``.

    Retorna um ``DataFrame`` com colunas ``table_name, formacao, confidence,
    notes``, ordenado por ``table_name`` (reproduzível). Tabelas com erro de
    leitura recebem ``confidence=low`` e nota explicativa.
    """
    base = Path(data_dir) if data_dir is not None else _DATA_DIR
    amostras = sorted((base / "table_samples").glob("*.csv"))
    registros: list[dict[str, str]] = []
    for path in amostras:
        table_name = path.stem
        try:
            df = carregar_amostra(table_name, data_dir=base)
            profile = profile_estrutural(df, file_size=path.stat().st_size)
            formacao, confidence, notes = classificar_formacao(table_name, df, profile)
        except Exception as exc:  # noqa: BLE001
            formacao, confidence, notes = "indeterminada", "low", f"erro: {exc}"
        registros.append(
            {
                "table_name": table_name,
                "formacao": formacao,
                "confidence": confidence,
                "notes": notes,
            }
        )
    return pd.DataFrame(registros, columns=COLUNAS)


def comparar_referencia(
    df_resultados: pd.DataFrame,
    path_referencia: Path | str | None = None,
) -> pd.DataFrame:
    """Compara com a referência autoritativa e sinaliza divergências.

    Para cada tabela listada como exemplo em
    ``classificacao_formacao_revisado_autoritativo.md`` cuja classificação
    automática difere da manual, atribui ``confidence=low`` e ``notes``
    indicando a divergência. Tabelas concordantes mantêm o nível de confiança
    determinado pelas regras (não há elevação).
    """
    path = Path(path_referencia) if path_referencia else _AUTORITATIVO_DEFAULT
    exemplos = _parse_referencia_autoritativa(path)
    df = df_resultados.copy()
    for idx, row in df.iterrows():
        table_name = str(row["table_name"])
        formacao = str(row["formacao"])
        ref = exemplos.get(table_name)
        if ref is not None and formacao != ref:
            nota = f"diverge de referência autoritativa: era {ref}"
            existente = str(row["notes"])
            df.at[idx, "notes"] = f"{existente}; {nota}" if existente else nota
            df.at[idx, "confidence"] = "low"
    return df


def sumarizar(df_resultados: pd.DataFrame) -> dict[str, int]:
    """Contagem de tabelas por categoria."""
    contagem = df_resultados["formacao"].value_counts().sort_index()
    return {str(k): int(v) for k, v in contagem.items()}


def gerar_relatorio_divergencias(
    df_resultados: pd.DataFrame,
    path_referencia: Path | str | None = None,
) -> str:
    """Gera um relatório textual de divergências contra a referência autoritativa.

    Inclui: concordância nos exemplos listados em
    ``classificacao_formacao_revisado_autoritativo.md``, divergências por tabela,
    e comparação de contagens por categoria quando disponível no arquivo
    operacional ``categorias_classificacao.md``.
    """
    path = Path(path_referencia) if path_referencia else _AUTORITATIVO_DEFAULT
    exemplos = _parse_referencia_autoritativa(path)
    # Tenta extrair contagens do arquivo operacional (se existir).
    contagens_ref: dict[str, int] = {}
    if _OPERACIONAL_DEFAULT.exists():
        _, contagens_ref = _parse_referencia(_OPERACIONAL_DEFAULT)
    linhas: list[str] = ["# Relatório de Divergências", ""]

    # Concordância nos exemplos
    tabelas_ours = set(df_resultados["table_name"].astype(str))
    concord = 0
    divergencias: list[str] = []
    for tabela, ref in exemplos.items():
        if tabela not in tabelas_ours:
            continue
        ours = str(
            df_resultados.loc[df_resultados["table_name"] == tabela, "formacao"].iloc[0]
        )
        if ours == ref:
            concord += 1
        else:
            divergencias.append(f"- `{tabela}`: nosso={ours} | referência={ref}")
    total_ex = sum(1 for t in exemplos if t in tabelas_ours)
    linhas.append(f"## Concordância nos exemplos: {concord}/{total_ex}")
    linhas.append("")
    if divergencias:
        linhas.append("### Divergências em tabelas de exemplo")
        linhas.extend(divergencias)
    else:
        linhas.append("### Divergências em tabelas de exemplo")
        linhas.append("- Nenhuma")
    linhas.append("")

    # Comparações de contagem
    contagens_ours = sumarizar(df_resultados)
    categorias = sorted(set(list(contagens_ours) + list(contagens_ref)))
    linhas.append("## Contagem por categoria (nosso vs referência)")
    linhas.append("")
    linhas.append("| categoria | nosso | referência | diff |")
    linhas.append("|---|---|---|---|")
    for cat in categorias:
        o = contagens_ours.get(cat, 0)
        r = contagens_ref.get(cat, 0)
        linhas.append(f"| {cat} | {o} | {r} | {o - r:+d} |")
    linhas.append(
        f"| **TOTAL** | {len(df_resultados)} | {sum(contagens_ref.values())} | "
        f"{len(df_resultados) - sum(contagens_ref.values()):+d} |"
    )
    linhas.append("")

    # Tabelas marcadas confidence=low
    baixas = df_resultados[df_resultados["confidence"] == "low"]
    linhas.append(f"## Tabelas com confidence=low: {len(baixas)}")
    linhas.append("")
    for _, row in baixas.iterrows():
        linhas.append(f"- `{row['table_name']}`: {row['formacao']} — {row['notes']}")
    linhas.append("")

    return "\n".join(linhas)


def classificar_todas_db(engine) -> pd.DataFrame:
    """Classifica todas as tabelas do schema source via banco de dados.

    Itera ``listar_tabelas()``, lê cada tabela via ``ler_tabela()``,
    aplica ``profile_estrutural()`` + ``classificar_formacao()``,
    e retorna DataFrame com colunas ``table_name``, ``formacao``,
    ``confidence``, ``notes``.

    Args:
        engine: SQLAlchemy Engine.

    Returns:
        DataFrame com colunas ``table_name``, ``formacao``, ``confidence``, ``notes``.
    """
    from classificacao.db.reader import listar_tabelas, ler_tabela
    from classificacao.db.connection import get_schema_source

    schema = get_schema_source()
    tabelas = listar_tabelas(engine, schema=schema)
    registros: list[dict[str, str]] = []

    for table_name in tabelas:
        try:
            df = ler_tabela(engine, table_name, schema=schema)
            profile = profile_estrutural(df, file_size=len(df))
            formacao, confidence, notes = classificar_formacao(table_name, df, profile)
        except Exception as exc:
            formacao, confidence, notes = "indeterminada", "low", f"erro: {exc}"

        registros.append(
            {
                "table_name": table_name,
                "formacao": formacao,
                "confidence": confidence,
                "notes": notes,
            }
        )

    return pd.DataFrame(registros, columns=COLUNAS)


def gerar_relatorio_divergencias_db(
    df_resultados: pd.DataFrame,
    path_referencia=None,
    output_path="data/relatorio_divergencias_db.md",
) -> str:
    """Gera relatório de divergências comparando classificação DB vs referência CSV.

    Similar a ``gerar_relatorio_divergencias`` mas escreve em
    ``data/relatorio_divergencias_db.md``.

    Args:
        df_resultados: DataFrame de classificação (do DB).
        path_referencia: Caminho para o doc autoritativo.
        output_path: Caminho para gerar o relatório Markdown.

    Returns:
        String com o conteúdo do relatório.
    """
    from pathlib import Path

    path = Path(path_referencia) if path_referencia else _AUTORITATIVO_DEFAULT
    exemplos = _parse_referencia_autoritativa(path)

    contagens_ref: dict[str, int] = {}
    if _OPERACIONAL_DEFAULT.exists():
        _, contagens_ref = _parse_referencia(_OPERACIONAL_DEFAULT)

    linhas: list[str] = ["# Relatório de Divergências (DB)", ""]

    # Concordância nos exemplos
    tabelas_ours = set(df_resultados["table_name"].astype(str))
    concord = 0
    divergencias: list[str] = []
    for tabela, ref in exemplos.items():
        if tabela not in tabelas_ours:
            continue
        ours = str(
            df_resultados.loc[df_resultados["table_name"] == tabela, "formacao"].iloc[0]
        )
        if ours == ref:
            concord += 1
        else:
            divergencias.append(f"- `{tabela}`: nosso={ours} | referência={ref}")

    total_ex = sum(1 for t in exemplos if t in tabelas_ours)
    linhas.append(f"## Concordância nos exemplos: {concord}/{total_ex}")
    linhas.append("")
    if divergencias:
        linhas.append("### Divergências em tabelas de exemplo")
        linhas.extend(divergencias)
    else:
        linhas.append("### Divergências em tabelas de exemplo")
        linhas.append("- Nenhuma")
    linhas.append("")

    # Comparações de contagem
    contagens_ours = sumarizar(df_resultados)
    categorias = sorted(set(list(contagens_ours) + list(contagens_ref)))
    linhas.append("## Contagem por categoria (nosso vs referência)")
    linhas.append("")
    linhas.append("| categoria | nosso | referência | diff |")
    linhas.append("|---|---|---|---|")
    for cat in categorias:
        o = contagens_ours.get(cat, 0)
        r = contagens_ref.get(cat, 0)
        linhas.append(f"| {cat} | {o} | {r} | {o - r:+d} |")
    linhas.append(
        f"| **TOTAL** | {len(df_resultados)} | {sum(contagens_ref.values())} | "
        f"{len(df_resultados) - sum(contagens_ref.values()):+d} |"
    )
    linhas.append("")

    # Tabelas marcadas confidence=low
    baixas = df_resultados[df_resultados["confidence"] == "low"]
    linhas.append(f"## Tabelas com confidence=low: {len(baixas)}")
    linhas.append("")
    for _, row in baixas.iterrows():
        linhas.append(f"- `{row['table_name']}`: {row['formacao']} — {row['notes']}")
    linhas.append("")

    relatorio = "\n".join(linhas)
    Path(output_path).write_text(relatorio, encoding="utf-8")
    return relatorio
