"""Inventory generation and quality reporting for treated MCMV tables.

Analyzes treated tables to produce a structured inventory CSV and a
comprehensive quality report in Markdown format.  The inventory captures
metadata about each table: formation status, duplicates, fronts (FAR, Rural,
FGTS, etc.), period coverage, predictive-dimension flags, and a utility score.

The module is designed as the final analytical step after classification
(stage 1) and treatment (stage 3) of the MCMV pipeline.

Functions
---------
identificar_frentes
    Detect MCMV fronts (FAR, Rural, FGTS, …) from table name and column
    values.
extrair_periodo_dados
    Extract min/max date from date columns in a DataFrame.
detectar_dimensoes
    Detect which predictive dimensions (status_obra, datas, progresso, …)
    are present.
listar_campos_uteis
    Return column names that match any predictive-dimension pattern.
calcular_score_utilidade
    Compute a numeric utility score (0–10) and its classification label.
gerar_inventario
    Main orchestration — reads classification, quality, and dedup data, then
    writes the inventory CSV.
gerar_relatorio_qualidade
    Generate a Markdown quality report from the inventory.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

FRONT_KW: dict[str, str] = {
    "far": "FAR",
    "rural": "Rural",
    "entidades": "Entidades",
    "fgts": "FGTS",
    "pnhr": "PNHR",
    "pnhb": "PNHB",
    "fds": "FDS",
    "ccfgts": "CCFGTS",
}

PF_PJ_PATTERNS: list[tuple[str, str]] = [
    (r"_pf_|_pf$|_pf\d", "PF"),
    (r"_pj_|_pj$|_pj\d", "PJ"),
    (r"contratacao_pf|contratação_pf", "PF"),
]

DATE_COL_SUBSTRINGS: tuple[str, ...] = (
    "dt_",
    "data_",
    "dat_",
    "cronograma_datatermino",
    "dataprevista",
    "data_prevista",
    "ano_mes",
    "ianomes",
)

METADATA_COLS: frozenset[str] = frozenset(
    {"source_table", "report_date", "institution", "profile", "content_hash"}
)

DIMENSION_PATTERNS: dict[str, tuple[str, ...]] = {
    "status_obra": (
        "situacao",
        "status_obra",
        "paralisad",
        "concluid",
        "andamento",
    ),
    "datas": (
        "dt_",
        "data_",
        "dat_",
    ),
    "progresso": (
        "percentual_obra",
        "_executada",
        "concluidas",
        "entregues",
        "em_obras",
        "faixa_perc",
        "prc_execucao",
    ),
    "financeiro": (
        "valor_",
        "vlr_",
        "vr_",
        "subsidio",
        "emprestimo",
        "investimento",
        "financiamento",
        "desembolsado",
        "contratado",
    ),
    "geolocalizacao_municipio": (
        "municipio",
        "codigo_ibge",
        "cod_municipio",
        "codmunicibge",
    ),
    "geolocalizacao_uf": ("uf",),
    "granularidade": (
        "apf",
        "cod_contrato",
        "nr_",
        "nu_apf",
        "idregistro",
        "codigo_empreendimento",
    ),
}

# ----------------------------------------------------------------------
# Auxiliary helpers
# ----------------------------------------------------------------------


def _parse_date(val: Any) -> datetime | None:
    """Try to parse *val* as a date using several known formats.

    Parameters
    ----------
    val : Any
        Value to parse (string, numeric, or ``None``).

    Returns
    -------
    datetime.datetime or None
    """
    if (
        val is None
        or val == "NA"
        or val == ""
        or (isinstance(val, float) and pd.isna(val))
    ):
        return None
    val_str = str(val).strip()

    # YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", val_str)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # DD/MM/YYYY
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})", val_str)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    # YYYYMMDD (8 digits)
    m = re.match(r"^(\d{8})$", val_str)
    if m:
        try:
            return datetime(int(val_str[:4]), int(val_str[4:6]), int(val_str[6:8]))
        except ValueError:
            pass

    # YYYYMM (6 digits)
    m = re.match(r"^(\d{6})$", val_str)
    if m:
        try:
            return datetime(int(val_str[:4]), int(val_str[4:6]), 1)
        except ValueError:
            pass

    return None


def _extract_report_date(table_name: str) -> str | None:
    """Try to extract an ISO date from the table name.

    Parameters
    ----------
    table_name : str
        Table name string.

    Returns
    -------
    str or None
        ISO date string if found.
    """
    m = re.search(r"(\d{4})[-_]?(\d{2})[-_]?(\d{2})?", table_name)
    if m:
        year, month = m.group(1), m.group(2)
        day = m.group(3) or "01"
        try:
            dt = datetime(int(year), int(month), int(day))
            return dt.isoformat()[:10]
        except ValueError:
            pass
    return None


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------


def identificar_frentes(table_name: str, df: pd.DataFrame | None = None) -> set[str]:
    """Identify MCMV fronts covered by a table.

    Uses three levels of heuristics:

    1. **Table name regex** — checks whole-word keyword matches and
       PF/PJ patterns.
    2. **Column values** — scans columns ``origem``, ``produto``,
       ``programa``, ``csigla_origem``, ``subprograma``, ``fase_do_pmcmv``
       for front keywords.
    3. **Institution + pattern inference** — applies rules for
       ``bb_*``, ``caixa_*``, ``historico_recente_*`` and ``snh_*`` names.

    Parameters
    ----------
    table_name : str
        Name of the table (file name without path/extension).
    df : pd.DataFrame or None, optional
        The loaded DataFrame.  Required for Level 2 detection.

    Returns
    -------
    set[str]
        Detected fronts (e.g. ``{'FAR', 'FGTS'}``).  Empty if none found.
    """
    fronts: set[str] = set()

    # -- Level 1: table name regex ------------------------------------
    name_lower = table_name.lower()

    for keyword, label in FRONT_KW.items():
        if re.search(rf"\b{re.escape(keyword)}\b", name_lower):
            fronts.add(label)

    for pattern, label in PF_PJ_PATTERNS:
        if re.search(pattern, name_lower):
            fronts.add(label)

    # -- Level 2: column values ---------------------------------------
    if df is not None and not df.empty:
        value_cols = [
            "origem",
            "produto",
            "programa",
            "csigla_origem",
            "subprograma",
            "fase_do_pmcmv",
        ]
        existing_cols = {c.lower(): c for c in df.columns}
        for col_lower, col_actual in existing_cols.items():
            if col_lower not in value_cols:
                continue
            unique_values = df[col_actual].dropna().unique()
            for val in unique_values:
                val_str = str(val).lower()
                for keyword, label in FRONT_KW.items():
                    if keyword in val_str:
                        fronts.add(label)

    # -- Level 3: institution + pattern inference ---------------------
    if table_name.startswith("bb_"):
        if re.search(r"_pf_|_pf$|_pf\d", table_name):
            fronts.add("PF")
        if re.search(r"_pj_|_pj$|_pj\d", table_name):
            fronts.add("PJ")

    if table_name.startswith("caixa_") and re.search(r"_001_", table_name):
        fronts.add("FAR")

    if "historico_recente_" in table_name:
        fronts.add("FAR")

    if "snh_" in table_name and ("af_bb" in table_name or "af_caixa" in table_name):
        fronts.add("FAR")

    return fronts


def extrair_periodo_dados(df: pd.DataFrame) -> tuple[str | None, str | None]:
    """Extract (min_date, max_date) from date columns within the DataFrame.

    Scans all columns whose name contains a known date substring (e.g.
    ``dt_``, ``data_``, ``ano_mes``).  Parses each value and returns
    the global minimum and maximum as ISO date strings.

    Parameters
    ----------
    df : pd.DataFrame
        The treated DataFrame.

    Returns
    -------
    tuple[str | None, str | None]
        ``(min_date, max_date)`` in ``YYYY-MM-DD`` format, or ``(None, None)``
        if no parseable dates were found.
    """
    all_dates: list[datetime] = []

    for col in df.columns:
        col_lower = col.lower().strip()
        if col_lower in METADATA_COLS:
            continue
        if not any(sub in col_lower for sub in DATE_COL_SUBSTRINGS):
            continue
        for val in df[col].dropna():
            parsed = _parse_date(val)
            if parsed is not None:
                all_dates.append(parsed)

    if not all_dates:
        return None, None

    return (
        min(all_dates).isoformat()[:10],
        max(all_dates).isoformat()[:10],
    )


def detectar_dimensoes(df: pd.DataFrame) -> dict[str, bool | int]:
    """Detect which predictive dimensions are present in the DataFrame.

    Examines column names against dimension patterns.  Returns a flat dict
    with the following keys:

    - ``status_obra`` : bool
    - ``datas`` : int (count of matching date columns)
    - ``progresso`` : bool
    - ``financeiro`` : bool
    - ``geolocalizacao`` : bool
    - ``granularidade`` : bool

    All substring matching is **case-insensitive**.

    Parameters
    ----------
    df : pd.DataFrame
        The treated DataFrame.

    Returns
    -------
    dict[str, bool | int]
    """
    col_lower_list = [c.lower() for c in df.columns]

    # status_obra
    status_obra = any(
        any(pat in c for pat in DIMENSION_PATTERNS["status_obra"])
        for c in col_lower_list
    )

    # datas -- count of columns matching date substrings (excl. metadata)
    datas_count = sum(
        1
        for c in col_lower_list
        if c.strip() not in METADATA_COLS
        and any(sub in c for sub in DIMENSION_PATTERNS["datas"])
    )

    # progresso
    progresso = any(
        any(pat in c for pat in DIMENSION_PATTERNS["progresso"]) for c in col_lower_list
    )

    # financeiro
    financeiro = any(
        any(pat in c for pat in DIMENSION_PATTERNS["financeiro"])
        for c in col_lower_list
    )

    # geolocalizacao -- needs municipio/IBGE column AND uf column
    has_muni = any(
        any(pat in c for pat in DIMENSION_PATTERNS["geolocalizacao_municipio"])
        for c in col_lower_list
    )
    has_uf = any(
        any(pat in c for pat in DIMENSION_PATTERNS["geolocalizacao_uf"])
        for c in col_lower_list
    )
    geolocalizacao = has_muni and has_uf

    # granularidade
    granularidade = any(
        any(pat in c for pat in DIMENSION_PATTERNS["granularidade"])
        for c in col_lower_list
    )

    return {
        "status_obra": status_obra,
        "datas": datas_count,
        "progresso": progresso,
        "financeiro": financeiro,
        "geolocalizacao": geolocalizacao,
        "granularidade": granularidade,
    }


def listar_campos_uteis(df: pd.DataFrame) -> list[str]:
    """Return column names that match any predictive-dimension pattern.

    The returned list is deduplicated and ordered by column appearance in
    the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The treated DataFrame.

    Returns
    -------
    list[str]
    """
    seen: set[str] = set()
    result: list[str] = []

    for col_actual in df.columns:
        cl = col_actual.lower()
        if cl in seen:
            continue
        for group_pats in DIMENSION_PATTERNS.values():
            if any(pat in cl for pat in group_pats):
                result.append(col_actual)
                seen.add(cl)
                break

    return result


def calcular_score_utilidade(
    perfil: str,
    dimensoes: dict[str, bool | int],
    is_duplicate: bool = False,
    is_tratado: bool = True,
) -> tuple[int, str]:
    """Compute a predictive utility score (0-10) and classification label.

    The formula awards positive points for useful dimensions and penalises
    aggregate profiles, duplicates, and untreated tables.

    Parameters
    ----------
    perfil : str
        Profile label from treatment (e.g. ``colunar_denso``,
        ``event_level``, ``agregado_uf``, ...).
    dimensoes : dict[str, bool | int]
        Dimension dict from :func:`detectar_dimensoes`.
    is_duplicate : bool, optional
        Whether the table is a duplicate (default ``False``).
    is_tratado : bool, optional
        Whether the table was successfully treated (default ``True``).

    Returns
    -------
    tuple[int, str]
        ``(score, classificacao)`` where *classificacao* is one of
        ``'Alta'``, ``'Média'``, ``'Baixa'``, or ``'Nenhuma'``.
    """
    score = 0

    # Positive dimensions
    if dimensoes.get("status_obra"):
        score += 2
    if isinstance(dimensoes.get("datas"), int) and dimensoes["datas"] >= 3:
        score += 2
    if dimensoes.get("progresso"):
        score += 2
    if dimensoes.get("financeiro"):
        score += 1
    if dimensoes.get("geolocalizacao"):
        score += 1
    if dimensoes.get("granularidade"):
        score += 1
    if perfil in ("colunar_denso", "event_level"):
        score += 1

    # Penalties
    if perfil in ("agregado_uf", "agregado_municipal"):
        score -= 2
    if perfil in ("lookup", "dados_sem_utilidade", "vazia"):
        score -= 3
    if is_duplicate:
        score -= 3
    if not is_tratado:
        score -= 1

    if score >= 8:
        classificacao = "Alta"
    elif score >= 5:
        classificacao = "M\u00e9dia"
    elif score >= 2:
        classificacao = "Baixa"
    else:
        classificacao = "Nenhuma"

    return score, classificacao


def gerar_inventario(
    quality_path: str,
    dedup_path: str,
    classificacao_path: str,
    treated_dir: str,
    output_path: str,
) -> pd.DataFrame:
    """Orchestrate inventory generation.

    Reads the quality report, dedup map, and classification CSV; iterates
    every classified table; computes per-table metadata (fronts, period,
    dimensions, utility score); and writes the result as a tab-separated CSV.

    Parameters
    ----------
    quality_path : str
        Path to ``_quality_report.csv`` (tab-separated).
    dedup_path : str
        Path to ``_dedup_map.csv`` (tab-separated).
    classificacao_path : str
        Path to ``classificacao_formacao.csv`` (comma-separated).
    treated_dir : str
        Directory containing treated CSV files (``*_tratado.csv``).
    output_path : str
        Destination path for the inventory CSV.

    Returns
    -------
    pd.DataFrame
        The complete inventory as a DataFrame.
    """
    quality_path_p = Path(quality_path)
    dedup_path_p = Path(dedup_path)
    classificacao_path_p = Path(classificacao_path)
    treated_dir_p = Path(treated_dir)
    output_path_p = Path(output_path)

    # -- Read inputs ---------------------------------------------------
    quality_df = pd.read_csv(quality_path_p, sep="\t")
    dedup_df = pd.read_csv(dedup_path_p)
    classif_df = pd.read_csv(classificacao_path_p, sep=",")

    # Build lookup dicts
    quality_by_table: dict[str, dict[str, Any]] = {}
    for _, row in quality_df.iterrows():
        quality_by_table[str(row["table_name"]).strip()] = row.to_dict()

    dedup_by_source: dict[str, dict[str, Any]] = {}
    for _, row in dedup_df.iterrows():
        dedup_by_source[str(row["source_table"]).strip()] = row.to_dict()

    # -- Build inventory rows ------------------------------------------
    columns = [
        "table_name",
        "formacao",
        "status_tratamento",
        "is_duplicate",
        "canonical_table",
        "frentes_cobertas",
        "instituicao",
        "perfil",
        "report_date",
        "periodo_dados_inicio",
        "periodo_dados_fim",
        "n_linhas",
        "n_colunas",
        "missing_pct",
        "encoding_issues",
        "date_parse_errors",
        "type_coercion_warnings",
        "score_utilidade_preditiva",
        "classificacao_utilidade",
        "tem_status_obra",
        "tem_datas",
        "tem_progresso",
        "tem_financeiro",
        "tem_geolocalizacao",
        "tem_granularidade_contrato",
        "campos_uteis_preditiva",
    ]

    rows: list[list[Any]] = []

    for _, classif_row in classif_df.iterrows():
        table_name = str(classif_row["table_name"]).strip()
        formacao = str(classif_row.get("formacao", ""))

        # Dedup info
        dedup_info = dedup_by_source.get(table_name, {})
        is_dup = bool(dedup_info.get("is_duplicate", False))
        canonical = str(dedup_info.get("canonical_table", ""))

        # Quality info
        qinfo = quality_by_table.get(table_name, None)

        # Defaults
        status_tratamento: str
        frentes: set[str] = set()
        periodo_inicio: str | None = None
        periodo_fim: str | None = None
        dimensoes: dict[str, bool | int] = {}
        campos_uteis: list[str] = []
        instituicao = ""
        perfil = ""
        report_date = ""
        n_linhas: int = 0
        n_colunas: int = 0
        missing_pct: float = 100.0
        encoding_issues: int = 0
        date_parse_errors: int = 0
        type_coercion_warnings: int = 0
        score = 0
        classificacao_utilidade = "Nenhuma"

        if qinfo is not None:
            # -- Tratado -----------------------------------------------
            status_tratamento = "tratado"
            instituicao = str(qinfo.get("institution", ""))
            perfil = str(qinfo.get("profile", ""))
            report_date = str(qinfo.get("report_date", ""))
            n_linhas = int(qinfo.get("n_rows", 0))
            n_colunas = int(qinfo.get("n_cols", 0))
            missing_pct_val = qinfo.get("missing_pct", 100.0)
            if (
                missing_pct_val is None
                or missing_pct_val == ""
                or pd.isna(missing_pct_val)
            ):
                missing_pct = 100.0
            else:
                missing_pct = float(missing_pct_val)

            def _safe_int(v: Any) -> int:
                if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
                    return 0
                return int(v)

            encoding_issues = _safe_int(qinfo.get("encoding_issues", 0))
            date_parse_errors = _safe_int(qinfo.get("date_parse_errors", 0))
            type_coercion_warnings = _safe_int(qinfo.get("type_coercion_warnings", 0))

            treated_file = treated_dir_p / f"{table_name}_tratado.csv"
            df: pd.DataFrame | None = None
            try:
                df = pd.read_csv(treated_file, sep="\t")
            except (FileNotFoundError, pd.errors.EmptyDataError, Exception):
                status_tratamento = "erro"

            if df is not None and not df.empty:
                frentes = identificar_frentes(table_name, df)
                periodo_inicio, periodo_fim = extrair_periodo_dados(df)
                dimensoes = detectar_dimensoes(df)
                campos_uteis = listar_campos_uteis(df)
                score, classificacao_utilidade = calcular_score_utilidade(
                    perfil, dimensoes, is_dup, True
                )
            else:
                frentes = identificar_frentes(table_name)
                score, classificacao_utilidade = calcular_score_utilidade(
                    perfil,
                    {
                        "status_obra": False,
                        "datas": 0,
                        "progresso": False,
                        "financeiro": False,
                        "geolocalizacao": False,
                        "granularidade": False,
                    },
                    is_dup,
                    True,
                )
        else:
            # -- Nao tratado -------------------------------------------
            status_tratamento = "nao_tratado"
            frentes = identificar_frentes(table_name)
            periodo_inicio = _extract_report_date(table_name)
            dimensoes = {
                "status_obra": False,
                "datas": 0,
                "progresso": False,
                "financeiro": False,
                "geolocalizacao": False,
                "granularidade": False,
            }
            campos_uteis = []
            score, classificacao_utilidade = calcular_score_utilidade(
                "", dimensoes, is_dup, False
            )

        frentes_str = ", ".join(sorted(frentes)) if frentes else ""
        campos_uteis_str = ", ".join(campos_uteis) if campos_uteis else ""

        rows.append(
            [
                table_name,
                formacao,
                status_tratamento,
                str(is_dup),
                canonical,
                frentes_str,
                instituicao,
                perfil,
                report_date,
                periodo_inicio or "",
                periodo_fim or "",
                n_linhas,
                n_colunas,
                missing_pct,
                encoding_issues,
                date_parse_errors,
                type_coercion_warnings,
                score,
                classificacao_utilidade,
                str(dimensoes.get("status_obra", False)),
                dimensoes.get("datas", 0),
                str(dimensoes.get("progresso", False)),
                str(dimensoes.get("financeiro", False)),
                str(dimensoes.get("geolocalizacao", False)),
                str(dimensoes.get("granularidade", False)),
                campos_uteis_str,
            ]
        )

    inventory_df = pd.DataFrame(rows, columns=columns)
    inventory_df.to_csv(output_path_p, sep="\t", index=False)
    return inventory_df


def gerar_relatorio_qualidade(
    inventario_path: str,
    quality_path: str,
    dedup_path: str,
    output_path: str,
) -> None:
    """Generate a Markdown quality report from the inventory.

    The report contains seven sections:

    1. Executive Summary
    2. Scoring Methodology
    3. Duplicates
    4. Quality by Profile
    5. Quality by Institution
    6. Score Distribution
    7. Known Limitations

    Parameters
    ----------
    inventario_path : str
        Path to the inventory CSV (tab-separated).
    quality_path : str
        Path to ``_quality_report.csv`` (tab-separated).
    dedup_path : str
        Path to ``_dedup_map.csv`` (tab-separated).
    output_path : str
        Destination path for the Markdown report.
    """
    inventario_path_p = Path(inventario_path)
    quality_path_p = Path(quality_path)
    dedup_path_p = Path(dedup_path)
    output_path_p = Path(output_path)

    inv_df = pd.read_csv(inventario_path_p, sep="\t")
    quality_df = pd.read_csv(quality_path_p, sep="\t")
    dedup_df = pd.read_csv(dedup_path_p)

    total_tables = len(inv_df)

    # -- Period range --------------------------------------------------
    valid_starts = inv_df.loc[
        inv_df["periodo_dados_inicio"].notna() & (inv_df["periodo_dados_inicio"] != ""),
        "periodo_dados_inicio",
    ]
    valid_ends = inv_df.loc[
        inv_df["periodo_dados_fim"].notna() & (inv_df["periodo_dados_fim"] != ""),
        "periodo_dados_fim",
    ]
    period_min = valid_starts.min() if not valid_starts.empty else "N/A"
    period_max = valid_ends.max() if not valid_ends.empty else "N/A"

    # -- Fronts --------------------------------------------------------
    all_fronts: set[str] = set()
    for val in inv_df["frentes_cobertas"].dropna():
        for f in str(val).split(", "):
            f_stripped = f.strip()
            if f_stripped:
                all_fronts.add(f_stripped)
    fronts_str = ", ".join(sorted(all_fronts)) if all_fronts else "Nenhuma"

    # -- Score distribution --------------------------------------------
    alta_count = int((inv_df["classificacao_utilidade"] == "Alta").sum())
    media_count = int((inv_df["classificacao_utilidade"] == "M\u00e9dia").sum())
    baixa_count = int((inv_df["classificacao_utilidade"] == "Baixa").sum())
    nenhuma_count = int((inv_df["classificacao_utilidade"] == "Nenhuma").sum())

    # -- Duplicates ----------------------------------------------------
    n_duplicates = int(dedup_df["is_duplicate"].sum())
    dup_pct = round(n_duplicates / max(len(dedup_df), 1) * 100, 1)
    dup_counts = (
        dedup_df[dedup_df["is_duplicate"] == True]  # noqa: E712
        .groupby("canonical_table")
        .size()
    )
    max_dup_table = dup_counts.idxmax() if not dup_counts.empty else "N/A"
    max_dup_count = int(dup_counts.max()) if not dup_counts.empty else 0
    top_dups = (
        dedup_df[dedup_df["is_duplicate"] == True]  # noqa: E712
        .head(5)["source_table"]
        .tolist()
    )

    # -- Quality by profile --------------------------------------------
    profile_stats = (
        quality_df.groupby("profile")
        .agg(
            n_tabelas=("table_name", "count"),
            missing_pct_medio=("missing_pct", "mean"),
            encoding_issues_total=("encoding_issues", "sum"),
            date_parse_errors_total=("date_parse_errors", "sum"),
        )
        .reset_index()
    )
    profile_stats["missing_pct_medio"] = profile_stats["missing_pct_medio"].round(2)
    profile_stats["encoding_issues_total"] = profile_stats[
        "encoding_issues_total"
    ].astype(int)
    profile_stats["date_parse_errors_total"] = profile_stats[
        "date_parse_errors_total"
    ].astype(int)

    # -- Quality by institution ----------------------------------------
    inst_stats = (
        inv_df.groupby("instituicao")
        .agg(
            n_tabelas=("table_name", "count"),
            missing_pct_medio=("missing_pct", "mean"),
            com_status_obra=(
                "tem_status_obra",
                lambda x: (x.astype(str).str.lower() == "true").sum(),
            ),
            com_financeiro=(
                "tem_financeiro",
                lambda x: (x.astype(str).str.lower() == "true").sum(),
            ),
        )
        .reset_index()
    )
    inst_stats["missing_pct_medio"] = inst_stats["missing_pct_medio"].round(2)
    inst_stats["pct_com_status_obra"] = (
        inst_stats["com_status_obra"] / inst_stats["n_tabelas"] * 100
    ).round(1)
    inst_stats["pct_com_financeiro"] = (
        inst_stats["com_financeiro"] / inst_stats["n_tabelas"] * 100
    ).round(1)

    # -- Score histogram -----------------------------------------------
    score_counts = inv_df["score_utilidade_preditiva"].value_counts().sort_index()
    hist_lines: list[str] = []
    for s in range(0, 11):
        cnt = int(score_counts.get(s, 0))
        bar = "#" * min(cnt, 50)
        hist_lines.append(f"- **{s}**: {cnt} tabelas {bar}")

    # -- Known limitations --------------------------------------------
    sem_periodo_mask = inv_df["periodo_dados_inicio"].isna() | (
        inv_df["periodo_dados_inicio"] == ""
    )
    n_sem_periodo = int(sem_periodo_mask.sum())

    sem_frente_mask = inv_df["frentes_cobertas"].isna() | (
        inv_df["frentes_cobertas"] == ""
    )
    n_sem_frente = int(sem_frente_mask.sum())

    # -- Build Markdown ------------------------------------------------
    lines: list[str] = []
    lines.append("# Relatório de Qualidade dos Dados")
    lines.append("")
    lines.append(
        "Relatório gerado automaticamente pelo módulo de inventário "
        "após a classificação e tratamento das tabelas MCMV."
    )
    lines.append("")

    # Section 1 - Executive Summary
    lines.append("## 1. Resumo Executivo")
    lines.append("")
    lines.append(f"- **Total de tabelas analisadas:** {total_tables}")
    lines.append(f"- **Per\u00edodo coberto:** {period_min} a {period_max}")
    lines.append(f"- **Frentes identificadas:** {fronts_str}")
    lines.append(f"- **Alta utilidade:** {alta_count}")
    lines.append(f"- **M\u00e9dia utilidade:** {media_count}")
    lines.append(f"- **Baixa utilidade:** {baixa_count}")
    lines.append(f"- **Nenhuma utilidade:** {nenhuma_count}")
    lines.append("")

    # Section 2 - Scoring Methodology
    lines.append("## 2. Metodologia de Scoring")
    lines.append("")
    lines.append(
        "A pontua\u00e7\u00e3o de utilidade preditiva (0-10) \u00e9 "
        "calculada como: +2 para presen\u00e7a de colunas de status de "
        "obra (situacao_, status_obra, paralisad, concluid, andamento); "
        "+2 para >=3 colunas de data; +2 para colunas de progresso "
        "(percentual_obra, concluidas, entregues, em_obras); +1 para "
        "colunas financeiras (valor_, vlr_, subsidio, investimento); "
        "+1 para geolocaliza\u00e7\u00e3o (municipio + UF/IBGE); +1 para "
        "granularidade de contrato (APF, cod_contrato); +1 para perfil "
        "colunar_denso ou event_level. Penalidades: -2 para "
        "agregado_uf/agregado_municipal; -3 para "
        "lookup/dados_sem_utilidade/vazia; -3 para tabelas duplicadas; "
        "-1 para tabelas n\u00e3o tratadas. Classifica\u00e7\u00e3o: "
        "8-10 = Alta, 5-7 = M\u00e9dia, 2-4 = Baixa, <=1 = Nenhuma."
    )
    lines.append("")

    # Section 3 - Duplicates
    lines.append("## 3. Duplicatas")
    lines.append("")
    lines.append(
        f"- **Total de duplicatas:** {n_duplicates} "
        f"({dup_pct}% do total de {len(dedup_df)} tabelas)"
    )
    lines.append(
        f"- **Tabela com mais c\u00f3pias:** {max_dup_table} "
        f"({max_dup_count} c\u00f3pias)"
    )
    lines.append("")
    lines.append("**Top 5 exemplos de duplicatas:**")
    for i, s in enumerate(top_dups, 1):
        lines.append(f"  {i}. {s}")
    lines.append("")

    # Section 4 - Quality by Profile
    lines.append("## 4. Qualidade por Perfil")
    lines.append("")
    lines.append(
        "| Perfil | N Tabelas | Missing % M\u00e9dio | Encoding Errors | Date Parse Errors |"
    )
    lines.append(
        "|--------|-----------|---------------------|-----------------|-------------------|"
    )
    for _, row in profile_stats.iterrows():
        lines.append(
            f"| {row['profile']} | {row['n_tabelas']} "
            f"| {row['missing_pct_medio']} | {row['encoding_issues_total']} "
            f"| {row['date_parse_errors_total']} |"
        )
    lines.append("")

    # Section 5 - Quality by Institution
    lines.append("## 5. Qualidade por Institui\u00e7\u00e3o")
    lines.append("")
    lines.append(
        "| Institui\u00e7\u00e3o | N Tabelas "
        "| Missing % M\u00e9dio | % com Status Obra "
        "| % com Financeiro |"
    )
    lines.append(
        "|--------------|-----------|---------------------"
        "|-------------------|------------------|"
    )
    for _, row in inst_stats.iterrows():
        lines.append(
            f"| {row['instituicao']} | {row['n_tabelas']} "
            f"| {row['missing_pct_medio']} "
            f"| {row['pct_com_status_obra']}% "
            f"| {row['pct_com_financeiro']}% |"
        )
    lines.append("")

    # Section 6 - Score Distribution
    lines.append("## 6. Distribui\u00e7\u00e3o de Scores")
    lines.append("")
    lines.append("### Histograma de Scores (0-10)")
    lines.extend(hist_lines)
    lines.append("")
    lines.append("### Classifica\u00e7\u00e3o agregada")
    lines.append(f"- **Alta (8-10):** {alta_count} tabelas")
    lines.append(f"- **M\u00e9dia (5-7):** {media_count} tabelas")
    lines.append(f"- **Baixa (2-4):** {baixa_count} tabelas")
    lines.append(f"- **Nenhuma (<=1):** {nenhuma_count} tabelas")
    lines.append("")

    # Section 7 - Known Limitations
    lines.append("## 7. Limita\u00e7\u00f5es Conhecidas")
    lines.append("")
    lines.append(
        f"- **Tabelas sem per\u00edodo identificado:** {n_sem_periodo} tabelas"
    )
    lines.append(
        f"- **Tabelas com frente n\u00e3o identificada:** {n_sem_frente} tabelas"
    )
    lines.append(
        "- Os dados analisados s\u00e3o amostras de 200 linhas do "
        "dump original. Datas m\u00ednima e m\u00e1xima podem n\u00e3o "
        "representar a totalidade do per\u00edodo real."
    )
    lines.append(
        "- Formatos de data n\u00e3o padronizados "
        '(ex.: "Pronto - Julho/2010") n\u00e3o s\u00e3o interpretados '
        "pelo parser atual."
    )
    lines.append("")

    output_path_p.write_text("\n".join(lines), encoding="utf-8")
