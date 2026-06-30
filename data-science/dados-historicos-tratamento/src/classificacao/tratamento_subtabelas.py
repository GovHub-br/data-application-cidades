"""Tratamento de tabelas com estrutura de sub-tabelas (sub_tabelas_1 a 4).

Cada subtipo exige uma estratégia diferente de split/reconstrução:

- **sub_tabelas_1**: Split por ≥2 linhas vazias. Primeira coluna é
  rótulo (recorte_tipo), demais são timestamps. Transforma para formato
  longo (data_referencia + valor).
- **sub_tabelas_2**: Split por palavras-chave (SÍNTESE, Faixa, Região,
  Quadro de Valores) na primeira coluna.
- **sub_tabelas_3**: Split por linhas vazias. Reconstroi cabeçalhos
  multi-linha via forward-fill + concatenação com underscore.
- **sub_tabelas_4**: Split por blocos text-heavy (3+ linhas com ≥2
  valores de texto cada).
"""

from __future__ import annotations

import re

import pandas as pd

from .profiling import _is_text_value
from .tratamento import (
    extrair_periodo_filename,
    inferir_instituicao,
    normalizar_nome_coluna,
    tratar_bem_formada,
)

# Padrão de coluna timestamp (YYYYMMDD_hhmmss)
_TIMESTAMP_COL_RE = re.compile(r"^\d{8}_\d{6}$")


# ─── Helpers ───────────────────────────────────────────────────────


def _classificar_recorte_tipo(label: str) -> str:
    """Classifica o tipo de recorte baseado no valor do rótulo.

    Heurísticas (em ordem de teste):
    - ``FGTS``, ``FAR``, ``Entidades``, ``PNHR``, ``PNHB`` → ``frente``
    - Padrão ``\"0 a 3 SM\"``, ``\"3 a 5 SM\"`` → ``faixa``
    - Começa com ``SUAT`` → ``suat``
    - ``len==2`` e todas uppercase → ``uf``
    - ``Norte``, ``Sul``, ``Nordeste``, ``Sudeste``, ``Centro-Oeste``,
      ``Centro Oeste`` → ``regiao``
    - ``\"Total\"`` no label → ``total``
    - Caso contrário → ``outro``
    """
    label_stripped = label.strip()

    # Frente
    if label_stripped in ("FGTS", "FAR", "Entidades", "PNHR", "PNHB"):
        return "frente"

    # Faixa (padrão "X a Y SM")
    if re.match(r"^\d+\s*a\s+\d+\s+SM$", label_stripped, re.IGNORECASE):
        return "faixa"

    # Suat
    if label_stripped.upper().startswith("SUAT"):
        return "suat"

    # UF (2 caracteres, todas maiúsculas)
    if (
        len(label_stripped) == 2
        and label_stripped.isalpha()
        and label_stripped.isupper()
    ):
        return "uf"

    # Região
    regioes = {"Norte", "Sul", "Nordeste", "Sudeste", "Centro-Oeste", "Centro Oeste"}
    if label_stripped in regioes:
        return "regiao"

    # Total
    if "Total" in label_stripped:
        return "total"

    return "outro"


def _extrair_sub_tabelas_por_linhas_vazias(
    df: pd.DataFrame, min_blank: int = 2
) -> list[pd.DataFrame]:
    """Divide o DataFrame em sub-tabelas separadas por ``min_blank``
    ou mais linhas totalmente vazias consecutivas.

    Args:
        df: DataFrame de entrada.
        min_blank: Número mínimo de linhas vazias consecutivas para
            considerar separação.

    Returns:
        Lista de DataFrames, cada um representando uma sub-tabela.
    """
    if len(df) == 0:
        return []

    empty_rows = df.isna().all(axis=1)
    blanks: list[list[int]] = []
    current_blank: list[int] = []

    for idx, is_empty in enumerate(empty_rows):
        if is_empty:
            current_blank.append(idx)
        else:
            if len(current_blank) >= min_blank:
                blanks.append(current_blank)
            current_blank = []

    if len(current_blank) >= min_blank:
        blanks.append(current_blank)

    # Constrói máscara para linhas a remover (blank separador)
    blank_indices: set[int] = set()
    for blk in blanks:
        blank_indices.update(blk)

    # Divide pelos pontos de corte
    sub_dfs: list[pd.DataFrame] = []
    start = 0
    for blk in blanks:
        mid = blk[0]
        if mid > start:
            chunk = df.iloc[start:mid].dropna(how="all")
            if len(chunk) > 0:
                sub_dfs.append(chunk)
        start = blk[-1] + 1

    # Último chunk após o último bloco de blanks
    if start < len(df):
        chunk = df.iloc[start:].dropna(how="all")
        if len(chunk) > 0:
            sub_dfs.append(chunk)

    return sub_dfs


# ─── sub_tabelas_1 ─────────────────────────────────────────────────


def _extrair_sub_tabelas_1(df: pd.DataFrame) -> list[pd.DataFrame]:
    """Extrai sub-tabelas de uma tabela sub_tabelas_1.

    Split por ≥2 linhas vazias. Cada sub-tabela tem:
    - Coluna 0: rótulo (ex.: ``FGTS``, ``0 a 3 SM``)
    - Colunas 1+: timestamps (``YYYYMMDD_hhmmss``)
    """
    return _extrair_sub_tabelas_por_linhas_vazias(df, min_blank=2)


def _para_long_format(
    df_sub: pd.DataFrame,
    recorte_tipo: str,
) -> pd.DataFrame:
    """Transforma uma sub-tabela do formato largo (rótulo + timestamps)
    para formato longo (recorte + data_referencia + valor).

    Args:
        df_sub: Sub-tabela com coluna 0 = rótulo, colunas 1+ = timestamps.
        recorte_tipo: Tipo de recorte (frente, faixa, etc.).

    Returns:
        DataFrame longo com colunas ``recorte_tipo``, ``recorte_valor``,
        ``data_referencia``, ``valor``.
    """
    if len(df_sub.columns) < 2:
        # Caso degenerado: apenas uma coluna
        rows = []
        for _, row in df_sub.iterrows():
            rows.append(
                {
                    "recorte_tipo": recorte_tipo,
                    "recorte_valor": str(row.iloc[0]),
                    "data_referencia": "",
                    "valor": None,
                }
            )
        return pd.DataFrame(rows)

    date_cols = [c for c in df_sub.columns[1:] if _TIMESTAMP_COL_RE.match(str(c))]

    registros: list[dict] = []
    for _, row in df_sub.iterrows():
        primeiro = row.iloc[0]
        recorte_valor = str(primeiro) if not pd.isna(primeiro) else ""
        if not recorte_valor:
            continue
        for col in date_cols:
            valor_raw = row[col]
            if valor_raw is None or (
                isinstance(valor_raw, float) and pd.isna(valor_raw)
            ):
                continue
            # Parse timestamp: YYYYMMDD_hhmmss → YYYY-MM-DD
            ts_str = str(col).strip()
            try:
                data_ref = f"{ts_str[:4]}-{ts_str[4:6]}-{ts_str[6:8]}"
            except (IndexError, ValueError):
                data_ref = ts_str

            # Tenta converter valor para numérico
            try:
                valor = float(str(valor_raw).replace(",", ".").replace(" ", ""))
            except (ValueError, TypeError):
                valor = valor_raw

            registros.append(
                {
                    "recorte_tipo": recorte_tipo,
                    "recorte_valor": recorte_valor,
                    "data_referencia": data_ref,
                    "valor": valor,
                }
            )

    return pd.DataFrame(registros)


def tratar_sub_tabelas_1(
    table_name: str,
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """Trata tabela classificada como sub_tabelas_1.

    Pipeline:
    1. Extrai sub-tabelas (split por ≥2 linhas vazias)
    2. Identifica ``recorte_tipo`` de cada sub-tabela pelo rótulo
    3. Transforma cada sub-tabela para formato longo
    4. Concatena e adiciona metadados

    Args:
        table_name: Nome da tabela.
        df: DataFrame bruto.

    Returns:
        Tupla ``(df_tratado, info_dict)``.
    """
    sub_tabelas = _extrair_sub_tabelas_1(df)

    partes: list[pd.DataFrame] = []
    total_sub = len(sub_tabelas)
    recortes_detectados: set[str] = set()

    for sub_df in sub_tabelas:
        if len(sub_df) == 0:
            continue
        # Identifica recorte_tipo pelo primeiro rótulo não vazio
        first_label = ""
        for _, row in sub_df.iterrows():
            val = row.iloc[0]
            if pd.notna(val) and str(val).strip():
                first_label = str(val).strip()
                break

        recorte_tipo = (
            _classificar_recorte_tipo(first_label) if first_label else "outro"
        )
        recortes_detectados.add(recorte_tipo)

        long_df = _para_long_format(sub_df, recorte_tipo)
        if len(long_df) > 0:
            partes.append(long_df)

    if partes:
        df_tratado = pd.concat(partes, ignore_index=True)
    else:
        df_tratado = pd.DataFrame(
            columns=["recorte_tipo", "recorte_valor", "data_referencia", "valor"]
        )

    # Adiciona metadados
    instituicao = inferir_instituicao(table_name)
    report_date = extrair_periodo_filename(table_name) or ""

    df_tratado["source_table"] = table_name
    df_tratado["report_date"] = report_date
    df_tratado["institution"] = instituicao
    df_tratado["profile"] = "sub_tabelas_1"

    # Verifica dissonância entre data_referencia (colunas) e report_date (filename)
    warnings_list: list[str] = []
    if report_date and "data_referencia" in df_tratado.columns:
        try:
            report_year = str(report_date)[:4]
            dr_years = set()
            for v in df_tratado["data_referencia"].dropna():
                dr_years.add(str(v)[:4])
            if dr_years and report_year not in dr_years:
                dr_years_str = ", ".join(sorted(dr_years))
                warnings_list.append(
                    f"data_referencia ({dr_years_str}) discorda de "
                    f"report_date ({report_year}); "
                    f"timestamps das colunas podem ser artefato de template"
                )
        except (ValueError, TypeError, KeyError):
            pass

    n_rows, n_cols = df_tratado.shape
    total_cells = n_rows * n_cols
    missing_pct = (
        round(float(df_tratado.isna().sum().sum() / total_cells * 100), 2)
        if total_cells > 0
        else 0.0
    )

    info: dict = {
        "table_name": table_name,
        "status": "treated",
        "n_rows": n_rows,
        "n_cols": n_cols,
        "profile": "sub_tabelas_1",
        "institution": instituicao,
        "report_date": report_date,
        "missing_pct": missing_pct,
        "encoding_issues": 0,
        "date_parse_errors": 0,
        "type_coercion_warnings": 0,
        "sub_tabelas_count": total_sub,
        "recortes_detectados": sorted(recortes_detectados),
        "warnings": warnings_list,
    }

    return df_tratado, info


# ─── sub_tabelas_2 ─────────────────────────────────────────────────


def _extrair_sub_tabelas_2(df: pd.DataFrame) -> list[pd.DataFrame]:
    """Extrai sub-tabelas de uma tabela sub_tabelas_2.

    Divide por palavras-chave (``SÍNTESE``, ``Faixa``, ``Região``,
    ``Quadro de Valores``) encontradas na primeira coluna.

    Cada bloco começa com uma linha contendo a keyword e termina antes
    da próxima keyword (ou fim do DataFrame).
    """
    keywords = {"SÍNTESE", "Faixa", "Região", "Quadro de Valores"}
    n_check = min(100, len(df))

    # Encontra índices onde cada sub-tabela começa
    split_points: list[int] = []
    for i in range(n_check):
        row = df.iloc[i]
        first_val = row.iloc[0]
        if pd.notna(first_val) and str(first_val).strip() in keywords:
            split_points.append(i)

    if not split_points:
        return [df.dropna(how="all").reset_index(drop=True)]

    sub_tabelas: list[pd.DataFrame] = []
    for j, start in enumerate(split_points):
        end = split_points[j + 1] if j + 1 < len(split_points) else len(df)
        chunk = df.iloc[start:end].dropna(how="all").reset_index(drop=True)
        if len(chunk) > 0:
            sub_tabelas.append(chunk)

    return sub_tabelas


def tratar_sub_tabelas_2(
    table_name: str,
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """Trata tabela classificada como sub_tabelas_2.

    Pipeline:
    1. Extrai sub-tabelas por palavras-chave
    2. Concatena com coluna ``sub_table_index``
    3. Aplica normalização básica

    Args:
        table_name: Nome da tabela.
        df: DataFrame bruto.

    Returns:
        Tupla ``(df_tratado, info_dict)``.
    """
    sub_tabelas = _extrair_sub_tabelas_2(df)

    partes: list[pd.DataFrame] = []
    for idx, sub_df in enumerate(sub_tabelas):
        sub_df = sub_df.copy()
        sub_df["sub_table_index"] = idx
        partes.append(sub_df)

    if partes:
        df_tratado = pd.concat(partes, ignore_index=True)
    else:
        df_tratado = df.copy()
        df_tratado["sub_table_index"] = 0

    instituicao = inferir_instituicao(table_name)
    report_date = extrair_periodo_filename(table_name) or ""

    df_tratado["source_table"] = table_name
    df_tratado["report_date"] = report_date
    df_tratado["institution"] = instituicao
    df_tratado["profile"] = "sub_tabelas_2"

    n_rows, n_cols = df_tratado.shape
    total_cells = n_rows * n_cols
    missing_pct = (
        round(float(df_tratado.isna().sum().sum() / total_cells * 100), 2)
        if total_cells > 0
        else 0.0
    )

    info: dict = {
        "table_name": table_name,
        "status": "treated",
        "n_rows": n_rows,
        "n_cols": n_cols,
        "profile": "sub_tabelas_2",
        "institution": instituicao,
        "report_date": report_date,
        "missing_pct": missing_pct,
        "encoding_issues": 0,
        "date_parse_errors": 0,
        "type_coercion_warnings": 0,
        "sub_tabelas_count": len(sub_tabelas),
    }

    return df_tratado, info


# ─── sub_tabelas_3 ─────────────────────────────────────────────────


def _is_title_row(row: pd.Series) -> bool:
    """Check if a row (after ffill) is a title row with identical text in all cells.

    After forward-fill, a title row like "Relatório 2: Total..." spreads to all
    columns. This function checks if all non-null values in the row are the same
    text, indicating a merged title row.
    """
    non_null = row.dropna()
    if len(non_null) == 0:
        return False
    return non_null.nunique() == 1


def _is_text_predominant(row: pd.Series, min_ratio: float = 0.6, min_count: int = 2) -> bool:
    """Check if a row is predominantly composed of text values.

    Returns True if at least ``min_ratio`` (default 60%) of non-null cells are
    text values AND there are at least ``min_count`` (default 2) text cells.

    Uses ``_is_text_value`` from profiling to detect text vs numeric values.
    """
    non_null = row.dropna()
    if len(non_null) == 0:
        return False
    text_cells = [v for v in non_null if _is_text_value(v)]
    return len(text_cells) >= min_count and len(text_cells) / len(non_null) >= min_ratio


def _is_numeric_predominant(row: pd.Series, min_ratio: float = 0.3) -> bool:
    """Check if a row has a significant proportion of numeric values.

    Returns True if at least ``min_ratio`` (default 30%) of non-null cells are
    NOT text (i.e., are numeric or empty-ish values).

    Uses ``_is_text_value`` negated to detect numeric values.
    """
    non_null = row.dropna()
    if len(non_null) == 0:
        return False
    non_text_count = sum(1 for v in non_null if not _is_text_value(v))
    return non_text_count / len(non_null) >= min_ratio


def _reconstruir_header_multilinha(df_bloco: pd.DataFrame) -> pd.DataFrame:
    """Reconstroi cabeçalho multi-linha via forward-fill de células
    mescladas + concatenação com formato ``sub (super)``.

    Detecta linhas de cabeçalho nas primeiras 5 linhas do bloco usando
    heurísticas de predominância de texto vs números. Linhas de título
    (mesma célula espalhada por todas as colunas via ffill) são ignoradas.
    A última linha de cabeçalho é tratada como sub-header; as linhas
    acima formam o super-header via forward-fill.

    Args:
        df_bloco: Bloco de sub-tabela com cabeçalho multi-linha.

    Returns:
        DataFrame com cabeçalho único na primeira linha.
    """
    if len(df_bloco) < 2:
        return df_bloco

    # Identifica linhas de cabeçalho com heurísticas melhoradas
    header_indices: list[int] = []
    for i in range(min(5, len(df_bloco))):
        row = df_bloco.iloc[i]
        non_null = row.dropna()
        if len(non_null) == 0:
            break  # empty row → end of headers

        # Title detection: if all non-null values are identical,
        # it's a merged title row (e.g., "Relatório 2: Total...")
        if non_null.nunique() == 1:
            continue  # skip title row, don't count as header

        # Data detection: if row is predominantly numeric, stop
        if _is_numeric_predominant(row):
            break

        # Header detection: if row is predominantly text
        if _is_text_predominant(row):
            header_indices.append(i)
        else:
            break

    if header_indices:
        n_header = len(header_indices)
        last_header_idx = header_indices[-1]
        header_rows = df_bloco.iloc[header_indices].copy()
        data_rows = df_bloco.iloc[last_header_idx + 1:].copy()
    else:
        n_header = 1  # fallback
        header_rows = df_bloco.iloc[:1].copy()
        data_rows = df_bloco.iloc[1:].copy()

    # Forward-fill nas linhas de cabeçalho (células mescladas).
    # Skip ffill for single-row headers — it only spreads the row's
    # values to trailing NaN columns, creating false duplicate names.
    if n_header > 1:
        header_rows = header_rows.ffill(axis=1)

    # Concatena com formato sub (super): última linha = sub, acima = super via ffill
    new_header_raw: list[str] = []
    for col_idx in range(len(header_rows.columns)):
        sub_val = ""
        super_parts: list[str] = []

        for row_idx in range(n_header):
            val = header_rows.iloc[row_idx, col_idx]
            text = str(val).strip() if pd.notna(val) and str(val).strip() else ""
            if row_idx == n_header - 1:
                sub_val = text
            else:
                super_parts.append(text)

        # Merge super parts (ffill — repeat last non-empty)
        merged_super = ""
        for part in super_parts:
            if part:
                merged_super = part

        # Build name in sub (super) format
        if merged_super and sub_val and merged_super != sub_val:
            name = f"{sub_val} ({merged_super})"
        elif sub_val:
            name = sub_val
        elif merged_super:
            name = merged_super
        else:
            name = f"col_{col_idx}"

        new_header_raw.append(name)

    # Normaliza nomes
    new_header_raw = [normalizar_nome_coluna(n) for n in new_header_raw]

    # Garante unicidade dos nomes de coluna com verificação cross-group
    new_header: list[str] = []
    seen: set[str] = set()
    counter: dict[str, int] = {}
    for name in new_header_raw:
        candidate = name
        while candidate in seen:
            counter[name] = counter.get(name, 0) + 1
            candidate = f"{name}_{counter[name]}"
        seen.add(candidate)
        counter[name] = counter.get(name, 0)
        new_header.append(candidate)

    # Aplica novo cabeçalho
    data_rows.columns = new_header
    data_rows = data_rows.reset_index(drop=True)

    return data_rows


def tratar_sub_tabelas_3(
    table_name: str,
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """Trata tabela classificada como sub_tabelas_3.

    Pipeline:
    1. Extrai sub-tabelas (split por linhas vazias)
    2. Reconstroi cabeçalho multi-linha de cada bloco
    3. Adiciona ``sub_table_index``
    4. Aplica normalização ``bem_formada``

    Args:
        table_name: Nome da tabela.
        df: DataFrame bruto.

    Returns:
        Tupla ``(df_tratado, info_dict)``.
    """
    sub_tabelas = _extrair_sub_tabelas_por_linhas_vazias(df, min_blank=1)

    partes: list[pd.DataFrame] = []
    for idx, sub_df in enumerate(sub_tabelas):
        if len(sub_df) < 2:
            continue
        reconstruido = _reconstruir_header_multilinha(sub_df)
        if len(reconstruido) > 0:
            reconstruido["sub_table_index"] = idx
            partes.append(reconstruido)

    if partes:
        # Alinha colunas via union para garantir consistência
        all_cols = sorted(set().union(*(set(p.columns) for p in partes)))
        partes_align = [p.reindex(columns=all_cols) for p in partes]
        df_concatenado = pd.concat(partes_align, ignore_index=True, sort=False)
    else:
        df_concatenado = df.copy()
        df_concatenado["sub_table_index"] = 0

    # Aplica pipeline bem_formada
    df_tratado, info_base = tratar_bem_formada(table_name, df_concatenado)

    # Preserva sub_table_index
    if "sub_table_index" in df_concatenado.columns:
        df_tratado["sub_table_index"] = df_concatenado["sub_table_index"].values

    # Ajusta info
    info_base["profile"] = "sub_tabelas_3"
    info_base["sub_tabelas_count"] = len(sub_tabelas)

    return df_tratado, info_base


# ─── sub_tabelas_4 ─────────────────────────────────────────────────


def _extrair_sub_tabelas_4(df: pd.DataFrame) -> list[pd.DataFrame]:
    """Extrai sub-tabelas de uma tabela sub_tabelas_4.

    Divide por blocos text-heavy: 3+ linhas consecutivas com ≥2 valores
    de texto cada, separados por linhas vazias.
    """
    if len(df) == 0:
        return []

    empty_rows = df.isna().all(axis=1)
    n_check = min(len(df), len(df))

    # Detecta blocos de sub-tabela
    blocos: list[list[int]] = []

    i = 0
    while i < n_check:
        if empty_rows.iloc[i]:
            i += 1
            continue

        # Início de bloco candidato
        bloco_rows: list[int] = []
        while i < n_check and not empty_rows.iloc[i]:
            bloco_rows.append(i)
            i += 1

        # Verifica se o bloco é text-heavy (pelo menos 3 linhas com ≥2 texto)
        text_heavy_count = 0
        for idx in bloco_rows[:5]:  # Verifica primeiras 5 linhas do bloco
            row = df.iloc[idx]
            non_null = row.dropna()
            n_text = sum(1 for v in non_null if _is_text_value(v))
            if n_text >= 2:
                text_heavy_count += 1

        if text_heavy_count >= 3 and len(bloco_rows) >= 3:
            blocos.append(bloco_rows)

    sub_tabelas: list[pd.DataFrame] = []
    for bloco in blocos:
        chunk = df.iloc[bloco].dropna(how="all").reset_index(drop=True)
        if len(chunk) > 0:
            sub_tabelas.append(chunk)

    return sub_tabelas


def tratar_sub_tabelas_4(
    table_name: str,
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """Trata tabela classificada como sub_tabelas_4.

    Pipeline:
    1. Extrai sub-tabelas por blocos text-heavy
    2. Reconstroi cabeçalho multi-linha de cada bloco
    3. Adiciona ``sub_table_index``
    4. Aplica normalização ``bem_formada``

    Args:
        table_name: Nome da tabela.
        df: DataFrame bruto.

    Returns:
        Tupla ``(df_tratado, info_dict)``.
    """
    sub_tabelas = _extrair_sub_tabelas_4(df)

    partes: list[pd.DataFrame] = []
    for idx, sub_df in enumerate(sub_tabelas):
        if len(sub_df) < 3:
            continue
        reconstruido = _reconstruir_header_multilinha(sub_df)
        if len(reconstruido) > 0:
            reconstruido["sub_table_index"] = idx
            partes.append(reconstruido)

    if partes:
        # Alinha colunas via union para garantir consistência
        all_cols = sorted(set().union(*(set(p.columns) for p in partes)))
        partes_align = [p.reindex(columns=all_cols) for p in partes]
        df_concatenado = pd.concat(partes_align, ignore_index=True, sort=False)
    else:
        df_concatenado = df.copy()
        df_concatenado["sub_table_index"] = 0

    # Aplica pipeline bem_formada
    df_tratado, info_base = tratar_bem_formada(table_name, df_concatenado)

    if "sub_table_index" in df_concatenado.columns:
        df_tratado["sub_table_index"] = df_concatenado["sub_table_index"].values

    info_base["profile"] = "sub_tabelas_4"
    info_base["sub_tabelas_count"] = len(sub_tabelas)

    return df_tratado, info_base
