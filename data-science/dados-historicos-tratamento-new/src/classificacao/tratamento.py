"""Tratamento estrutural e type-canonization de tabelas bem formadas.

Pipeline de tratamento (Stage 2-3 do projeto MCMV) que opera sobre
DataFrames já classificados como ``bem_formada``. O módulo oferece:

* **Group 1:** Normalização de nomes de coluna.
* **Group 2:** Detecção e correção de separador decimal.
* **Group 3:** Parsing de datas e detecção de formato.
* **Group 4:** Reparo de encoding (charset-normalizer + ftfy).
* **Group 5:** Type canonization (manual + auto via inventário).
* **Group 6:** Classificação de perfil (lookup / event_level / agregado_* /
  colunar_denso).
* **Group 7:** Extração de período e instituição.
* **Group 8:** Metadata enrichment e pipeline completo.

Todas as funções são puras (sem I/O) exceto ``carregar_inventario_colunas``
e ``tratar_bem_formada``.
"""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any

import ftfy
import pandas as pd
from charset_normalizer import detect

# ──────────────────────────────────────────────────────────────────────
# Data Cleaning (pre-treatment)
# ──────────────────────────────────────────────────────────────────────


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Remove linhas e colunas sem dados antes do tratamento.

    Remove:
    - Linhas onde todas as células são NaN ou contêm apenas o caractere
      ``'-'`` (com whitespace opcional).
    - Colunas onde todos os valores são NaN.

    A função é segura para DataFrames vazios (retorna cópia sem alteração).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a ser limpo.

    Returns
    -------
    pd.DataFrame
        DataFrame sem linhas vazias/dash-only e sem colunas 100% NaN.
    """
    if df.empty:
        return df.copy()

    df = df.copy()

    # Remove rows where all cells are NaN or '-'
    mask_keep = ~df.apply(
        lambda row: all(pd.isna(v) or str(v).strip() in ("", "-") for v in row),
        axis=1,
    )
    df = df.loc[mask_keep].reset_index(drop=True)

    # Remove columns that are 100% NaN
    df = df.dropna(axis=1, how="all")

    return df


# ──────────────────────────────────────────────────────────────────────
# Data Cleaning (post-treatment)
# ──────────────────────────────────────────────────────────────────────

# Labels conhecidos de vazamento de multi-header Excel
_LABELS_HEADER_VAZAMENTO: set[str] = {
    "mês",
    "mes",
    "acumulado",
    "mensal",
    "anual",
    "%",
    "% entregas",
    "total",
    "subtotal",
    "portal",
    "siaci",
    "apf",
    "fonte:",
    "obs:",
    "nota:",
}

# Colunas de metadados que não devem ser contadas como dados
_COLUNAS_METADADOS: set[str] = {
    "source_table",
    "report_date",
    "institution",
    "profile",
    "sub_table_index",
    "content_hash",
}


def classificar_coluna_esparsa(df: pd.DataFrame, col_name: str) -> tuple[int, str]:
    """Classifica uma coluna com alta taxa de NULL em um dos 5 tipos.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame tratado.
    col_name : str
        Nome da coluna a classificar.

    Returns
    -------
    tuple[int, str]
        ``(tipo, evidencia)`` onde tipo é 1-5 e evidencia descreve
        o critério usado.
    """
    non_null = df[col_name].dropna()
    n_non_null = len(non_null)

    # Tipo 2: 100% NULL
    if n_non_null == 0:
        return 2, "100% valores NULL"

    # Coleta amostra de valores não-nulos
    unique_vals = non_null.unique()
    sample_vals = [str(v).strip().lower() for v in unique_vals[:20] if pd.notna(v)]

    # Verifica se é numérico
    numeric_count = 0
    for v in unique_vals:
        try:
            float(str(v).replace(",", "."))
            numeric_count += 1
        except (ValueError, TypeError):
            pass

    pct_numeric = numeric_count / len(unique_vals) if len(unique_vals) > 0 else 0

    has_sub_table = "sub_table_index" in df.columns

    # Tipo 4: Vazamento de multi-header
    header_label_count = sum(1 for v in sample_vals if v in _LABELS_HEADER_VAZAMENTO)
    if header_label_count > 0 and (has_sub_table or pct_numeric < 0.3):
        return 4, f"labels de header detectados: {sample_vals[:5]}"

    # Tipo 5: Nome da coluna parece um valor (timestamp, data)
    if re.match(r"^\d{4}[_-]\d{2}[_-]\d{2}", col_name):
        return 5, f"nome da coluna é uma data/timestamp: {col_name}"

    # Tipo 3: Metadado de sub-relatório (poucos valores, texto, sub_table_index)
    if has_sub_table and len(unique_vals) <= 5 and pct_numeric < 0.5:
        return 3, f"metadado de sub-relatório ({len(unique_vals)} valores distintos)"

    # Tipo 1: Dado legítimo esparso (numérico ou código)
    if pct_numeric >= 0.5 or len(unique_vals) > 5:
        return (
            1,
            f"dado legítimo esparso ({n_non_null} valores, {pct_numeric:.0%} numéricos)",
        )

    # Fallback: Tipo 4 para qualquer texto curto suspeito
    if all(len(v) < 30 for v in sample_vals) and pct_numeric < 0.3:
        return 4, f"textos curtos suspeitos: {sample_vals[:5]}"

    return 3, "indeterminado — baixa densidade, rever manualmente"


def limpar_colunas_pos_tratamento(
    df: pd.DataFrame, threshold: float = 0.95
) -> tuple[pd.DataFrame, dict]:
    """Remove colunas com alta taxa de NULL após o tratamento.

    Regras:
    - Tipo 2 (100% NULL): remove sempre
    - Tipo 1 (dado legítimo): preserva sempre
    - Demais tipos: remove se taxa de NULL > threshold

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame tratado (já com metadados).
    threshold : float
        Threshold de NULL para remoção (default 0.95 = 95%).

    Returns
    -------
    tuple[pd.DataFrame, dict]
        DataFrame limpo e dicionário com metadados da limpeza.
    """
    colunas_removidas: list[str] = []
    taxonomia: dict[str, str] = {}

    cols_to_drop = []
    for col in df.columns:
        if col in _COLUNAS_METADADOS:
            continue

        null_rate = df[col].isna().mean()
        tipo, evidencia = classificar_coluna_esparsa(df, col)
        taxonomia[col] = f"Tipo {tipo}: {evidencia}"

        if tipo == 2:
            cols_to_drop.append(col)
            colunas_removidas.append(f"{col} (Tipo 2: 100% NULL)")
        elif tipo != 1 and null_rate > threshold:
            cols_to_drop.append(col)
            colunas_removidas.append(
                f"{col} (Tipo {tipo}, {null_rate:.1%} NULL > {threshold:.0%})"
            )

    df_clean = df.drop(columns=cols_to_drop) if cols_to_drop else df

    info = {
        "colunas_removidas": colunas_removidas,
        "taxonomia_colunas": taxonomia,
        "n_cols_antes": len(df.columns),
        "n_cols_depois": len(df_clean.columns),
    }

    return df_clean, info


def detectar_linha_header_vazamento(row: pd.Series, col_names: list[str]) -> bool:
    """Detecta se uma linha é vazamento de multi-header do Excel.

    Uma linha é considerada vazamento quando:
    - Contém labels conhecidos de header (``_LABELS_HEADER_VAZAMENTO``)
    - Tem baixa densidade de dados numéricos (< 30% das colunas de dados
      têm valores numéricos)

    Parameters
    ----------
    row : pd.Series
        Uma linha do DataFrame.
    col_names : list[str]
        Nomes de todas as colunas.

    Returns
    -------
    bool
        True se a linha é vazamento de header.
    """
    data_cols = [c for c in col_names if c not in _COLUNAS_METADADOS]
    if not data_cols:
        return False

    values = [str(row[c]).strip().lower() for c in data_cols if pd.notna(row[c])]
    if not values:
        return False

    # Verifica labels conhecidos
    header_hits = sum(1 for v in values if v in _LABELS_HEADER_VAZAMENTO)

    # Verifica densidade numérica
    numeric_count = 0
    for v in values:
        try:
            float(v.replace(",", "."))
            numeric_count += 1
        except (ValueError, TypeError):
            pass

    pct_numeric = numeric_count / len(values) if values else 0

    # É vazamento se: tem labels de header E baixa densidade numérica
    return header_hits > 0 and pct_numeric < 0.3


def limpar_linhas_pos_tratamento(
    df: pd.DataFrame, threshold: float = 0.90
) -> tuple[pd.DataFrame, dict]:
    """Remove linhas majoritariamente vazias e linhas com vazamento de header.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame tratado (já com metadados e colunas limpas).
    threshold : float
        Threshold de NULL para considerar linha majoritariamente vazia
        (default 0.90 = 90%).

    Returns
    -------
    tuple[pd.DataFrame, dict]
        DataFrame limpo e dicionário com metadados da limpeza.
    """
    n_rows_antes = len(df)
    data_cols = [c for c in df.columns if c not in _COLUNAS_METADADOS]
    n_data_cols = len(data_cols)

    if n_data_cols == 0:
        return df, {"linhas_removidas": 0, "linhas_header_removidas": 0}

    linhas_removidas_null = 0
    linhas_removidas_header = 0
    mask_keep = []

    for idx, (_, row) in enumerate(df.iterrows()):
        # Preserva linhas com sub_table_index específico (separadores estruturais)
        if "sub_table_index" in df.columns:
            sti = row.get("sub_table_index")
            if pd.notna(sti) and str(sti) not in ("0", ""):
                mask_keep.append(True)
                continue

        # Verifica vazamento de header
        if detectar_linha_header_vazamento(row, list(df.columns)):
            linhas_removidas_header += 1
            mask_keep.append(False)
            continue

        # Verifica taxa de NULL
        null_count = sum(1 for c in data_cols if pd.isna(row[c]))
        null_rate = null_count / n_data_cols if n_data_cols > 0 else 0
        if null_rate > threshold:
            linhas_removidas_null += 1
            mask_keep.append(False)
        else:
            mask_keep.append(True)

    df_clean = df.loc[mask_keep].reset_index(drop=True)

    info = {
        "linhas_removidas": linhas_removidas_null,
        "linhas_header_removidas": linhas_removidas_header,
        "n_linhas_antes": n_rows_antes,
        "n_linhas_depois": len(df_clean),
    }

    return df_clean, info


def validar_metadados(df: pd.DataFrame, table_name: str) -> list[str]:
    """Valida colunas de metadados e retorna lista de warnings.

    Verifica se ``report_date``, ``institution``, ``profile`` e
    ``source_table`` estão preenchidos.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame tratado após limpeza.
    table_name : str
        Nome da tabela para contextualizar os warnings.

    Returns
    -------
    list[str]
        Lista de strings de warning. Vazia se tudo OK.
    """
    warnings: list[str] = []

    for col in ["report_date", "institution", "profile", "source_table"]:
        if col not in df.columns:
            warnings.append(f"{col}: coluna ausente")
            continue

        null_rate = df[col].isna().mean()
        if null_rate == 1.0:
            if col == "report_date":
                warnings.append(
                    f"report_date 100% NULL — extração de data falhou "
                    f"para '{table_name}'"
                )
            else:
                warnings.append(f"{col}: 100% NULL")
        elif null_rate > 0:
            n_null = int(df[col].isna().sum())
            warnings.append(f"{col}: {n_null}/{len(df)} NULL ({null_rate:.1%})")

    # Valida consistência de institution
    if "institution" in df.columns:
        unique_inst = df["institution"].dropna().unique()
        for inst in unique_inst:
            inst_str = str(inst).lower()
            if inst_str not in ("bb", "caixa", "unknown"):
                warnings.append(f"institution com valor inesperado: '{inst}'")

    return warnings


# ──────────────────────────────────────────────────────────────────────
# Group 1: Column Name Normalization
# ──────────────────────────────────────────────────────────────────────


def normalizar_nome_coluna(nome: str) -> str:
    """Normaliza nome de coluna: lowercase, strip, substitui espaços/especiais
    por ``_``, remove acentos.

    Usa ``unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')``
    para remover acentos. Depois ``re.sub(r'[^a-z0-9_]', '_', nome)`` para limpar.
    Múltiplos underscores consecutivos são colapsados em um só.
    Não pode resultar em nome vazio — se resultar, retorna ``'col'``.

    Examples
    --------
    >>> normalizar_nome_coluna('Código do IBGE')
    'codigo_do_ibge'
    >>> normalizar_nome_coluna('Valor (R$)')
    'valor_r'
    >>> normalizar_nome_coluna('   ')
    'col'
    """
    nome = nome.strip().lower()
    # Remove acentos via NFKD
    nome = unicodedata.normalize("NFKD", nome).encode("ASCII", "ignore").decode("ASCII")
    # Substitui tudo que não for letra, dígito ou underscore por '_'
    nome = re.sub(r"[^a-z0-9_]", "_", nome)
    # Colapsa múltiplos underscores consecutivos
    nome = re.sub(r"_+", "_", nome)
    # Remove underscores nas pontas
    nome = nome.strip("_")
    # Fallback para nome vazio
    if not nome or nome.isdigit():
        return "col"
    return nome


# ──────────────────────────────────────────────────────────────────────
# Group 2: Decimal Separator Detection & Correction
# ──────────────────────────────────────────────────────────────────────


def detectar_separador_decimal(series: pd.Series) -> bool:
    """Amostra 20 valores não-nulos; se >50% contêm vírgula, retorna
    ``True`` (separador decimal é vírgula).

    Parameters
    ----------
    series : pd.Series
        Série de dados (espera-se strings ou numéricos).

    Returns
    -------
    bool
        ``True`` se vírgula decimal foi detectada como separador predominante.
    """
    sample = series.dropna().astype(str).head(20)
    if len(sample) == 0:
        return False
    n_comma = sum(1 for v in sample if "," in v)
    return n_comma / len(sample) > 0.5


def converter_decimal(series: pd.Series) -> pd.Series:
    """Se a série tem vírgula decimal, substitui ',' por '.' e converte
    para ``float64`` via ``pd.to_numeric``.

    Se a série não contém vírgula, retorna ``pd.to_numeric(series, errors='coerce')``
    diretamente. Falhas de conversão viram ``NaN``.

    Parameters
    ----------
    series : pd.Series
        Série de string ou numérica.

    Returns
    -------
    pd.Series
        Série convertida para ``float64``.
    """
    if detectar_separador_decimal(series):
        series = series.astype(str).str.replace(",", ".", regex=False)
    return pd.to_numeric(series, errors="coerce").astype("float64")


# ──────────────────────────────────────────────────────────────────────
# Group 3: Date Parsing
# ──────────────────────────────────────────────────────────────────────


def _parece_numerico(valor: str) -> bool:
    """True se o valor parece ser um número (ID), não uma data."""
    v = valor.strip()
    # Elimina separadores comuns de data
    if "/" in v or "-" in v:
        return False
    # Se é só dígitos com 1-4 caracteres (IDs curtos), parece numérico
    return v.isdigit() and len(v) <= 4


def detectar_formato_data(series: pd.Series) -> str | None:
    """Amostra 20 valores string não-nulos. Testa formatos ISO (``%Y-%m-%d``)
    e BR (``%d/%m/%Y``). Retorna o formato (``'iso'``, ``'br'``) que cobre
    maior % de valores, ou ``None`` se nenhum > 50%.

    Filtra valores que parecem numéricos (podem ser IDs, não datas).

    Parameters
    ----------
    series : pd.Series
        Série candidata a conter datas.

    Returns
    -------
    str or None
        ``'iso'``, ``'br'`` ou ``None``.
    """
    sample = series.dropna().astype(str).head(20)
    # Filtra valores que parecem IDs numéricas
    sample = sample[~sample.apply(_parece_numerico)]
    if len(sample) == 0:
        return None

    n_iso = 0
    n_br = 0
    for val in sample:
        v = val.strip()
        # Tenta ISO: YYYY-MM-DD
        try:
            pd.Timestamp(v)
            n_iso += 1
            continue
        except (ValueError, TypeError):
            pass
        # Tenta BR: DD/MM/YYYY
        try:
            pd.Timestamp(v, dayfirst=True)
            n_br += 1
        except (ValueError, TypeError):
            pass

    total = len(sample)
    ratio_iso = n_iso / total
    ratio_br = n_br / total

    if ratio_iso > 0.5:
        return "iso"
    if ratio_br > 0.5:
        return "br"
    return None


def parse_datas(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Para colunas cujo nome normalizado contém ``'dat_'``, ``'dt_'`` ou
    ``'data_'``: detecta formato, aplica ``pd.to_datetime`` com
    ``dayfirst=(formato=='br')``.

    Colunas com > 10% de falha no parse mantêm o tipo original e são
    reportadas na lista retornada.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame com colunas a serem analisadas.

    Returns
    -------
    tuple[pd.DataFrame, list[str]]
        ``(df modificado, lista de colunas com falha)``.
    """
    df = df.copy()
    falhas: list[str] = []

    for col in df.columns:
        col_str = str(col)
        nome_norm = normalizar_nome_coluna(col_str)
        # Verifica se o nome normalizado sugere data
        if not any(token in nome_norm for token in ("dat_", "dt_", "data_")):
            continue

        fmt = detectar_formato_data(df[col])
        if fmt is None:
            continue  # sem formato detectado, mantém original

        dayfirst = fmt == "br"
        original_col = df[col].copy()  # backup para possível reversão
        original_count = original_col.notna().sum()
        df[col] = pd.to_datetime(df[col], dayfirst=dayfirst, errors="coerce")

        # Verifica falha: mais de 10% dos valores originais não-nulos se
        # tornaram NaT
        parsed_count = df[col].notna().sum()
        if original_count > 0:
            fail_ratio = 1 - (parsed_count / original_count)
            if fail_ratio > 0.1:
                falhas.append(col_str)
                # Reverte para a coluna original (parse falhou)
                df[col] = original_col

    return df, falhas


# ──────────────────────────────────────────────────────────────────────
# Group 4: Encoding Repair
# ──────────────────────────────────────────────────────────────────────


def reparar_encoding(valor: object) -> object:
    """Se o valor é string, tenta ``charset-normalizer`` para detectar
    encoding. Se detectado não-UTF-8 com confiança > 0.7, redecodifica.
    Como fallback, aplica ``ftfy.fix_text()``.

    Se o valor não é string, retorna inalterado.

    Parameters
    ----------
    valor : object
        Valor a ser reparado.

    Returns
    -------
    object
        Valor com encoding reparado (se aplicável).
    """
    if not isinstance(valor, str):
        return valor

    # Tenta charset-normalizer
    raw = valor.encode("utf-8", errors="replace")
    result = detect(raw)
    if result is not None and result["encoding"] is not None:
        encoding_detectado = result["encoding"].lower()
        confidence: float = result.get("confidence", 0.0) or 0.0
        if encoding_detectado not in ("utf_8", "utf-8", "ascii") and confidence > 0.7:
            try:
                return raw.decode(encoding_detectado, errors="replace")
            except (LookupError, ValueError, UnicodeDecodeError):
                pass

    # Fallback: ftfy
    return ftfy.fix_text(valor)


def reparar_encoding_df(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica ``reparar_encoding`` a todas as colunas de texto
    (``object`` dtype).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame com possíveis problemas de encoding.

    Returns
    -------
    pd.DataFrame
        DataFrame com encoding reparado.
    """
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(reparar_encoding)
    return df


# ──────────────────────────────────────────────────────────────────────
# Group 5: Type Canonization
# ──────────────────────────────────────────────────────────────────────


def carregar_inventario_colunas(csv_path: Path | None = None) -> dict[str, str]:
    """Lê o arquivo ``columns_*.csv`` (com aspas, colunas: ``table_name``,
    ``column_name``, ``data_type``).

    Retorna ``dict``: ``{column_name_normalizado: tipo_mais_frequente}``.
    O tipo mais frequente é determinado por contagem entre todas as tabelas.
    Aplica ``normalizar_nome_coluna`` no ``column_name`` antes de agrupar.

    Parameters
    ----------
    csv_path : Path or None
        Caminho para o CSV de inventário. Se ``None``, usa o caminho padrão
        ``data/columns_202605211425_perfil_cidades_dados_historicos.csv``
        relativo ao diretório raiz do projeto.

    Returns
    -------
    dict[str, str]
        Mapeamento ``{nome_normalizado: tipo_mais_frequente}``.
    """
    if csv_path is None:
        root = Path(__file__).resolve().parents[2]
        csv_path = (
            root / "data" / "columns_202605211425_perfil_cidades_dados_historicos.csv"
        )

    df_schema = pd.read_csv(csv_path, encoding="utf-8")

    # Normaliza os nomes de coluna
    df_schema["nome_norm"] = df_schema["column_name"].apply(normalizar_nome_coluna)

    # Agrupa por nome normalizado e coleta tipos
    inventario: dict[str, str] = {}
    for nome_norm, group in df_schema.groupby("nome_norm"):
        tipo_counts: Counter = Counter(group["data_type"].dropna())
        if tipo_counts:
            inventario[nome_norm] = tipo_counts.most_common(1)[0][0]
        else:
            inventario[nome_norm] = "text"

    return inventario


def tipo_canonico_manual(nome_normalizado: str) -> str | None:
    """Aplica regras manuais de tipo baseadas no nome da coluna.

    Regras (em ordem de prioridade):

    - contém ``'cnpj'`` → ``'str'``
    - contém ``'cod_'`` ou ``'nr_'`` ou ``'nu_'`` → ``'str'`` (códigos
      identificadores)
    - contém ``'vlr_'`` ou ``'valor_'`` ou ``'total_'`` → ``'float64'``
    - contém ``'qtd_'`` ou ``'qt_'`` ou ``'unidades'`` → ``'Int64'``
      (nullable int)
    - contém ``'dat_'`` ou ``'dt_'`` ou ``'data_'`` → ``'datetime64[ns]'``

    Retorna ``None`` se nenhuma regra aplicar.

    Parameters
    ----------
    nome_normalizado : str
        Nome de coluna já normalizado (lowercase, underscores, sem acentos).

    Returns
    -------
    str or None
        Tipo canônico ou ``None``.
    """
    nome = nome_normalizado.lower()

    if "cnpj" in nome:
        return "str"

    if nome.startswith("cod_") or nome.startswith("nr_") or nome.startswith("nu_"):
        return "str"

    if "vlr_" in nome or "valor_" in nome or "total_" in nome:
        return "float64"

    if "qtd_" in nome or "qt_" in nome or "unidades" in nome:
        return "Int64"

    if "dat_" in nome or "dt_" in nome or "data_" in nome:
        return "datetime64[ns]"

    return None


def tipo_canonico_auto(nome_normalizado: str, inventario: dict[str, str]) -> str:
    """Busca o tipo mais frequente no inventário para esta coluna.

    Se não encontrado, retorna ``'str'`` como safe default.

    Parameters
    ----------
    nome_normalizado : str
        Nome de coluna normalizado.
    inventario : dict[str, str]
        Dict de ``{nome_normalizado: tipo_mais_frequente}`` gerado por
        ``carregar_inventario_colunas``.

    Returns
    -------
    str
        Tipo canônico ou ``'str'``.
    """
    return inventario.get(nome_normalizado, "str")


def _coerce_coluna(series: pd.Series, target_type: str) -> pd.Series:
    """Tenta converter uma série para o tipo alvo.

    Parameters
    ----------
    series : pd.Series
        Série a ser convertida.
    target_type : str
        Tipo alvo (``'str'``, ``'float64'``, ``'Int64'``,
        ``'datetime64[ns]'``).

    Returns
    -------
    pd.Series
        Série convertida (ou original se falhar).
    """
    try:
        if target_type == "str":
            return series.astype(str)
        if target_type == "float64":
            return pd.to_numeric(series, errors="coerce").astype("float64")
        if target_type == "Int64":
            numeric = pd.to_numeric(series, errors="coerce")
            return numeric.astype("Int64")
        if target_type == "datetime64[ns]":
            return pd.to_datetime(series, errors="coerce", dayfirst=True)
    except (ValueError, TypeError, OverflowError):
        pass
    return series


def aplicar_tipos_canonicos(
    df: pd.DataFrame, inventario: dict[str, str]
) -> tuple[pd.DataFrame, list[str]]:
    """Para cada coluna: normaliza nome, determina tipo canônico
    (manual -> auto fallback), aplica cast.

    Colunas que falham na coerção mantêm o tipo original.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame com nomes de coluna originais.
    inventario : dict[str, str]
        Dict gerado por ``carregar_inventario_colunas``.

    Returns
    -------
    tuple[pd.DataFrame, list[str]]
        ``(df com nomes normalizados e tipos aplicados, lista de warnings)``.
    """
    df = df.copy()
    warnings: list[str] = []

    # Dicionário de renomeação: nome_original -> nome_normalizado
    rename_map: dict[str, str] = {}
    for col in df.columns:
        col_str = str(col)
        nome_norm = normalizar_nome_coluna(col_str)
        if nome_norm != col_str:
            rename_map[col_str] = nome_norm

    # Aplica renomeação
    if rename_map:
        df = df.rename(columns=rename_map)
        warnings.append(f"Colunas renomeadas: {rename_map}")

    # Agora aplica tipos canônicos
    for col in df.columns:
        col_str = str(col)
        # Determina tipo: manual primeiro, auto como fallback
        tipo = tipo_canonico_manual(col_str)
        if tipo is None:
            tipo = tipo_canonico_auto(col_str, inventario)

        original_dtype = df[col].dtype
        nova_series = _coerce_coluna(df[col], tipo)

        # Verifica se a coerção foi bem-sucedida
        # Para tipos numéricos, verifica se não perdeu muitos valores
        if tipo in ("float64", "Int64"):
            n_original = df[col].notna().sum()
            n_convertido = nova_series.notna().sum()
            if n_original > 0 and n_convertido == 0 and n_original > 5:
                warnings.append(
                    f"Coerção falhou para coluna '{col_str}': "
                    f"todos os {n_original} valores se tornaram NaN. "
                    f"Mantendo tipo original ({original_dtype})."
                )
                continue  # mantém original

        df[col] = nova_series

    return df, warnings


# ──────────────────────────────────────────────────────────────────────
# Group 6: Profile Detection
# ──────────────────────────────────────────────────────────────────────


def _tem_chave_repetida(df: pd.DataFrame) -> bool:
    """Verifica se há colunas com nome contendo ``'apf'``, ``'cod_'``,
    ``'contrato'``, ``'nr_'`` onde valores se repetem em múltiplas linhas
    (> 20% de duplicação).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a ser analisado.

    Returns
    -------
    bool
        ``True`` se alguma coluna-chave tem mais de 20% de valores
        duplicados.
    """
    keywords = ("apf", "cod_", "contrato", "nr_")
    for col in df.columns:
        col_str = str(col).lower()
        if any(kw in col_str for kw in keywords):
            series = df[col].dropna()
            if len(series) == 0:
                continue
            n_dup = series.duplicated(keep=False).sum()
            if n_dup / len(series) > 0.2:
                return True
    return False


def _tem_linhas_total(df: pd.DataFrame) -> bool:
    """Verifica se a primeira coluna contém valores como ``'Total'``,
    ``'AC Total'``, etc.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a ser analisado.

    Returns
    -------
    bool
        ``True`` se a primeira coluna tem linhas de total.
    """
    if len(df.columns) == 0 or len(df) == 0:
        return False
    first_col = df.iloc[:, 0]
    for val in first_col.dropna().astype(str):
        v = val.strip().lower()
        if "total" in v:
            return True
    return False


def classificar_perfil(df: pd.DataFrame) -> str:
    """Classifica a tabela em um dos 5 perfis.

    Heurística:

    1. ``col_count <= 3`` -> ``'lookup'``
    2. ``col_count`` in ``[4, 5, 6]`` AND ``_tem_chave_repetida``
       -> ``'event_level'``
    3. ``col_count`` in ``[4, 5, 6]`` AND NOT ``_tem_chave_repetida``
       -> ``'colunar_denso'``
    4. ``col_count`` in ``[7, 8, 9]`` AND ``_tem_linhas_total``
       -> ``'agregado_uf'``
    5. ``col_count`` in ``[7, 8, 9]`` AND NOT ``_tem_linhas_total``
       -> ``'colunar_denso'``
    6. ``col_count >= 13`` -> ``'colunar_denso'``
    7. else -> ``'colunar_denso'`` (default conservador)

    Returns
    -------
    str
        ``'lookup'`` | ``'event_level'`` | ``'agregado_uf'`` |
        ``'agregado_municipal'`` | ``'colunar_denso'``
    """
    n_cols = len(df.columns)

    # 1. lookup
    if n_cols <= 3:
        return "lookup"

    # 2-3. event_level / colunar_denso (4-6 colunas)
    if 4 <= n_cols <= 6:
        if _tem_chave_repetida(df):
            return "event_level"
        return "colunar_denso"

    # 4-5. agregado_uf / colunar_denso (7-9 colunas)
    if 7 <= n_cols <= 9:
        if _tem_linhas_total(df):
            return "agregado_uf"
        return "colunar_denso"

    # 6. colunar_denso (>= 13 colunas)
    if n_cols >= 13:
        return "colunar_denso"

    # Caso intermediário (10-12 colunas): default conservador
    return "colunar_denso"


# ──────────────────────────────────────────────────────────────────────
# Group 7: Period Extraction & Institution
# ──────────────────────────────────────────────────────────────────────


def extrair_periodo_filename(table_name: str) -> str | None:
    """Extrai data do nome do arquivo e normaliza para ``'YYYY-MM-DD'``.

    Padrões testados em ordem (primeiro match vence):

    0a. ``historico_recente_(20\d{2})(\d{2})_``: ex ``historico_recente_202406_snh_...``
    0b. ``o_recente_(20\d{2})(\d{2})_``: ex ``o_recente_202406_snh_...``
    0c. Prefixos truncados: ``^(ecente|storico)_(20\d{2})_(\d{2})_``
    1. ``DD_MM_YYYY`` no final: ``r'_(\d{2})_(\d{2})_(\d{4})$'``
    2. ``YYYYMMDD`` no final: ``r'_(\d{8})$'``
    3. ``DDMMYYYY`` no final: ``r'(\d{8})$'`` (se não capturado por #2,
       tenta parse como data)
    4. ``YYYYMM`` no início: ``r'^(\d{6})_'``
    5. ``YYYY_MM`` no meio: ``r'_(\d{4})_(\d{2})_'``

    Se nenhum padrão match, retorna ``None``.

    Parameters
    ----------
    table_name : str
        Nome da tabela (sem extensão).

    Returns
    -------
    str or None
        Data no formato ``'YYYY-MM-DD'`` ou ``None``.
    """
    name = table_name.strip()

    # 0a. historico_recente_YYYYMM_... (ex: historico_recente_202406_snh_...)
    m = re.search(r"historico_recente_(20\d{2})(\d{2})_", name)
    if m:
        year, month = m.group(1), m.group(2)
        try:
            return f"{year}-{month}-01"
        except (ValueError, TypeError):
            pass

    # 0b. o_recente_YYYYMM_... (ex: o_recente_202406_snh_...)
    m = re.search(r"o_recente_(20\d{2})(\d{2})_", name)
    if m:
        year, month = m.group(1), m.group(2)
        try:
            return f"{year}-{month}-01"
        except (ValueError, TypeError):
            pass

    # 0c. Prefixos truncados: ecente_YYYY_MM_... ou storico_YYYY_MM_...
    m = re.search(r"^(ecente|storico)_(20\d{2})_(\d{2})_", name)
    if m:
        year, month = m.group(2), m.group(3)
        try:
            return f"{year}-{month}-01"
        except (ValueError, TypeError):
            pass

    # 1. DD_MM_YYYY no final
    m = re.search(r"_(\d{2})_(\d{2})_(\d{4})$", name)
    if m:
        day, month, year = m.group(1), m.group(2), m.group(3)
        try:
            return f"{year}-{month}-{day}"
        except (ValueError, TypeError):
            pass

    # 2. YYYYMMDD no final (especificamente após underscore)
    m = re.search(r"_(\d{8})$", name)
    if m:
        candidate = m.group(1)
        try:
            ts = pd.to_datetime(candidate, format="%Y%m%d")
            return ts.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

    # 3. DDMMYYYY genérico (8 dígitos, tenta parse com dayfirst)
    m = re.search(r"(\d{8})$", name)
    if m:
        candidate = m.group(1)
        # Tenta DDMMYYYY (formato brasileiro) primeiro
        try:
            ts = pd.to_datetime(candidate, format="%d%m%Y")
            return ts.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
        # Fallback: YYYYMMDD
        try:
            ts = pd.to_datetime(candidate, format="%Y%m%d")
            return ts.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

    # 3b. Meses em português: _DDmesAAAA ou _DDmesAA (ex: _31dez09, _11jan2011)
    MESES_PT: dict[str, int] = {
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }
    # _DDmesAAAA (ex: _11jan2011)
    m = re.search(r"_(\d{2})([a-z]{3})(\d{4})", name.lower())
    if m:
        day_str, mes_abbr, year_str = m.group(1), m.group(2), m.group(3)
        if mes_abbr in MESES_PT:
            try:
                day = int(day_str)
                if 1 <= day <= 31:
                    return f"{year_str}-{MESES_PT[mes_abbr]:02d}-{day:02d}"
            except (ValueError, TypeError):
                pass
    # _DDmesAA (ex: _31dez09)
    m = re.search(r"_(\d{2})([a-z]{3})(\d{2})", name.lower())
    if m:
        day_str, mes_abbr, year_str = m.group(1), m.group(2), m.group(3)
        if mes_abbr in MESES_PT:
            try:
                day = int(day_str)
                if 1 <= day <= 31:
                    year = 2000 + int(year_str)
                    return f"{year}-{MESES_PT[mes_abbr]:02d}-{day:02d}"
            except (ValueError, TypeError):
                pass

    # 3c. Meses em português: _mesAAAA, _mesAA, _mes_YYYY (ex: _abr2018, _dez17, _jan_2017, _abr2018_v2)
    for abbr, mes_num in MESES_PT.items():
        # _mesAAAA ou _mesAAAA_vN (ex: _abr2018, _abr2018_v2)
        m = re.search(rf"_({abbr})(20\d{{2}})(?:_v?\d+)?$", name.lower())
        if m:
            return f"{m.group(2)}-{mes_num:02d}-01"
        # _mesAA ou _mesAA_vN (ex: _dez17)
        m = re.search(rf"_({abbr})(\d{{2}})(?:_v?\d+)?$", name.lower())
        if m:
            return f"20{m.group(2)}-{mes_num:02d}-01"
        # _mes_YYYY (ex: _jan_2017)
        m = re.search(rf"_({abbr})_(20\d{{2}})$", name.lower())
        if m:
            return f"{m.group(2)}-{mes_num:02d}-01"

    # 3d. Truncation-resilient: _DDMMYYYY or _YYYYMMDD anywhere in name
    # (not just at end, for names truncated at 63 chars)
    m = re.search(r"_(\d{8})(?:_|$)", name)
    if m:
        candidate = m.group(1)
        try:
            ts = pd.to_datetime(candidate, format="%d%m%Y")
            return ts.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
        try:
            ts = pd.to_datetime(candidate, format="%Y%m%d")
            return ts.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

    # 3e. YYYY_MM_DD no meio (data específica com underscores, ex: _2014_10_30_)
    # Captura data exata que aparece em sufixos de tabelas BB como
    # bb_2014_10_outubro_2014_10_30_min_cidades_pj
    m = re.search(r"_(\d{4})_(\d{2})_(\d{2})_", name)
    if m:
        year, month, day = m.group(1), m.group(2), m.group(3)
        try:
            d = int(day)
            if 1 <= d <= 31:
                return f"{year}-{month}-{day}"
        except (ValueError, TypeError):
            pass

    # 4. YYYYMM no início (6 dígitos no começo)
    m = re.search(r"^(\d{6})(?:_|$)", name)
    if m:
        candidate = m.group(1)
        try:
            ts = pd.Timestamp(candidate + "01")
            return ts.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

    # 5. YYYY_MM no meio
    m = re.search(r"_(\d{4})_(\d{2})_", name)
    if m:
        year, month = m.group(1), m.group(2)
        try:
            return f"{year}-{month}-01"
        except (ValueError, TypeError):
            pass

    return None


def inferir_instituicao(table_name: str) -> str:
    """Infere instituição pelo prefixo do nome da tabela.

    - Começa com ``'bb_'`` -> ``'bb'``
    - Contém ``'caixa_'`` -> ``'caixa'``
    - Caso contrário -> ``'unknown'``

    Parameters
    ----------
    table_name : str
        Nome da tabela (sem extensão).

    Returns
    -------
    str
        ``'bb'``, ``'caixa'`` ou ``'unknown'``.
    """
    name = table_name.lower().strip()
    if name.startswith("bb_"):
        return "bb"
    if "caixa_" in name:
        return "caixa"
    return "unknown"


# ──────────────────────────────────────────────────────────────────────
# Group 8: Metadata & Pipeline
# ──────────────────────────────────────────────────────────────────────


def adicionar_metadados(
    df: pd.DataFrame,
    table_name: str,
    content_hash: str,
    perfil: str,
    instituicao: str,
    report_date: str | None,
) -> pd.DataFrame:
    """Adiciona colunas de metadados ao DataFrame:

    - ``source_table`` — nome da tabela original
    - ``report_date`` — data de referência (ou ``pd.NA``)
    - ``institution`` — instituição (``'bb'``, ``'caixa'``, ``'unknown'``)
    - ``profile`` — perfil classificado
    - ``content_hash`` — hash de conteúdo (omitido se string vazia)

    Todas as colunas são do tipo ``string``.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame tratado.
    table_name : str
        Nome da tabela de origem.
    content_hash : str
        Hash do conteúdo (opcional, vazio para omitir).
    perfil : str
        Perfil classificado.
    instituicao : str
        Instituição inferida.
    report_date : str or None
        Data de referência no formato ``'YYYY-MM-DD'`` ou ``None``.

    Returns
    -------
    pd.DataFrame
        DataFrame com colunas de metadados adicionadas.
    """
    df = df.copy()
    df["source_table"] = table_name
    df["report_date"] = report_date if report_date is not None else pd.NA
    df["institution"] = instituicao
    df["profile"] = perfil
    if content_hash:
        df["content_hash"] = content_hash
    return df


def tratar_bem_formada(
    table_name: str,
    df: pd.DataFrame,
    content_hash: str = "",
    inventario_colunas: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Pipeline completo de tratamento para uma tabela ``bem_formada``.

    Orquestra:

    1. ``classificar_perfil(df)`` -> perfil
    2. Para ``colunar_denso`` e ``agregado_uf``:
       a. ``aplicar_tipos_canonicos(df, inventario)`` -> df normalizado
       b. ``parse_datas(df)`` -> df com datas
       c. ``converter_decimal`` em colunas float
       d. ``reparar_encoding_df(df)``
    3. Para ``lookup``: apenas ``reparar_encoding_df``
    4. Para ``event_level``: normalizar nomes + parse datas +
       reparar encoding
    5. Para ``agregado_uf``: adicionalmente adiciona coluna
       ``is_aggregate`` (bool)
    6. ``extrair_periodo_filename`` + ``inferir_instituicao``
    7. ``adicionar_metadados``

    Parameters
    ----------
    table_name : str
        Nome da tabela (sem extensão).
    df : pd.DataFrame
        DataFrame carregado (já com índice descartado).
    content_hash : str, optional
        Hash do conteúdo (string vazia = omitir).
    inventario_colunas : dict[str, str] or None, optional
        Dict de tipos canônicos. Se ``None``, carrega do caminho padrão:
        ``data/columns_202605211425_perfil_cidades_dados_historicos.csv``
        no diretório de dados do projeto.

    Returns
    -------
    tuple[pd.DataFrame, dict]
        ``(df_tratado, info_dict)`` onde ``info_dict`` contém:
        ``{profile, institution, report_date, n_cols_original,
        n_cols_tratado, n_rows, warnings: [...]}``.
    """
    df = df.copy()
    n_cols_original = len(df.columns)
    n_rows = len(df)
    warnings: list[str] = []

    # 0. Limpeza pré-tratamento (remove linhas/colunas vazias)
    df = clean_dataframe(df)
    n_rows_cleaned = len(df) - n_rows
    if n_rows_cleaned != 0:
        warnings.append(
            f"{abs(n_rows_cleaned)} linhas vazias/dash removidas na limpeza"
        )
    if len(df.columns) != n_cols_original:
        warnings.append(
            f"{n_cols_original - len(df.columns)} colunas 100% vazias removidas na limpeza"
        )
    n_rows = len(df)

    # 1. Classifica perfil
    perfil = classificar_perfil(df)

    # 2. Pipeline por perfil
    if perfil in ("colunar_denso", "agregado_uf"):
        # a. Carrega inventário se necessário e aplica tipos canônicos
        if inventario_colunas is None:
            inventario_colunas = carregar_inventario_colunas()
        df, warn_tipos = aplicar_tipos_canonicos(df, inventario_colunas)
        warnings.extend(warn_tipos)

        # b. Parse de datas
        df, falhas_datas = parse_datas(df)
        for col in falhas_datas:
            warnings.append(
                f"Falha no parse de datas para coluna '{col}' "
                "(>10% de valores nao convertidos)"
            )

        # c. Converter decimal em colunas float
        for col in df.columns:
            if df[col].dtype == "float64":
                df[col] = converter_decimal(df[col])

        # d. Reparo de encoding
        df = reparar_encoding_df(df)

        # e. Coluna is_aggregate para agregado_uf
        if perfil == "agregado_uf":
            df["is_aggregate"] = True

    elif perfil == "lookup":
        # Apenas reparo de encoding
        df = reparar_encoding_df(df)

    elif perfil == "event_level":
        # Normalizar nomes + parse datas + reparar encoding
        if inventario_colunas is None:
            inventario_colunas = carregar_inventario_colunas()
        df, warn_tipos = aplicar_tipos_canonicos(df, inventario_colunas)
        warnings.extend(warn_tipos)

        df, falhas_datas = parse_datas(df)
        for col in falhas_datas:
            warnings.append(
                f"Falha no parse de datas para coluna '{col}' "
                "(>10% de valores nao convertidos)"
            )

        df = reparar_encoding_df(df)

    # 3. Extrai período e instituição
    report_date = extrair_periodo_filename(table_name)
    instituicao = inferir_instituicao(table_name)

    # 3a. Normaliza data_de_movimento se presente
    dm_value: str | None = None
    if "data_de_movimento" in df.columns:
        # Normaliza datas DD/MM/YYYY -> YYYY-MM-DD
        def _norm_dm(val: Any) -> str | None:
            if pd.isna(val):
                return None
            s = str(val).strip()
            # Tenta DD/MM/YYYY
            m = re.match(r"(\d{2})/(\d{2})/(\d{4})", s)
            if m:
                return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
            # Tenta YYYY-MM-DD (já normalizado)
            if re.match(r"\d{4}-\d{2}-\d{2}", s):
                return s
            return s

        df["data_de_movimento"] = df["data_de_movimento"].apply(_norm_dm)
        non_null = df["data_de_movimento"].dropna()
        if len(non_null) > 0:
            dm_value = str(non_null.iloc[0])

    # 4. Adiciona metadados
    df = adicionar_metadados(
        df=df,
        table_name=table_name,
        content_hash=content_hash,
        perfil=perfil,
        instituicao=instituicao,
        report_date=report_date,
    )

    # 5. Monta info_dict
    info: dict[str, Any] = {
        "profile": perfil,
        "institution": instituicao,
        "report_date": report_date,
        "data_de_movimento": dm_value,
        "n_cols_original": n_cols_original,
        "n_cols_tratado": len(df.columns),
        "n_rows": n_rows,
        "warnings": warnings,
    }

    return df, info


# ──────────────────────────────────────────────────────────────────────
# Roteador: tratar_tabela
# ──────────────────────────────────────────────────────────────────────


# Categorias que são cabeçalho deslocado
_CABECALHO_DESLOCADO = {
    "cabecalho_na_primeira_linha_1",
    "cabecalho_na_primeira_linha_2",
    "cabecalho_na_segunda_linha",
    "cabecalho_na_terceira_linha_1",
    "cabecalho_na_terceira_linha_2",
    "cabecalho_composto_1",
    "cabecalho_composto_2",
}


def tratar_tabela(
    table_name: str,
    formacao: str,
    df: pd.DataFrame,
    content_hash: str = "",
    data_dir: Path | None = None,
    inferir_nomes: bool = True,
) -> tuple[pd.DataFrame | None, dict]:
    """Roteia para o pipeline de tratamento correto baseado em ``formacao``.

    Args:
        table_name: Nome da tabela (sem extensao).
        formacao: Categoria estrutural (ex: ``'bem_formada'``,
            ``'sem_cabecalho'``).
        df: DataFrame bruto carregado.
        content_hash: Hash MD5 do conteudo para deduplicacao.
        data_dir: Diretorio base de dados (opcional).
        inferir_nomes: Se ``True`` (default), infere nomes descritivos
            para tabelas ``sem_cabecalho`` quando o matching de
            referencia falhar.

    Returns:
        ``(df_tratado, info_dict)`` para tabelas tratadas, ou
        ``(None, info_dict)`` para tabelas descartadas.
    """
    # Importes locais para evitar dependência circular
    from classificacao.tratamento_especiais import (
        tratar_dados_sem_utilidade,
        tratar_separador_pipe,
        tratar_sem_cabecalho,
        tratar_vazia,
    )
    from classificacao.tratamento_subtabelas import (
        tratar_sub_tabelas_1,
        tratar_sub_tabelas_2,
        tratar_sub_tabelas_3,
        tratar_sub_tabelas_4,
    )
    from classificacao.tratamento_cabecalho import tratar_cabecalho_deslocado

    # Descartes
    if formacao == "vazia":
        info = tratar_vazia(table_name)
        info["table_name"] = table_name
        info["status"] = "discarded"
        return None, info

    if formacao == "dados_sem_utilidade":
        info = tratar_dados_sem_utilidade(table_name)
        info["table_name"] = table_name
        info["status"] = "discarded"
        return None, info

    # Casos especiais
    if formacao == "separador_|":
        df_t, info = tratar_separador_pipe(table_name, data_dir=data_dir)
        info["table_name"] = table_name
        info["status"] = "treated"
        info["content_hash"] = content_hash
        if df_t is not None and len(df_t) > 0:
            df_t, clean_info = _aplicar_limpeza_pos_tratamento(df_t, table_name, info)
            info.update(clean_info)
        return df_t, info

    if formacao == "sem_cabecalho":
        df_t, info = tratar_sem_cabecalho(
            table_name, df, data_dir=data_dir, inferir_nomes=inferir_nomes
        )
        info["table_name"] = table_name
        info["status"] = "treated"
        info["content_hash"] = content_hash
        if df_t is not None and len(df_t) > 0:
            df_t, clean_info = _aplicar_limpeza_pos_tratamento(df_t, table_name, info)
            info.update(clean_info)
        return df_t, info

    # Sub-tabelas
    if formacao == "sub_tabelas_1":
        df_t, info = tratar_sub_tabelas_1(table_name, df)
    elif formacao == "sub_tabelas_2":
        df_t, info = tratar_sub_tabelas_2(table_name, df)
    elif formacao == "sub_tabelas_3":
        df_t, info = tratar_sub_tabelas_3(table_name, df)
    elif formacao == "sub_tabelas_4":
        df_t, info = tratar_sub_tabelas_4(table_name, df)
    elif formacao in _CABECALHO_DESLOCADO:
        df_t, info = tratar_cabecalho_deslocado(table_name, df, formacao)
    elif formacao == "bem_formada":
        df_t, info = tratar_bem_formada(table_name, df, content_hash=content_hash)
    else:
        # Fallback: tenta bem_formada
        df_t, info = tratar_bem_formada(table_name, df, content_hash=content_hash)
        info["warnings"] = info.get("warnings", []) + [
            f"formacao '{formacao}' não reconhecida, aplicado pipeline bem_formada"
        ]

    info["table_name"] = table_name
    info["status"] = "treated"
    info["content_hash"] = content_hash

    # Aplica limpeza pós-tratamento e validação de metadados
    if df_t is not None and len(df_t) > 0:
        df_t, clean_info = _aplicar_limpeza_pos_tratamento(df_t, table_name, info)
        info.update(clean_info)

    return df_t, info


def _aplicar_limpeza_pos_tratamento(
    df: pd.DataFrame,
    table_name: str,
    info: dict,
    threshold_colunas: float = 0.95,
    threshold_linhas: float = 0.90,
) -> tuple[pd.DataFrame, dict]:
    """Aplica pipeline de limpeza pós-tratamento: colunas, linhas, validação.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame tratado com metadados.
    table_name : str
        Nome da tabela.
    info : dict
        info_dict existente (será enriquecido).
    threshold_colunas : float
        Threshold de NULL para remoção de colunas.
    threshold_linhas : float
        Threshold de NULL para remoção de linhas.

    Returns
    -------
    tuple[pd.DataFrame, dict]
        DataFrame limpo e dicionário com dados da limpeza.
    """
    result_info: dict[str, Any] = {}

    # 1. Limpeza de colunas
    df, col_info = limpar_colunas_pos_tratamento(df, threshold=threshold_colunas)
    result_info.update(col_info)

    # 2. Limpeza de linhas
    df, row_info = limpar_linhas_pos_tratamento(df, threshold=threshold_linhas)
    result_info.update(row_info)

    # 3. Validação de metadados
    metadata_warnings = validar_metadados(df, table_name)
    if metadata_warnings:
        existing_warnings: list = info.get("warnings", [])
        info["warnings"] = existing_warnings + metadata_warnings
    result_info["metadata_warnings"] = metadata_warnings

    return df, result_info
