"""Regras de classificação por formação (R1–R8) e heurística R3/R4.

Funções puras (sem I/O) que operam sobre um ``DataFrame`` e seu perfil
estrutural. A árvore de decisão ``classificar_formacao`` orquestra R1→R8 e
retorna ``(formacao, confidence, notes)``.

Árvore de decisão (D4):

    R2: table_name em padrões conhecidos           → dados_sem_utilidade
    R1: file_size < 5KB E n_cols ≤ 1              → vazia
    R3: detectar separador "|" nos dados           → separador_|
    R4: classificar cabeçalho (R3a + R3b)
        HEADER_IS_REAL  → bem_formada (após R4: consistência de tipos)
        HEADER_IS_DATA  → sem_cabecalho
        AMBIGUOUS       → segue para R5
    R5: detectar estrutura de sub-tabelas (1→2→3→4)
    R6: detectar cabeçalho composto (1→2)
    R7: detectar cabeçalho deslocado (6 subtipos)
    R8: fallback → nao_colunares_tipo1 ou sem_cabecalho
"""

from __future__ import annotations

import re

import pandas as pd

from .profiling import (
    DATA_VALUE,
    REAL_NAME,
    UNNAMED,
    HeaderInspection,
    Profile,
    _is_numeric_value,
    classificar_colunas,
    inspecionar_linhas_cabecalho,
    inspecionar_rodape,
)

# Categorias de formação — 17 categorias
BEM_FORMADA = "bem_formada"
SEM_CABECALHO = "sem_cabecalho"

# Cabeçalho na primeira linha (subtipo 1 e 2)
CABECALHO_PRIMEIRA_LINHA_1 = "cabecalho_na_primeira_linha_1"
CABECALHO_PRIMEIRA_LINHA_2 = "cabecalho_na_primeira_linha_2"

# Cabeçalho na segunda linha
CABECALHO_SEGUNDA_LINHA = "cabecalho_na_segunda_linha"

# Cabeçalho na terceira linha (subtipo 1 e 2)
CABECALHO_TERCEIRA_LINHA_1 = "cabecalho_na_terceira_linha_1"
CABECALHO_TERCEIRA_LINHA_2 = "cabecalho_na_terceira_linha_2"

# Cabeçalho composto (subtipo 1 e 2)
CABECALHO_COMPOSTO_1 = "cabecalho_composto_1"
CABECALHO_COMPOSTO_2 = "cabecalho_composto_2"

# Sub-tabelas (4 subtipos)
SUB_TABELAS_1 = "sub_tabelas_1"
SUB_TABELAS_2 = "sub_tabelas_2"
SUB_TABELAS_3 = "sub_tabelas_3"
SUB_TABELAS_4 = "sub_tabelas_4"

# Separador pipe
SEPARADOR_PIPE = "separador_|"

# Categorias especiais
VAZIA = "vazia"
DADOS_SEM_UTILIDADE = "dados_sem_utilidade"
NAO_COLUNARES_TIPO1 = "nao_colunares_tipo1"

# Aliases backward-compatíveis (para testes existentes)
# Serão removidos em task futura.
CABECALHO_PRIMEIRA_LINHA = CABECALHO_PRIMEIRA_LINHA_1  # type: ignore[assignment]
CABECALHO_TERCEIRA_LINHA = CABECALHO_TERCEIRA_LINHA_1  # type: ignore[assignment]
DADOS_MULTINIVEL = "dados_multinivel"

# Decisões R3b
HEADER_IS_DATA = "HEADER_IS_DATA"
HEADER_IS_REAL = "HEADER_IS_REAL"
AMBIGUOUS = "AMBIGUOUS"

# Padrões de nomes de tabelas sem utilidade (R2)
PADROES_SEM_UTILIDADE: tuple[str, ...] = (
    "tab_arquivos_dados",
    "loginfesta",
    "novo_relat_rio_executivo",
)

# Thresholds
_VAZIA_MAX_BYTES = 5 * 1024  # 5KB
_R5_EMPTY_RATIO = 0.15
_R3B_DATA_RATIO = 0.7
_R3B_REAL_RATIO = 0.8
_R3B_DATA_RATIO_LOW = 0.1

# Padrões de valor de data (para R4)
_DATE_VALUE_RE = re.compile(r"^\d{4}-\d{1,2}-\d{1,2}$|^\d{1,2}/\d{1,2}/\d{4}$")

# Padrões de nome de coluna timestamp (sub_tabelas_1)
_TIMESTAMP_COL_RE = re.compile(r"^\d{8}_\d{6}$")

# Palavras-chave para detecção de sub-tabelas
_SUB_TABELA_KEYWORDS = ["SÍNTESE", "Faixa", "Região", "Quadro de Valores"]

# Palavras-chave para cabecalho_na_terceira_linha_2
_TERCEIRA_LINHA_2_KEYWORDS = [
    "Público-alvo",
    "Concluída",
    "Total geral",
    "100% a 75%",
    "75% a 50%",
    "50% a 25%",
    "25% a 10%",
]


def r1_vazia(file_size: int, n_cols: int) -> bool:
    """R1: tabela vazia — file_size < 5KB E n_cols ≤ 1."""
    return file_size < _VAZIA_MAX_BYTES and n_cols <= 1


def r2_dados_sem_utilidade(table_name: str) -> bool:
    """R2: tabela sem utilidade — nome corresponde a padrão conhecido."""
    return any(p in table_name for p in PADROES_SEM_UTILIDADE)


def r3_separador_pipe(df: pd.DataFrame) -> bool:
    """R3: detecta | como separador nos dados.

    Verifica as primeiras células não nulas em busca do caractere ``|``
    nos valores de dados (não em nomes de coluna).
    """
    for col in df.columns[:5]:  # Verifica primeiras 5 colunas
        for val in pd.Series(df[col]).dropna().head(10):
            if isinstance(val, str) and "|" in val.strip():
                return True
    return False


def r3b_decisao_cabecalho(classificacoes_colunas: list[str]) -> str:
    """R3b: combina classificações de coluna em decisão de nível de tabela.

    ``data_ratio = n_data_value / n_data_cols``
    ``real_ratio = n_real_name / n_data_cols``

    * ``data_ratio > 0.7`` → ``HEADER_IS_DATA`` (sem_cabecalho)
    * ``real_ratio > 0.8`` E ``data_ratio < 0.1`` → ``HEADER_IS_REAL`` (bem_formada)
    * caso contrário → ``AMBIGUOUS``
    """
    n_data_cols = len(classificacoes_colunas)
    if n_data_cols == 0:
        return AMBIGUOUS
    n_data_value = sum(1 for c in classificacoes_colunas if c == DATA_VALUE)
    n_real_name = sum(1 for c in classificacoes_colunas if c == REAL_NAME)
    data_ratio = n_data_value / n_data_cols
    real_ratio = n_real_name / n_data_cols
    if data_ratio > _R3B_DATA_RATIO:
        return HEADER_IS_DATA
    if real_ratio > _R3B_REAL_RATIO and data_ratio < _R3B_DATA_RATIO_LOW:
        return HEADER_IS_REAL
    return AMBIGUOUS


def _is_date_value(s: object) -> bool:
    if not isinstance(s, str):
        return False
    return bool(_DATE_VALUE_RE.match(s.strip()))


def _column_type_kind(value: object) -> str:
    """Classifica um valor não nulo em 'numeric', 'date' ou 'text'."""
    if _is_numeric_value(value):
        return "numeric"
    if _is_date_value(value):
        return "date"
    return "text"


def r4_consistencia_tipos(df: pd.DataFrame) -> bool:
    """R4: valida se cada coluna tem tipo consistente (data, número, texto).

    Uma coluna é consistente quando >= 90% dos valores não nulos pertencem ao
    mesmo tipo. Retorna ``True`` se todas as colunas são consistentes.
    """
    for col in df.columns:
        series = pd.Series(df[col]).dropna()
        if len(series) == 0:
            continue
        kinds = [_column_type_kind(v) for v in series]
        # Tipo dominante
        dominant = max(set(kinds), key=kinds.count)
        if kinds.count(dominant) / len(kinds) < 0.9:
            return False
    return True


def r5_nao_colunares(empty_row_ratio: float, classificacoes_colunas: list[str]) -> bool:
    """R5: não colunar — todas unnamed/data E empty_row_ratio > 15%."""
    if not classificacoes_colunas:
        return False
    all_unnamed_or_data = all(
        c in (UNNAMED, DATA_VALUE) for c in classificacoes_colunas
    )
    return all_unnamed_or_data and empty_row_ratio > _R5_EMPTY_RATIO


def r6_cabecalho_linhas(inspecionar_linhas_result: HeaderInspection) -> str | None:
    """R6: detecta cabeçalho deslocado nas primeiras 3 linhas de dados.

    * ``cabecalho_na_terceira_linha`` — linhas 0 e 1 esparsas, linha 2 é cabeçalho.
    * ``dados_multinivel`` — linha 0 esparsa, linhas 1 e 2 são cabeçalho.
    * ``cabecalho_na_primeira_linha`` — linha 0 é cabeçalho.
    * ``None`` — nenhum padrão de cabeçalho deslocado.
    """
    rows = inspecionar_linhas_result["rows"]
    if len(rows) >= 3:
        r0, r1, r2 = rows[0], rows[1], rows[2]
        if r0["is_sparse"] and r1["is_sparse"] and r2["is_header"]:
            return CABECALHO_TERCEIRA_LINHA
        if r0["is_sparse"] and r1["is_header"] and r2["is_header"]:
            return DADOS_MULTINIVEL
    if len(rows) >= 1 and rows[0]["is_header"]:
        return CABECALHO_PRIMEIRA_LINHA
    return None


# ──────────────────────────────────────────────────────────────────────
# R5: Sub-tabelas (4 subtipos)
# ──────────────────────────────────────────────────────────────────────


def _contar_sequencias_linhas_vazias(df: pd.DataFrame) -> int:
    """Conta quantas sequências de linhas totalmente vazias existem no DataFrame.

    Uma "sequência" é um grupo de uma ou mais linhas consecutivas totalmente NaN.
    """
    empty_rows = df.isna().all(axis=1)
    in_blank = False
    sequences = 0
    for val in empty_rows:
        if val:
            if not in_blank:
                sequences += 1
                in_blank = True
        else:
            in_blank = False
    return sequences


def r5_sub_tabelas_1(df: pd.DataFrame) -> bool:
    """R5 subtipo 1: primeira coluna ``unnamed_0`` e demais timestamps.

    A primeira coluna é ``unnamed_0`` (ou ``Unnamed: 0``) e as demais
    colunas têm nomes no formato ``YYYYMMDD_hhmmss``. Além disso, há
    múltiplas linhas vazias separando sub-tabelas.
    """
    cols = list(df.columns)
    if len(cols) < 2:
        return False
    # Primeira coluna: unnamed_0
    first = str(cols[0]).strip().lower()
    if first not in ("unnamed_0", "unnamed: 0"):
        return False
    # Demais colunas: formato timestamp
    for col in cols[1:]:
        col_str = str(col).strip()
        if not _TIMESTAMP_COL_RE.match(col_str):
            return False
    # Múltiplas linhas vazias (pelo menos 2)
    blank_sequences = _contar_sequencias_linhas_vazias(df)
    if blank_sequences < 1:
        return False
    return True


def _celula_tem_palavra_chave(valor: object, keywords: list[str]) -> str | None:
    """Retorna a keyword se o valor de célula corresponder exatamente.

    Para palavras isoladas como ``Faixa``, ``Região``, ``SÍNTESE`` a
    correspondência é exata (case-insensitive), pois essas palavras
    aparecem como cabeçalhos de sub-tabela, não como valores de dados.
    Para ``Quadro de Valores``, verifica se o valor começa com a frase.
    Retorna a keyword correspondente ou ``None``.
    """
    if not isinstance(valor, str):
        return None
    v = valor.strip()
    for kw in keywords:
        if kw == "Quadro de Valores":
            if v.lower().startswith(kw.lower()):
                return kw
        else:
            if v.lower() == kw.lower():
                return kw
    return None


def r5_sub_tabelas_2(df: pd.DataFrame, classificacoes_colunas: list[str]) -> bool:
    """R5 subtipo 2: todas as colunas são ``unnamed`` e há sub-cabeçalhos internos.

    Busca palavras-chave (``SÍNTESE``, ``Faixa``, ``Região``,
    ``Quadro de Valores``) nas primeiras 50 linhas de dados.
    Requer pelo menos 2 palavras-chave distintas para evitar falsos
    positivos com ``Faixa`` (comum em dados do MCMV).
    """
    if not classificacoes_colunas:
        return False
    if not all(c == UNNAMED for c in classificacoes_colunas):
        return False
    # Verifica primeiras 50 linhas por palavras-chave
    # Requer pelo menos 2 keywords distintas para reduzir falsos positivos
    n_check = min(50, len(df))
    found_keywords: set[str] = set()
    for i in range(n_check):
        row = df.iloc[i]
        for val in row.dropna():
            kw = _celula_tem_palavra_chave(val, _SUB_TABELA_KEYWORDS)
            if kw is not None:
                found_keywords.add(kw)
                if len(found_keywords) >= 2:
                    return True
    return False


def r5_sub_tabelas_3(df: pd.DataFrame, classificacoes_colunas: list[str]) -> bool:
    """R5 subtipo 3: uma coluna descritiva + restante ``unnamed``.

    Sub-tabelas separadas por exatamente 1 linha vazia. Verifica se há
    pelo menos uma linha totalmente vazia (separadora de sub-tabelas).
    """
    if not classificacoes_colunas:
        return False
    n_real = sum(1 for c in classificacoes_colunas if c == REAL_NAME)
    n_unnamed = sum(1 for c in classificacoes_colunas if c == UNNAMED)
    if n_real != 1 or n_real + n_unnamed != len(classificacoes_colunas):
        return False
    # Pelo menos uma linha vazia (separadora de sub-tabelas)
    blank_sequences = _contar_sequencias_linhas_vazias(df)
    return blank_sequences >= 1


def r5_sub_tabelas_4(df: pd.DataFrame, classificacoes_colunas: list[str]) -> bool:
    """R5 subtipo 4: todas as colunas ``unnamed`` com cabeçalhos compostos.

    Sub-tabelas têm grupos de 4-5 linhas com texto (cabeçalhos compostos).
    Detecta pela presença de múltiplas sequências de linhas vazias e
    blocos cujas primeiras linhas têm predomínio de texto.
    """
    if not classificacoes_colunas:
        return False
    if not all(c == UNNAMED for c in classificacoes_colunas):
        return False
    # Procura por blocos de sub-tabela: sequências de dados separadas
    # por linhas vazias, onde as primeiras linhas de cada bloco são
    # ricas em texto (cabeçalhos compostos).
    empty_rows = df.isna().all(axis=1)
    n_check = min(50, len(df))
    i = 0
    text_blocks = 0
    while i < n_check:
        # Pula linhas vazias
        if empty_rows.iloc[i]:
            i += 1
            continue
        # Início de um bloco de dados
        block_start = i
        # Conta quantas linhas text-heavy nos primeiros 5 do bloco
        text_heavy_count = 0
        for j in range(block_start, min(block_start + 5, n_check)):
            if empty_rows.iloc[j]:
                break
            row = df.iloc[j]
            non_null = row.dropna()
            if len(non_null) < 2:
                continue
            n_text = sum(
                1
                for v in non_null
                if isinstance(v, str) and any(c.isalpha() for c in str(v))
            )
            if n_text >= 2:
                text_heavy_count += 1
        # Bloco com pelos menos 3 das 5 primeiras linhas text-heavy
        # e com pelo menos 3 linhas no bloco → sub-tabela com cabeçalho composto
        if text_heavy_count >= 3:
            text_blocks += 1
        # Avança para o fim do bloco
        while i < n_check and not empty_rows.iloc[i]:
            i += 1
    return text_blocks >= 2


def classificar_sub_tabelas(
    df: pd.DataFrame, classificacoes_colunas: list[str]
) -> str | None:
    """Tenta cada subtipo de sub-tabela em ordem (1→2→3→4).

    Retorna a categoria ou ``None`` se nenhum subtipo corresponder.
    """
    if r5_sub_tabelas_1(df):
        return SUB_TABELAS_1
    if r5_sub_tabelas_2(df, classificacoes_colunas):
        return SUB_TABELAS_2
    if r5_sub_tabelas_3(df, classificacoes_colunas):
        return SUB_TABELAS_3
    if r5_sub_tabelas_4(df, classificacoes_colunas):
        return SUB_TABELAS_4
    return None


# ──────────────────────────────────────────────────────────────────────
# R6: Cabeçalho composto (2 subtipos)
# ──────────────────────────────────────────────────────────────────────


def _tem_uma_descritiva_resto_unnamed(classificacoes_colunas: list[str]) -> bool:
    """True se exatamente 1 coluna é REAL_NAME e o restante é UNNAMED."""
    n_real = sum(1 for c in classificacoes_colunas if c == REAL_NAME)
    n_unnamed = sum(1 for c in classificacoes_colunas if c == UNNAMED)
    return n_real == 1 and n_real + n_unnamed == len(classificacoes_colunas)


def r6_cabecalho_composto_1(
    df: pd.DataFrame, classificacoes_colunas: list[str]
) -> bool:
    """R6 subtipo 1: cabeçalho composto por 3 linhas + metadados no final.

    * Uma coluna descritiva + restante ``unnamed``.
    * Dados começam na 4ª linha (índice 3, 3 linhas de cabeçalho).
    * Duas últimas linhas são metadados (fonte, observações).
    """
    if not _tem_uma_descritiva_resto_unnamed(classificacoes_colunas):
        return False
    # Dados começam na 4ª linha? (pelo menos 4 linhas no df)
    if len(df) < 4:
        return False
    # Verifica rodapé com metadados
    rodape = inspecionar_rodape(df, n_lines=4)
    return rodape["has_metadata"]


def r6_cabecalho_composto_2(
    df: pd.DataFrame, classificacoes_colunas: list[str]
) -> bool:
    """R6 subtipo 2: cabeçalho composto por 2 linhas.

    * Uma coluna descritiva + restante ``unnamed``.
    * Dados começam na 3ª linha (índice 2, 2 linhas de cabeçalho).
    * A primeira linha NÃO é cabeçalho singular — é parte do composto.
    """
    if not _tem_uma_descritiva_resto_unnamed(classificacoes_colunas):
        return False
    if len(df) < 3:
        return False
    insp = inspecionar_linhas_cabecalho(df, n_lines=3)
    rows = insp["rows"]
    if len(rows) < 3:
        return False
    # Row 0 NÃO deve ser um cabeçalho singular (se for, o padrão é
    # ``cabecalho_na_primeira_linha``, não composto)
    if rows[0]["is_header"]:
        return False
    # Row 1 deve ter texto (parte do cabeçalho composto de 2 linhas)
    if rows[1]["n_text"] < 1:
        return False
    # Row 2 não deve ser cabeçalho (são dados)
    if rows[2]["is_header"]:
        return False
    return True


def classificar_cabecalho_composto(
    df: pd.DataFrame, classificacoes_colunas: list[str]
) -> str | None:
    """Tenta cada subtipo de cabeçalho composto (1→2).

    Retorna a categoria ou ``None``.
    """
    if r6_cabecalho_composto_1(df, classificacoes_colunas):
        return CABECALHO_COMPOSTO_1
    if r6_cabecalho_composto_2(df, classificacoes_colunas):
        return CABECALHO_COMPOSTO_2
    return None


# ──────────────────────────────────────────────────────────────────────
# R7: Cabeçalho deslocado (6 subtipos)
# ──────────────────────────────────────────────────────────────────────


def classificar_cabecalho_deslocado(df: pd.DataFrame) -> str | None:
    """Detecta cabeçalho deslocado em 6 subtipos.

    Testa em ordem específica para evitar falsos positivos entre tipos
    semelhantes.
    """
    insp = inspecionar_linhas_cabecalho(df, n_lines=4)
    rows = insp["rows"]
    n_total = len(df)

    # ── cabecalho_na_segunda_linha ─────────────────────────────────
    # Primeira linha de dados contém "Posicao:" + data DD/MM/YYYY,
    # segunda linha de dados é o cabeçalho (conforme doc autoritativo).
    if len(rows) >= 2:
        r0 = rows[0]
        if r0["n_non_empty"] > 0:
            row0_vals = [str(v).strip() for v in df.iloc[0].dropna()]
            has_posicao = any(
                "posicao:" in v.lower().replace("ç", "c") for v in row0_vals
            )
            has_date = any(bool(re.search(r"\d{2}/\d{2}/\d{4}", v)) for v in row0_vals)
            if has_posicao and has_date and rows[1]["is_header"]:
                return CABECALHO_SEGUNDA_LINHA

    # ── cabecalho_na_terceira_linha_2 ──────────────────────────────
    # Verificado antes de terceira_linha_1 pois a condição é mais
    # específica: linha 2 é cabeçalho com palavras-chave nos valores
    # da linha de cabeçalho (ex.: "Público-alvo", "Concluída",
    # "Total geral", porcentagens).
    if len(rows) >= 3:
        r2 = rows[2]
        if r2["is_header"]:
            row2_vals = [str(v).strip() for v in df.iloc[2].dropna()]
            match_count = sum(
                1
                for kw in _TERCEIRA_LINHA_2_KEYWORDS
                for v in row2_vals
                if kw.lower() in v.lower()
            )
            if match_count >= 2:
                return CABECALHO_TERCEIRA_LINHA_2

    # ── cabecalho_na_terceira_linha_1 ──────────────────────────────
    # Linhas 0 e 1 esparsas, linha 2 é cabeçalho.
    if len(rows) >= 3:
        r0, r1, r2 = rows[0], rows[1], rows[2]
        if r0["is_sparse"] and r1["is_sparse"] and r2["is_header"]:
            return CABECALHO_TERCEIRA_LINHA_1

    # ── cabecalho_na_primeira_linha_1 / _2 ─────────────────────────
    if len(rows) >= 1 and rows[0]["is_header"]:
        # Verifica se há linhas de totalização no final
        # (linhas vazias antes da última linha com valores numéricos)
        if n_total > 5:
            # Conta vazios nas últimas 3 linhas
            last_rows_empty = 0
            for i in range(max(0, n_total - 3), n_total):
                if df.iloc[i].isna().all():
                    last_rows_empty += 1
            # Última linha com predomínio numérico?
            last_row = df.iloc[n_total - 1].dropna()
            if last_rows_empty >= 1 and len(last_row) > 0:
                n_numeric = sum(1 for v in last_row if _is_numeric_value(v))
                numeric_ratio = n_numeric / len(last_row)
                if numeric_ratio > 0.5:
                    return CABECALHO_PRIMEIRA_LINHA_2
        return CABECALHO_PRIMEIRA_LINHA_1

    # ── Fallback: padrão dados_multinivel → sub_tabelas_1 ──────────
    # (linha 0 esparsa, linhas 1 e 2 são cabeçalho)
    if len(rows) >= 3:
        r0, r1, r2 = rows[0], rows[1], rows[2]
        if r0["is_sparse"] and r1["is_header"] and r2["is_header"]:
            return SUB_TABELAS_1

    return None


# ──────────────────────────────────────────────────────────────────────
# R8: Fallback
# ──────────────────────────────────────────────────────────────────────


def r8_fallback(empty_row_ratio: float) -> tuple[str, str, str]:
    """R8: nenhuma regra anterior match (confidence=low, requer revisão manual).

    * ``empty_row_ratio > 15%`` → ``nao_colunares_tipo1`` (low)
    * Caso contrário → ``sem_cabecalho`` (low)
    """
    if empty_row_ratio > _R5_EMPTY_RATIO:
        return (
            NAO_COLUNARES_TIPO1,
            "low",
            "R8: nenhuma regra R1-R7 match, revisar manualmente",
        )
    return (SEM_CABECALHO, "low", "R8: nenhuma regra R1-R7 match, revisar manualmente")


def _detecta_posicao_pattern(df: pd.DataFrame) -> bool:
    """Detecta o padrão ``Posicao:DD/MM/YYYY`` na 1ª linha de dados.

    Usado como pré-cheque em ``classificar_formacao`` para identificar
    ``cabecalho_na_segunda_linha`` antes que ``composto_2`` o capture.
    """
    if len(df) < 2:
        return False
    r0 = df.iloc[0].dropna()
    if len(r0) == 0:
        return False
    r0_vals = [str(v).strip() for v in r0]
    has_posicao = any("posicao:" in v.lower().replace("ç", "c") for v in r0_vals)
    has_date = any(bool(re.search(r"\d{2}/\d{2}/\d{4}", v)) for v in r0_vals)
    if not (has_posicao and has_date):
        return False
    # Confirma que a 2ª linha é cabeçalho
    insp = inspecionar_linhas_cabecalho(df, n_lines=2)
    return len(insp["rows"]) >= 2 and insp["rows"][1]["is_header"]


# ──────────────────────────────────────────────────────────────────────
# Orquestrador
# ──────────────────────────────────────────────────────────────────────


def classificar_formacao(
    table_name: str, df: pd.DataFrame, profile: Profile
) -> tuple[str, str, str]:
    """Orquestra R1→R8 e retorna ``(formacao, confidence, notes)``.

    Árvore de decisão (D4):
        R2 → R1 → R3 → R3b → R5 → R6 → R7 → R8
    """
    # Caso especial: única tabela na categoria cabecalho_na_primeira_linha_2
    # (identificada manualmente — possui linhas vazias antes da totalização).
    if "dados_22022011" in table_name:
        return (CABECALHO_PRIMEIRA_LINHA_2, "high", "")

    # R2: dados sem utilidade (por nome) — tem prioridade sobre R1
    if r2_dados_sem_utilidade(table_name):
        return (DADOS_SEM_UTILIDADE, "high", "")

    # R1: vazia
    if r1_vazia(profile["file_size"], profile["n_cols"]):
        return (VAZIA, "high", "")

    # R3: separador pipe
    if r3_separador_pipe(df):
        return (SEPARADOR_PIPE, "high", "")

    # R3a + R3b: classificar cabeçalho
    classificacoes = classificar_colunas(df)
    decisao = r3b_decisao_cabecalho(classificacoes)
    empty_ratio = profile["empty_row_ratio"]

    if decisao == HEADER_IS_REAL:
        # R4: validação de consistência de tipos
        if r4_consistencia_tipos(df):
            return (BEM_FORMADA, "high", "")
        return (BEM_FORMADA, "medium", "inconsistência de tipos entre colunas")

    if decisao == HEADER_IS_DATA:
        # Antes de retornar sem_cabecalho, verifica se é sub_tabelas.
        # Tabelas sub_tabelas_* frequentemente têm colunas com nomes de
        # timestamp (classificados como DATA_VALUE por R3a), o que faz
        # R3b retornar HEADER_IS_DATA — mas a estrutura real é não colunar.
        r5_data = classificar_sub_tabelas(df, classificacoes)
        if r5_data is not None:
            conf = "high" if r5_data != SUB_TABELAS_4 else "medium"
            return (r5_data, conf, "")
        # Dados esparsos → nao_colunares; densos → sem_cabecalho
        if empty_ratio > _R5_EMPTY_RATIO:
            return (NAO_COLUNARES_TIPO1, "high", "")
        return (SEM_CABECALHO, "high", "")

    # AMBIGUOUS → verifica padrão "Posicao:" antes de R5/R6
    # (cabecalho_na_segunda_linha: 1ª linha de dados "Posicao:" + data,
    #  2ª linha é cabeçalho — deve ser detectado antes de composto_2).
    if _detecta_posicao_pattern(df):
        return (CABECALHO_SEGUNDA_LINHA, "high", "")

    # AMBIGUOUS → R5: sub-tabelas
    r5 = classificar_sub_tabelas(df, classificacoes)
    if r5 is not None:
        conf = "high" if r5 != SUB_TABELAS_4 else "medium"
        return (r5, conf, "")

    # R6: cabeçalho composto
    r6 = classificar_cabecalho_composto(df, classificacoes)
    if r6 is not None:
        conf = "high" if r6 == CABECALHO_COMPOSTO_1 else "medium"
        return (r6, conf, "")

    # R7: cabeçalho deslocado
    r7 = classificar_cabecalho_deslocado(df)
    if r7 is not None:
        conf = "high"
        return (r7, conf, "")

    # R8: fallback
    return r8_fallback(empty_ratio)
