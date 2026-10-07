"""Inferencia de nomes de colunas para tabelas ``sem_cabecalho``.

Analisa os valores de cada coluna e infere nomes descritivos baseados
em padroes de dominio MCMV (CNPJ, datas, valores monetarios, frentes,
situacao de obra, etc.).

Arquitetura em duas passadas:
1. Classificacao independente por coluna — detectores em ordem de
   prioridade decrescente.
2. Desambiguacao multi-coluna — resolve conflitos de nomes duplicados.

Uso tipico::

    from classificacao.inferencia_colunas import inferir_nomes_colunas
    nomes, confidence_note = inferir_nomes_colunas(df)

"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

# ──────────────────────────────────────────────────────────────────────
# Utility functions
# ──────────────────────────────────────────────────────────────────────


def _ratio_match(series: pd.Series, predicate: Any) -> float:
    """Proporcao de valores nao-nulos que satisfazem o predicado.

    Args:
        series: Coluna do DataFrame.
        predicate: Callable ``(value) -> bool`` ou string de metodo
            (ex: ``'isdigit'``, ``'isalpha'``).

    Returns:
        Float entre 0.0 e 1.0. Retorna 0.0 se nao houver valores
        nao-nulos.
    """
    valid = series.dropna()
    if len(valid) == 0:
        return 0.0
    if isinstance(predicate, str):
        return sum(bool(getattr(str(v).strip(), predicate)()) for v in valid) / len(
            valid
        )
    if callable(predicate):
        return sum(bool(predicate(v)) for v in valid) / len(valid)
    return 0.0


def _mediana_numerica(series: pd.Series) -> float:
    """Mediana de coluna convertida para numerico, ignorando erros.

    Args:
        series: Coluna do DataFrame.

    Returns:
        Mediana como float. Retorna 0.0 se a conversao falhar
        completamente.
    """
    numeric = pd.to_numeric(
        series.astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    valid = numeric.dropna()
    if len(valid) == 0:
        return 0.0
    return float(valid.median())


def _normalizar_nome(nome: str) -> str:
    """Normaliza nome para lowercase, sem acentos, underscores.

    Args:
        nome: Nome bruto.

    Returns:
        Nome normalizado (ex: ``'situacao_obra'``).
    """
    import unicodedata

    nome = unicodedata.normalize("NFKD", nome).encode("ASCII", "ignore").decode("ASCII")
    nome = nome.strip().lower()
    nome = re.sub(r"[^a-z0-9]+", "_", nome)
    nome = nome.strip("_")
    return nome if nome else "col"


def _count_unique(series: pd.Series) -> int:
    """Conta valores unicos nao-nulos em uma coluna."""
    return int(series.dropna().nunique())


# ──────────────────────────────────────────────────────────────────────
# Pass 1: Column detectors (priority order = definition order)
# ──────────────────────────────────────────────────────────────────────


def detectar_situacao_obra(series: pd.Series) -> str | None:
    """Detecta coluna de situacao da obra.

    Threshold: 70% dos valores nao-nulos pertencem ao conjunto conhecido.
    """
    STATUS = {
        "concluida",
        "concluido",
        "atrasada",
        "atrasado",
        "paralisada",
        "paralisado",
        "normal",
        "adiantada",
        "adiantado",
        "sem_medicao",
        "sem_medicoes",
        "obra_fisica_concluida",
        "obra_fisica_concluido",
    }

    def _check(v: Any) -> bool:
        s = _normalizar_nome(str(v))
        return s in STATUS

    if _ratio_match(series, _check) >= 0.70:
        return "situacao_obra"
    return None


def detectar_frente(series: pd.Series) -> str | None:
    """Detecta coluna de frente do programa MCMV.

    Threshold: 70% de match com o conjunto conhecido.
    """
    FRENTES = {"far", "fgts", "rural", "entidades", "pnhr", "pnhb"}

    def _check(v: Any) -> bool:
        return str(v).strip().lower() in FRENTES

    if _ratio_match(series, _check) >= 0.70:
        return "frente"
    return None


def detectar_data_referencia(series: pd.Series) -> str | None:
    """Detecta coluna de data de referencia (data unica repetida).

    Threshold: 70% de datas validas E no maximo 2 valores unicos.
    """
    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$|^\d{8}$")

    def _is_date(v: Any) -> bool:
        return bool(date_re.match(str(v).strip()))

    n_unique = _count_unique(series)

    if _ratio_match(series, _is_date) >= 0.70 and n_unique <= 2:
        return "data_referencia"
    return None


def detectar_cnpj(series: pd.Series) -> str | None:
    """Detecta coluna de CNPJ (13-14 digitos ou formato XX.XXX.XXX/XXXX-XX).

    Threshold: 70%.
    """
    cnpj_formatted_re = re.compile(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$")

    def _check(v: Any) -> bool:
        s = str(v).strip()
        if cnpj_formatted_re.match(s):
            return True
        if s.isdigit() and len(s) in (13, 14):
            return True
        return False

    if _ratio_match(series, _check) >= 0.70:
        return "cnpj"
    return None


def detectar_percentual(series: pd.Series) -> str | None:
    """Detecta coluna de percentual (0-100, virgula decimal, ate 2 casas).

    Threshold: 70%. Exige que ao menos 5% dos valores sejam > 0 e
    que nenhum valor convertido exceda 100.
    """
    pct_re = re.compile(r"^\d{1,3}(,\d{1,2})?$")

    vals_parsed: list[float] = []
    for v in series.dropna():
        s = str(v).strip()
        if pct_re.match(s):
            try:
                val = float(s.replace(",", "."))
                if 0.0 <= val <= 100.0:
                    vals_parsed.append(val)
                    continue
            except ValueError:
                pass
        vals_parsed.append(float("nan"))

    valid = len(vals_parsed)
    if valid == 0:
        return None

    match_count = sum(1 for v in vals_parsed if not (v != v))  # not NaN
    ratio = match_count / valid

    if ratio < 0.70:
        return None

    # Exige que pelo menos 5% dos valores sejam > 0
    non_zero = sum(1 for v in vals_parsed if v > 0)
    if non_zero < valid * 0.05:
        return None

    # Nenhum valor > 100 (ja filtrado pelo regex, mas segurança)
    if any(v > 100 for v in vals_parsed if not (v != v)):
        return None

    return "valor_percentual_conclusao"


def detectar_faixa_mcmv(series: pd.Series) -> str | None:
    """Detecta coluna de faixa do MCMV (inteiros 1-4, ate 4 valores unicos).

    Threshold: 70%.
    """

    def _check(v: Any) -> bool:
        s = str(v).strip()
        return s in {"1", "2", "3", "4"}

    n_unique = _count_unique(series)

    if _ratio_match(series, _check) >= 0.70 and n_unique <= 4:
        return "faixa_mcmv"
    return None


def detectar_data(series: pd.Series) -> str | None:
    """Detecta coluna de data generica (YYYY-MM-DD ou YYYYMMDD).

    Threshold: 70%.
    """
    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$|^\d{8}$")

    def _check(v: Any) -> bool:
        return bool(date_re.match(str(v).strip()))

    if _ratio_match(series, _check) >= 0.70:
        return "data"
    return None


def detectar_nome_empreendimento(series: pd.Series) -> str | None:
    """Detecta coluna de nome de empreendimento (texto com marcadores residenciais).

    Threshold: 50%.
    """
    MARCADORES = [
        "residencial",
        "conj",
        "condominio",
        "cond.",
        "loteamento",
        "parque",
        "jardim",
        "viver melhor",
        "orgulho",
        "morar",
        "cidade",
        "bem viver",
        "recanto",
        "portal",
    ]

    def _check(v: Any) -> bool:
        s = str(v).strip().lower()
        # Precisa ter pelo menos 3 palavras (ou 2 se contem marcador)
        palavras = s.split()
        if len(palavras) < 2:
            return False
        return any(m in s for m in MARCADORES)

    if _ratio_match(series, _check) >= 0.50:
        return "nome_empreendimento"
    return None


def detectar_endereco(series: pd.Series) -> str | None:
    """Detecta coluna de endereco (marcadores de logradouro).

    Threshold: 50%.
    """
    MARCADORES = [
        "rua",
        "avenida",
        "av.",
        "estrada",
        "rodovia",
        " br ",
        "s/n",
        "lote",
        "gleba",
        "fazenda",
        " km ",
        "quadra",
        "ramal",
        "travessa",
        "anel viario",
        "terreno",
        "margem",
        "rod.",
    ]

    def _check(v: Any) -> bool:
        s = str(v).strip().lower()
        return any(m in s for m in MARCADORES)

    if _ratio_match(series, _check) >= 0.50:
        return "endereco"
    return None


def detectar_nome_empresa(series: pd.Series) -> str | None:
    """Detecta coluna de nome de empresa (sufixos juridicos).

    Threshold: 50% (ou 30% se houver CNPJ na mesma linha — indicativo
    forte de contexto empresarial).

    So ativa se a coluna NAO for classificada como empreendimento
    (tratado pelo orquestrador pela ordem de prioridade).
    """
    SUFIXOS = [
        "ltda",
        "s.a.",
        "s/a",
        "eireli",
        "epp",
        " - me",
        "em recuperacao",
        "em recuperação",
        "engenharia",
        "construtora",
        "incorpora",
        "empreendimentos",
    ]

    def _check(v: Any) -> bool:
        s = str(v).strip().lower()
        return any(suf in s for suf in SUFIXOS)

    if _ratio_match(series, _check) >= 0.50:
        return "nome_empresa"
    return None


def detectar_valor_monetario(series: pd.Series) -> str | None:
    """Detecta coluna de valor monetario.

    Dois sub-padroes:
    1. Virgula decimal com 2 digitos (threshold 50%).
    2. Inteiro >= 10000 (threshold 70%).

    Nao ativa se o padrao for percentual (tratado por prioridade).
    """
    # Padrao com virgula decimal: XX.XXX,XX ou XXXXX,XX etc.
    comma_re = re.compile(r"^\d{1,3}(\.\d{3})*,\d{2}$")
    comma_simple_re = re.compile(r"^\d+,\d{2}$")

    def _is_comma(v: Any) -> bool:
        s = str(v).strip()
        return bool(comma_re.match(s) or comma_simple_re.match(s))

    ratio_comma = _ratio_match(series, _is_comma)
    if ratio_comma >= 0.50:
        return "valor_monetario"

    def _is_large_int(v: Any) -> bool:
        s = str(v).strip()
        if not (s.lstrip("-").isdigit()):
            return False
        try:
            return abs(int(s)) >= 10000
        except ValueError:
            return False

    ratio_large = _ratio_match(series, _is_large_int)
    if ratio_large >= 0.70:
        # Verifica se nao eh tudo data (YYYYMMDD de 8 digitos >= 10000)
        date_re = re.compile(r"^\d{8}$")
        date_count = sum(1 for v in series.dropna() if date_re.match(str(v).strip()))
        valid_count = len(series.dropna())
        if valid_count > 0 and date_count / valid_count < 0.70:
            return "valor_monetario"

    return None


def detectar_uh(series: pd.Series) -> str | None:
    """Detecta coluna de unidades habitacionais.

    Inteiro nao-negativo <= 50000, threshold 70%.
    Nao ativa se for CNPJ/data/codigo/percentual/faixa
    (tratado por prioridade).
    """

    def _check(v: Any) -> bool:
        s = str(v).strip()
        if not s.isdigit():
            return False
        try:
            val = int(s)
            return 0 <= val <= 50000
        except ValueError:
            return False

    if _ratio_match(series, _check) >= 0.70:
        return "qtd_uh"
    return None


def detectar_codigo(series: pd.Series) -> str | None:
    """Detecta coluna de codigo (5-7 digitos).

    Threshold: 70%. Nao ativa se for data (YYYYMMDD de 8 digitos)
    — tratado por prioridade.
    """

    def _check(v: Any) -> bool:
        s = str(v).strip()
        return s.isdigit() and 5 <= len(s) <= 7

    if _ratio_match(series, _check) >= 0.70:
        return "codigo"
    return None


# ──────────────────────────────────────────────────────────────────────
# Pass 2: Multi-column disambiguation
# ──────────────────────────────────────────────────────────────────────


def _desambiguar_datas(nomes: list[str | None], df: pd.DataFrame) -> list[str | None]:
    """Resolve nomes de colunas de data: inicio, fim, referencia.

    Heuristica:
    - Coluna de data com <= 2 valores unicos -> data_referencia
    - Primeira data restante -> data_inicio
    - Demais -> data_fim
    """
    date_indices = [i for i, n in enumerate(nomes) if n == "data"]
    if not date_indices:
        return nomes

    result = list(nomes)
    # Identifica data_referencia: ultima data com <= 2 valores unicos
    ref_candidates = sorted(date_indices, reverse=True)
    ref_idx = None
    for idx in ref_candidates:
        if _count_unique(df.iloc[:, idx]) <= 2:
            ref_idx = idx
            break
    if ref_idx is not None:
        result[ref_idx] = "data_referencia"
        date_indices.remove(ref_idx)

    for i, idx in enumerate(date_indices):
        if i == 0:
            result[idx] = "data_inicio"
        else:
            result[idx] = "data_fim"

    return result


def _desambiguar_valores(nomes: list[str | None], df: pd.DataFrame) -> list[str | None]:
    """Resolve nomes de colunas de valor monetario por magnitude.

    Heuristica:
    - Maior mediana -> valor_investimento
    - Segunda maior -> valor_repasse
    - Terceira -> valor_contrapartida
    - Excedentes -> valor_monetario_N
    """
    valor_indices = [i for i, n in enumerate(nomes) if n == "valor_monetario"]
    if len(valor_indices) <= 1:
        return nomes

    # Ordena indices por mediana decrescente
    indexed = [(i, _mediana_numerica(df.iloc[:, i])) for i in valor_indices]
    indexed.sort(key=lambda x: x[1], reverse=True)

    LABELS = ["valor_investimento", "valor_repasse", "valor_contrapartida"]

    result = list(nomes)
    for rank, (idx, _) in enumerate(indexed):
        if rank < len(LABELS):
            result[idx] = LABELS[rank]
        else:
            result[idx] = f"valor_monetario_{rank - len(LABELS) + 1}"

    return result


def _desambiguar_uh(nomes: list[str | None], df: pd.DataFrame) -> list[str | None]:
    """Resolve nomes de colunas de UH por magnitude.

    Heuristica:
    - Maior mediana -> qtd_uh_contratadas
    - Menor mediana -> qtd_uh_concluidas
    - Intermediarias -> qtd_uh_entregues
    - Excedentes -> qtd_uh_N
    """
    uh_indices = [i for i, n in enumerate(nomes) if n == "qtd_uh"]
    if len(uh_indices) <= 1:
        return nomes

    indexed = [(i, _mediana_numerica(df.iloc[:, i])) for i in uh_indices]
    indexed.sort(key=lambda x: x[1])

    result = list(nomes)
    if len(indexed) == 2:
        result[indexed[-1][0]] = "qtd_uh_contratadas"
        result[indexed[0][0]] = "qtd_uh_concluidas"
    else:
        # Maior -> contratadas, menor -> concluidas
        result[indexed[-1][0]] = "qtd_uh_contratadas"
        result[indexed[0][0]] = "qtd_uh_concluidas"
        for i in range(1, len(indexed) - 1):
            result[indexed[i][0]] = "qtd_uh_entregues"
        # Se houver mais de 3, renomeia excedentes
        if len(indexed) > 3:
            extra = len(indexed) - 3
            for e in range(extra):
                result[indexed[-(2 + e)][0]] = f"qtd_uh_{e + 1}"

    return result


def _desambiguar_codigos(nomes: list[str | None]) -> list[str | None]:
    """Adiciona sufixo numerico sequencial a colunas de codigo."""
    result = list(nomes)
    counter = 1
    for i, n in enumerate(result):
        if n == "codigo":
            result[i] = f"codigo_{counter}"
            counter += 1
    return result


def _garantir_unicidade(nomes: list[str]) -> list[str]:
    """Garante que todos os nomes sao unicos, adicionando sufixos se necessario."""
    seen: dict[str, int] = {}
    result = []
    for nome in nomes:
        if nome in seen:
            seen[nome] += 1
            result.append(f"{nome}_{seen[nome]}")
        else:
            seen[nome] = 0
            result.append(nome)
    return result


# ──────────────────────────────────────────────────────────────────────
# Main orchestrator
# ──────────────────────────────────────────────────────────────────────

# Ordered by priority (highest first). Each entry is a callable
# (pd.Series) -> str | None.
_DETECTORES: list[tuple[str, Any]] = [
    ("situacao_obra", detectar_situacao_obra),
    ("frente", detectar_frente),
    ("data_referencia", detectar_data_referencia),
    ("cnpj", detectar_cnpj),
    ("faixa_mcmv", detectar_faixa_mcmv),
    ("percentual", detectar_percentual),
    ("data", detectar_data),
    ("nome_empreendimento", detectar_nome_empreendimento),
    ("endereco", detectar_endereco),
    ("nome_empresa", detectar_nome_empresa),
    ("codigo", detectar_codigo),
    ("valor_monetario", detectar_valor_monetario),
    ("qtd_uh", detectar_uh),
]


def inferir_nomes_colunas(df: pd.DataFrame) -> tuple[list[str], str]:
    """Infere nomes semanticos para colunas de uma tabela sem cabecalho.

    Args:
        df: DataFrame sem nomes de coluna (colunas sao 0, 1, 2, ...).

    Returns:
        Tupla ``(lista_de_nomes, confidence_note)``. A confidence note
        indica a qualidade da inferencia:
        - ``'alta_confianca'``: todas as colunas foram inferidas.
        - ``'parcial'``: algumas colunas foram inferidas, outras usaram
          fallback.
        - ``'sem_exito'``: nenhuma coluna foi inferida, todas ``col_N``.
    """
    n_cols = len(df.columns)
    nomes: list[str | None] = [None] * n_cols

    # Pass 1: Classificar cada coluna com o primeiro detector que bater
    for col_idx in range(n_cols):
        series = df.iloc[:, col_idx]
        for _detector_name, detector_fn in _DETECTORES:
            resultado = detector_fn(series)
            if resultado is not None:
                nomes[col_idx] = resultado
                break

    # Pass 2: Desambiguacao multi-coluna
    nomes = _desambiguar_datas(nomes, df)
    nomes = _desambiguar_valores(nomes, df)
    nomes = _desambiguar_uh(nomes, df)
    nomes = _desambiguar_codigos(nomes)

    # Fallback: colunas nao classificadas recebem col_N
    n_inferidas = 0
    for i in range(n_cols):
        if nomes[i] is None:
            nomes[i] = f"col_{i}"
        else:
            n_inferidas += 1

    # Garante unicidade — todos Nones ja foram substituidos
    nomes_limpos: list[str] = [str(n) for n in nomes]
    nomes_finais = _garantir_unicidade(nomes_limpos)

    # Confidence note
    if n_inferidas == n_cols:
        confidence_note = "nomes inferidos dos dados com alta confianca"
    elif n_inferidas > 0:
        confidence_note = "nomes parcialmente inferidos dos dados"
    else:
        confidence_note = "nomes genericos (col_0...) - inferencia de dados sem exito"

    return nomes_finais, confidence_note
