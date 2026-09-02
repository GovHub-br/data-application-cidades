"""Normalização de domínio — localização (UF, município, código IBGE) e status.

Módulo novo (OpenSpec change ``padronizacao-dominio-nulos-duplicados``) que
oferece canonicalização de domínios:

* **Localização:** ``normalizar_uf``, ``normalizar_municipio`` e
  ``normalizar_codigo_ibge`` normalizam valores brutos contra a referência
  oficial IBGE (``data/referencia_ibge.csv``, 5570 municípios — fonte de
  verdade de localização).
* **Status:** ``normalizar_status`` mapeia valores brutos → canônicos via
  ``data/mapa_status.csv`` (política de nulos/duplicados ciente de APF).
* ``normalizar_localizacao`` aplica as três funções às colunas de
  localização detectadas, **sem descartar linhas**: preserva as colunas
  originais, adiciona colunas canônicas e gera sinalização de não-mapeado
  (``_localizacao_nao_mapeada`` + contagem de cobertura em ``df.attrs``).

A política de nulos/duplicados (``classificar_nulos``,
``converter_placeholders`` e dedup SEMÂNTICA ciente de APF) vive em
``tratamento_nulos_duplicados.py``.

Todas as funções são puras (sem I/O) exceto os carregadores
``carregar_referencia_ibge`` e ``carregar_mapa_status``.
"""

from __future__ import annotations

import logging
import math
import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import cast

import pandas as pd

logger = logging.getLogger(__name__)


def _eh_nulo(valor: object) -> bool:
    """True para nulos escalares (None, NaN, pd.NA, NaT).

    Evita ``pd.isna`` genérico que, segundo os stubs, pode retornar array;
    como aqui o input é sempre escalar, a checagem explícita é determinística.
    """
    if valor is None or valor is pd.NA or valor is pd.NaT:
        return True
    if isinstance(valor, float):
        return math.isnan(valor)
    return False


# ──────────────────────────────────────────────────────────────────────
# Caminhos padrão (projeto: <root>/data/)
# ──────────────────────────────────────────────────────────────────────

# src/classificacao/normalizacao_dominio.py -> parents[0]=classificacao,
# parents[1]=src, parents[2]=raiz do projeto.
_PROJETO_DATA: Path = Path(__file__).resolve().parents[2] / "data"
_CAMINHO_REFERENCIA: Path = _PROJETO_DATA / "referencia_ibge.csv"
_CAMINHO_MAPA_STATUS: Path = _PROJETO_DATA / "mapa_status.csv"


# ──────────────────────────────────────────────────────────────────────
# Utilitários de texto
# ──────────────────────────────────────────────────────────────────────


def _normalizar_texto(valor: object) -> str:
    """NFKD remove acentos → ASCII → lowercase → colapsa espaços.

    Usado como forma canônica de comparação para nomes e valores.
    """
    texto = unicodedata.normalize("NFKD", str(valor))
    texto = texto.encode("ascii", "ignore").decode("ascii")
    texto = texto.lower()
    return re.sub(r"\s+", " ", texto).strip()


def _normalizar_nome_coluna(nome: str) -> str:
    """Normaliza nome de coluna para detecção (underscores, sem acentos)."""
    texto = _normalizar_texto(nome)
    texto = re.sub(r"[^a-z0-9_]", "_", texto)
    texto = re.sub(r"_+", "_", texto).strip("_")
    return texto or "col"


def _eh_uf(uf: str) -> bool:
    return len(uf) == 2 and uf.isalpha() and uf.upper() in _UFS


# ──────────────────────────────────────────────────────────────────────
# UF
# ──────────────────────────────────────────────────────────────────────

# Nome completo do estado (normalizado) -> UF de 2 letras.
_UF_POR_ESTADO: dict[str, str] = {
    "acre": "AC",
    "alagoas": "AL",
    "amapa": "AP",
    "amazonas": "AM",
    "bahia": "BA",
    "ceara": "CE",
    "distrito federal": "DF",
    "espirito santo": "ES",
    "goias": "GO",
    "maranhao": "MA",
    "mato grosso": "MT",
    "mato grosso do sul": "MS",
    "minas gerais": "MG",
    "para": "PA",
    "paraiba": "PB",
    "parana": "PR",
    "pernambuco": "PE",
    "piaui": "PI",
    "rio de janeiro": "RJ",
    "rio grande do norte": "RN",
    "rio grande do sul": "RS",
    "rondonia": "RO",
    "roraima": "RR",
    "santa catarina": "SC",
    "sao paulo": "SP",
    "sergipe": "SE",
    "tocantins": "TO",
}

_UFS: frozenset[str] = frozenset(_UF_POR_ESTADO.values())

# Sufixo do tipo " - UF" (ex.: "Sao Paulo - SP").
_SUFIXO_UF = re.compile(r"\s*-\s*([a-z]{2})$")


def normalizar_uf(valor: object) -> str | None:
    """Normaliza para UF de 2 letras.

    Examples
    --------
    >>> normalizar_uf("SP")
    'SP'
    >>> normalizar_uf("sp")
    'SP'
    >>> normalizar_uf("São Paulo")
    'SP'
    >>> normalizar_uf("Sao Paulo - SP")
    'SP'
    >>> normalizar_uf("Minas Gerais")
    'MG'
    >>> normalizar_uf("ZZ")
    >>> normalizar_uf("")
    >>> normalizar_uf(None)
    """
    if _eh_nulo(valor):
        return None

    texto = _normalizar_texto(valor)
    if not texto:
        return None

    # Formato direto "XX"
    if _eh_uf(texto):
        return texto.upper()

    # Remove sufixo " - XX" (ex.: "Sao Paulo - SP" -> "sao paulo")
    sem_sufixo = _SUFIXO_UF.sub("", texto)
    if sem_sufixo in _UF_POR_ESTADO:
        return _UF_POR_ESTADO[sem_sufixo]

    # Nome completo do estado
    if texto in _UF_POR_ESTADO:
        return _UF_POR_ESTADO[texto]

    return None


# ──────────────────────────────────────────────────────────────────────
# Referência IBGE
# ──────────────────────────────────────────────────────────────────────

# Cache por caminho: path -> DataFrame | None (None = indisponível).
_REFERENCIA_CACHE: dict[Path, pd.DataFrame | None] = {}


def carregar_referencia_ibge(path: Path | str | None = None) -> pd.DataFrame | None:
    """Carrega ``data/referencia_ibge.csv`` (fonte oficial IBGE).

    Retorna DataFrame com colunas ``codigo_ibge`` (str, zero-padded a 7
    dígitos), ``nome_municipio`` e ``uf``. Retorna ``None`` se o arquivo
    não existir, estiver vazio ou não tiver as colunas esperadas — nunca
    levanta exceção (a ausência da referência não pode quebrar o pipeline).

    Parameters
    ----------
    path : Path or str or None
        Caminho alternativo. Se ``None`` (default), usa
        ``data/referencia_ibge.csv`` no diretório do projeto.
    """
    if path is None:
        path = _CAMINHO_REFERENCIA
    path = Path(path)

    if path in _REFERENCIA_CACHE:
        return _REFERENCIA_CACHE[path]

    if not path.exists():
        logger.warning("Referência IBGE não encontrada em %s", path)
        _REFERENCIA_CACHE[path] = None
        return None

    try:
        df = pd.read_csv(path, sep=",", dtype={"codigo_ibge": str})
    except Exception as exc:  # noqa: BLE001 - referência é opcional
        logger.warning("Erro ao ler referência IBGE %s: %s", path, exc)
        _REFERENCIA_CACHE[path] = None
        return None

    if df.empty or not {"codigo_ibge", "nome_municipio", "uf"} <= set(df.columns):
        logger.warning("Referência IBGE %s sem colunas esperadas", path)
        _REFERENCIA_CACHE[path] = None
        return None

    df = df.copy()
    df["codigo_ibge"] = df["codigo_ibge"].astype(str).str.strip().str.zfill(7)
    df["uf"] = df["uf"].astype(str).str.strip().str.upper()
    df["nome_municipio"] = df["nome_municipio"].astype(str).str.strip()

    _REFERENCIA_CACHE[path] = df
    return df


def _referencia_ibge() -> pd.DataFrame | None:
    """Referência IBGE do caminho padrão (usada pelas funções puras)."""
    return carregar_referencia_ibge(None)


@lru_cache(maxsize=1)
def _municipios_por_nome() -> dict[str, list[tuple[str, str]]]:
    """Índice {nome_normalizado: [(uf, nome_canonico), ...]}."""
    idx: dict[str, list[tuple[str, str]]] = {}
    ref = _referencia_ibge()
    if ref is None:
        return idx
    for nome, uf in zip(ref["nome_municipio"], ref["uf"]):
        texto = _normalizar_texto(nome)
        idx.setdefault(texto, []).append((uf, nome))
    return idx


@lru_cache(maxsize=1)
def _codigos_ibge() -> frozenset[str]:
    """Conjunto de códigos IBGE válidos (zero-padded, 7 dígitos)."""
    ref = _referencia_ibge()
    if ref is None:
        return frozenset()
    return frozenset(ref["codigo_ibge"])


# ──────────────────────────────────────────────────────────────────────
# Município
# ──────────────────────────────────────────────────────────────────────


def normalizar_municipio(nome: object, uf: object | None = None) -> str | None:
    """Canonicaliza nome de município contra ``referencia_ibge.csv``.

    O input é normalizado (NFKD remove acentos, lowercase, colapsa espaços,
    remove sufixo ``" - UF"``) e comparado por match exato normalizado com a
    referência. Se ``uf`` for informado, prefere municípios do mesmo UF.

    Retorna o nome canônico oficial do IBGE (com acentos) ou ``None`` quando
    não há match (valor não-mapeado) ou o match é ambíguo (mesmo nome
    normalizado em múltiplos UFs sem ``uf`` para desambiguar).

    Parameters
    ----------
    nome : object
        Nome bruto do município.
    uf : object or None
        UF bruta (ex.: ``"SP"``, ``"São Paulo"``) usada como dica de
        desambiguação.

    Returns
    -------
    str or None
        Nome canônico do IBGE ou ``None``.
    """
    if _eh_nulo(nome):
        return None

    texto = _normalizar_texto(nome)
    if not texto:
        return None

    # Remove sufixo " - UF" (ex.: "Sao Paulo - SP" -> "sao paulo")
    texto = _SUFIXO_UF.sub("", texto)

    candidatos = _municipios_por_nome().get(texto, [])
    if not candidatos:
        return None

    if uf is not None:
        uf_norm = normalizar_uf(uf)
        if uf_norm is not None:
            do_mesmo_uf = [c for c in candidatos if c[0] == uf_norm]
            if len(do_mesmo_uf) == 1:
                return do_mesmo_uf[0][1]
            if len(do_mesmo_uf) > 1:
                return None  # ambíguo mesmo com UF
        # sem candidato no mesmo UF: cai para a regra geral abaixo

    if len(candidatos) == 1:
        return candidatos[0][1]

    return None  # ambíguo: mesmo nome em múltiplos UFs e sem UF para desambiguar


# ──────────────────────────────────────────────────────────────────────
# Código IBGE
# ──────────────────────────────────────────────────────────────────────


def normalizar_codigo_ibge(valor: object) -> str | None:
    """Zero-pad para 7 dígitos e valida presença em ``referencia_ibge.csv``.

    Lida com floats renderizados como ``"3550308.0"`` e códigos com menos de
    7 dígitos (ex.: ``"410315"`` → ``"0410315"``). Valores não numéricos,
    com mais de 7 dígitos ou ausentes da referência retornam ``None``.

    Parameters
    ----------
    valor : object
        Código IBGE bruto (str, int ou float).

    Returns
    -------
    str or None
        Código IBGE canônico de 7 dígitos ou ``None``.
    """
    if _eh_nulo(valor):
        return None

    s = str(valor).strip()
    if not s:
        return None

    # Float renderizado como "3550308.0"
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".")[0]
    elif not s.isdigit():
        try:
            f = float(s)
            if not f.is_integer():
                return None
            s = str(int(f))
        except (ValueError, TypeError):
            return None

    if len(s) > 7:
        return None

    codigo = s.zfill(7)
    if codigo in _codigos_ibge():
        return codigo
    return None


# ──────────────────────────────────────────────────────────────────────
# Detecção de colunas de localização
# ──────────────────────────────────────────────────────────────────────

# Colunas canônicas geradas por ``normalizar_localizacao`` — nunca podem ser
# re-detectadas como fonte (idempotência).
_COLUNAS_CANONICAS_LOCALIZACAO: frozenset[str] = frozenset(
    {"uf_canonico", "municipio_canonico", "codigo_ibge_canonico"}
)
_COLUNAS_RESERVADAS: frozenset[str] = frozenset(
    _COLUNAS_CANONICAS_LOCALIZACAO | {"_localizacao_nao_mapeada"}
)


def _detectar_colunas_localizacao(df: pd.DataFrame) -> dict[str, list[str]]:
    """Detecta colunas de localização (nome normalizado).

    * ``uf``: token ``uf`` no nome (ex.: ``uf``, ``sg_uf``, ``sg_uf_imovel``).
    * ``municipio``: nome contém ``municipio`` (ex.: ``municipio``,
      ``no_municipio``, ``nome_municipio``) — exclui colunas de código IBGE.
    * ``codigo_ibge``: nome contém ``ibge``/``cod_ibge``/``codigo_ibge`` ou
      começa com ``cod_municipio`` (ex.: ``cod_municipio_ibge``,
      ``co_municipio_ibge``, ``codigo_ibge``).

    Colunas canônicas já geradas (``*_canonico``) e a coluna de sinalização
    são ignoradas para manter a operação idempotente.
    """
    resultado: dict[str, list[str]] = {"uf": [], "municipio": [], "codigo_ibge": []}

    for col in df.columns:
        nome = _normalizar_nome_coluna(str(col))
        if nome in _COLUNAS_RESERVADAS or nome.endswith("_canonico"):
            continue

        if "ibge" in nome or nome.startswith("cod_municipio"):
            resultado["codigo_ibge"].append(col)
            continue

        if "municipio" in nome:
            resultado["municipio"].append(col)
            continue

        if "uf" in nome.split("_"):
            resultado["uf"].append(col)

    return resultado


def _como_string_nullable(series: pd.Series) -> pd.Series:
    """Converte série (possivelmente com ``None``) para ``string`` nullable."""
    return pd.Series(series.to_list(), index=series.index, dtype="string")


def _valor_nao_vazio(valor: object) -> bool:
    """True se o valor existe (não é null/NaN e não é string em branco)."""
    if _eh_nulo(valor):
        return False
    if isinstance(valor, str):
        return bool(valor.strip())
    return True


def _mapear_municipios(
    serie_mun: pd.Series, uf_hint: pd.Series | None
) -> list[str | None]:
    """Mapeia nomes de município usando a UF canônica por linha como dica."""
    valores: list[str | None] = []
    hints = uf_hint if uf_hint is not None else [None] * len(serie_mun)
    for nome, uf in zip(serie_mun, hints):
        if _valor_nao_vazio(nome):
            valores.append(normalizar_municipio(nome, uf))
        else:
            valores.append(None)
    return valores


def _sinalizar_nao_mapeado(df: pd.DataFrame, pares: list[tuple[str, str]]) -> pd.Series:
    """Flag por linha: True se alguma coluna de localização tem valor
    preenchido cujo canônico não pôde ser derivado."""
    nao_mapeado = pd.Series(False, index=df.index)
    for origem, canonica in pares:
        for i in df.index:
            if _valor_nao_vazio(df.at[i, origem]) and _eh_nulo(df.at[i, canonica]):
                nao_mapeado.at[i] = True
    return nao_mapeado


def normalizar_localizacao(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica ``normalizar_uf``/``normalizar_municipio``/``normalizar_codigo_ibge``
    às colunas de localização detectadas.

    * **NÃO descarta linhas** — preserva as colunas originais.
    * Adiciona colunas canônicas para cada grupo detectado:
      ``uf_canonico``, ``municipio_canonico``, ``codigo_ibge_canonico``
      (todas ``string`` nullable; valores não-mapeados ficam ``pd.NA``).
    * ``municipio_canonico`` usa ``uf_canonico`` por linha como dica de
      desambiguação.
    * Adiciona sinalização ``_localizacao_nao_mapeada`` (bool): True quando
      alguma coluna de localização tem valor preenchido que não pôde ser
      mapeado para canônico.
    * Registra contagem de cobertura em ``df.attrs["_localizacao_cobertura"]``
      (``n_linhas``, ``n_nao_mapeadas``, ``colunas_detectadas``).

    A operação é idempotente: re-executar sobre o resultado produz o mesmo
    DataFrame (as colunas canônicas são re-detectadas como reservadas).
    """
    df = df.copy()
    colunas = _detectar_colunas_localizacao(df)

    # UF
    if colunas["uf"]:
        origem_uf = colunas["uf"][0]
        serie_uf = cast(pd.Series, df[origem_uf])
        df["uf_canonico"] = _como_string_nullable(serie_uf.map(normalizar_uf))

    # Código IBGE
    if colunas["codigo_ibge"]:
        origem_ibge = colunas["codigo_ibge"][0]
        serie_ibge = cast(pd.Series, df[origem_ibge])
        df["codigo_ibge_canonico"] = _como_string_nullable(
            serie_ibge.map(normalizar_codigo_ibge)
        )

    # Município (dica de UF por linha, se houver uf_canonico)
    if colunas["municipio"]:
        origem_mun = colunas["municipio"][0]
        serie_mun = cast(pd.Series, df[origem_mun])
        uf_hint: pd.Series | None = (
            cast(pd.Series, df["uf_canonico"]) if "uf_canonico" in df.columns else None
        )
        df["municipio_canonico"] = _como_string_nullable(
            pd.Series(_mapear_municipios(serie_mun, uf_hint), index=df.index)
        )

    # Sinalização de não-mapeado (apenas se há alguma coluna de localização)
    pares: list[tuple[str, str]] = []
    if colunas["uf"]:
        pares.append((colunas["uf"][0], "uf_canonico"))
    if colunas["codigo_ibge"]:
        pares.append((colunas["codigo_ibge"][0], "codigo_ibge_canonico"))
    if colunas["municipio"]:
        pares.append((colunas["municipio"][0], "municipio_canonico"))

    if pares:
        df["_localizacao_nao_mapeada"] = _sinalizar_nao_mapeado(df, pares)

    n_nao_mapeadas = 0
    if "_localizacao_nao_mapeada" in df.columns:
        serie_flag = cast(pd.Series, df["_localizacao_nao_mapeada"])
        n_nao_mapeadas = int(serie_flag.sum())

    df.attrs["_localizacao_cobertura"] = {
        "n_linhas": len(df),
        "n_nao_mapeadas": n_nao_mapeadas,
        "colunas_detectadas": {k: v for k, v in colunas.items() if v},
    }
    return df


# ──────────────────────────────────────────────────────────────────────
# Status
# ──────────────────────────────────────────────────────────────────────


def carregar_mapa_status(path: Path | str | None = None) -> dict[str, str | None]:
    """Carrega ``data/mapa_status.csv`` (colunas ``valor_bruto``,
    ``valor_canonico``, ``classe``).

    Retorna dict ``{valor_bruto: valor_canonico}``. Valores com
    ``classe=pendente`` (ou ``valor_canonico`` vazio) são mapeados para
    ``None`` — representam status a preencher/desconhecidos. Retorna dict
    vazio se o arquivo não existir ou for inválido.

    Parameters
    ----------
    path : Path or str or None
        Caminho alternativo. Se ``None`` (default), usa
        ``data/mapa_status.csv`` no diretório do projeto.
    """
    if path is None:
        path = _CAMINHO_MAPA_STATUS
    path = Path(path)

    if not path.exists():
        logger.warning("Mapa de status não encontrado em %s", path)
        return {}

    try:
        df = pd.read_csv(path, sep=",", dtype=str)
    except Exception as exc:  # noqa: BLE001 - mapa é opcional
        logger.warning("Erro ao ler mapa de status %s: %s", path, exc)
        return {}

    mapa: dict[str, str | None] = {}
    for _, row in df.iterrows():
        bruto = row.get("valor_bruto")
        if bruto is None or pd.isna(bruto):
            continue  # vazio não vira chave: normalizar_status trata "" como None
        bruto = str(bruto)
        canonico = row.get("valor_canonico")
        classe = str(row.get("classe", "valido")).strip().lower()
        if (
            classe == "pendente"
            or canonico is None
            or pd.isna(canonico)
            or not str(canonico).strip()
        ):
            mapa[bruto] = None
        else:
            mapa[bruto] = str(canonico)

    return mapa


def _normalizar_mapa(mapa: dict[str, str | None]) -> dict[str, str | None]:
    """Normaliza chaves do mapa para lookup por forma canônica."""
    return {_normalizar_texto(k): v for k, v in mapa.items()}


def normalizar_status(series: pd.Series, mapa: dict[str, str | None]) -> pd.Series:
    """Mapeia status bruto → canônico via ``mapa``.

    * Valores nulos/vazios → ``None`` (classe ``pendente``).
    * Valores com chave no mapa (comparação por forma canônica, insensível a
      acentos/caixa/espaços) → valor canônico (que pode ser ``None``).
    * Valores NÃO mapeados são **preservados** (não-destrutivo): a política
      nunca apaga status real não coberto pelo seed.

    Parameters
    ----------
    series : pd.Series
        Série de status brutos.
    mapa : dict[str, str | None]
        Mapeamento ``{valor_bruto: valor_canonico}`` (ver
        ``carregar_mapa_status``).

    Returns
    -------
    pd.Series
        Série com status canônicos (``None`` para pendente).
    """
    mapa_norm = _normalizar_mapa(mapa)

    def _mapear(valor: object) -> object:
        if _eh_nulo(valor):
            return None
        texto = _normalizar_texto(valor)
        if not texto:
            return None
        if texto in mapa_norm:
            return mapa_norm[texto]
        return valor  # preserva valor não-mapeado

    # String nullable: pendente/desconhecido viram pd.NA; valores preservados
    # permanecem como string.
    return _como_string_nullable(series.map(_mapear))


def detectar_colunas_status(df: pd.DataFrame) -> list[str]:
    """Detecta colunas de status pelo nome normalizado (token ``situacao`` ou
    ``status``). Colunas canônicas (``*_canonico``) são ignoradas para manter
    a operação idempotente."""
    resultado: list[str] = []
    for col in df.columns:
        nome = _normalizar_nome_coluna(str(col))
        if nome.endswith("_canonico") or nome in _COLUNAS_RESERVADAS:
            continue
        tokens = nome.split("_")
        if "situacao" in tokens or "status" in tokens:
            resultado.append(col)
    return resultado
