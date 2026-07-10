"""Normalização de nomes de tabela e coluna para matching fuzzy.

O batimento entre schemas com convenções de nomenclatura radicalmente
diferentes (dump 2009–2018 vs SFTP 2020–2026) exige normalização para
permitir matching semântico. Este módulo fornece funções para:

- Extrair o *stem canônico* de um nome de tabela (removendo prefixos
  institucionais/temporais e sufixos de variante).
- Normalizar nomes de coluna para comparação (lowercase, sem acentos).
- Extrair anos e prefixo institucional de nomes de tabela.

Design de normalização (D2)
----------------------------
A função ``canonicalizar_stem`` aplica remoções em ordem:

1. **Prefixos**: ``gefus_anteriores_``, ``gefus_``, ``o_recente_``,
   ``caixa_NNN_``, ``bb_NNN_``, ``int\\d{3}_``, ``_\\d{6}_``
2. **Sufixos**: ``_\\d{6}$``, ``_m\\d{8}$``, ``_af_(caixa|bb)$``,
   ``_entregas$``, ``_\\d{4}$`` (variantes 0001 etc.),
   ``_v\\d+$``, ``_da_entrega_da_unidade$``
3. **Normalização**: lowercase, remover underscores duplicados, strip ``_``.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Optional


# ---------------------------------------------------------------------------
# Constantes de normalização – Design D2
# ---------------------------------------------------------------------------

_PREFIXOS: list[tuple[re.Pattern[str], str]] = [
    # historico_recente_* — prefixo completo (prioritário, antes das formas truncadas)
    (re.compile(r"^historico_recente_"), ""),
    # gefus_anteriores_*
    (re.compile(r"^gefus_anteriores_"), ""),
    # gefus_*
    (re.compile(r"^gefus(?![a-z])"), ""),
    # ecente_* — truncamento de historico_recente_ (PostgreSQL 63‑char limit)
    (re.compile(r"^ecente_"), ""),
    # storico_recente_* — truncamento alternativo de historico_recente_
    (re.compile(r"^storico_recente_"), ""),
    # o_recente_*
    (re.compile(r"^o_recente_"), ""),
    # caixa_NNN_ (ex: caixa_001_)
    (re.compile(r"^caixa_\d{3}_"), ""),
    # bb_NNN_ (ex: bb_001_)
    (re.compile(r"^bb_\d{3}_"), ""),
    # intNNN_ (ex: int058_)
    (re.compile(r"^int\d{3}_"), ""),
    # _YYYYMM_ no início (com sublinhado)
    (re.compile(r"^_\d{6}_"), ""),
    # YYYYMM_ no início (residual após remoção de prefixo institucional)
    (re.compile(r"^\d{6}_"), ""),
    # YYYY_ no início (residual — ano seguido de sublinhado)
    (re.compile(r"^\d{4}_"), ""),
    # prefixo numérico tipo 001_ ou "001_ ou 01_ no início
    (re.compile(r'^"?0*\d{1,3}_'), ""),
    # prefixo histórico tipo 024_ (usado em alguns nomes dump)
    (re.compile(r"^\d{3}_"), ""),
]

_SUFIXOS: list[tuple[re.Pattern[str], str]] = [
    # _YYYYMMDD no final (ex: _20231228)
    (re.compile(r"_\d{8}$"), ""),
    # _mYYYYMMDD no final (ex: _m20210831)
    (re.compile(r"_m\d{8}$"), ""),
    # _YYYYMM no final (ex: _202406)
    (re.compile(r"_\d{6}$"), ""),
    # _af_caixa ou _af_bb
    (re.compile(r"_af_(caixa|bb)$"), ""),
    # _entregas no final
    (re.compile(r"_entregas$"), ""),
    # _da_entrega_da_unidade no final
    (re.compile(r"_da_entrega_da_unidade(_af_(bb|caixa))?$"), ""),
    # _vN no final (versões)
    (re.compile(r"_v\d+$"), ""),
    # _NNNN no final (variantes 0001, 0002, etc.) -- só se for apenas dígitos
    (re.compile(r"_\d{4}$"), ""),
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def canonicalizar_stem(nome: str) -> str:
    """Extrai o stem canônico de um nome de tabela.

    Aplica remoção de prefixos institucionais/temporais, sufixos de
    variante e normalização. A função é idempotente.

    Parameters
    ----------
    nome : str
        Nome original da tabela.

    Returns
    -------
    str
        Stem canônico normalizado.

    Examples
    --------
    >>> canonicalizar_stem("gefus_anteriores_202408_snh_pmcmv_dados_prioritarios_af_caixa")
    'snh_pmcmv_dados_prioritarios'

    >>> canonicalizar_stem("o_recente_202406_snh_pmcmv_dados_prioritarios_af_caixa_entregas")
    'snh_pmcmv_dados_prioritarios'

    >>> canonicalizar_stem("caixa_001_2016_02_fevereiro_relatorio_cidades_20160229_v2")
    'relatorio_cidades'
    """
    stem = nome.strip().strip('"')

    # 1. Remover prefixos (ordem importa: mais específicos primeiro)
    for pattern, replacement in _PREFIXOS:
        stem = pattern.sub(replacement, stem, count=1)

    # 2. Remover sufixos (loop repetido até estabilizar para idempotência)
    while True:
        previous = stem
        for pattern, replacement in _SUFIXOS:
            stem = pattern.sub(replacement, stem, count=1)
        if stem == previous:
            break

    # 3. Normalizar: lowercase, remover underscores múltiplos, strip
    stem = stem.lower()
    stem = re.sub(r"_+", "_", stem)
    stem = stem.strip("_")

    return stem


def normalizar_nome_coluna(nome: str) -> str:
    """Normaliza nome de coluna para comparação fuzzy.

    - Converte para lowercase.
    - Remove acentos e caracteres não-ASCII via NFKD.
    - Substitui caracteres não alfanuméricos por underscore.
    - Colapsa underscores múltiplos.
    - Remove underscore inicial e final.

    Parameters
    ----------
    nome : str
        Nome original da coluna.

    Returns
    -------
    str
        Nome normalizado.

    Examples
    --------
    >>> normalizar_nome_coluna("Dt_Nasc")
    'dt_nasc'

    >>> normalizar_nome_coluna("dt_nascimento")
    'dt_nascimento'
    """
    nome = nome.strip().strip('"').lower()
    # NFKD decomposition removes accents
    nome = unicodedata.normalize("NFKD", nome)
    nome = "".join(c for c in nome if not unicodedata.combining(c))
    # Replace non-alphanumeric with underscore
    nome = re.sub(r"[^a-z0-9]+", "_", nome)
    nome = re.sub(r"_+", "_", nome)
    return nome.strip("_")


def extrair_anos(nome: str) -> set[int]:
    """Extrai todos os anos (4 dígitos começando com 20) do nome da tabela.

    Parameters
    ----------
    nome : str
        Nome da tabela.

    Returns
    -------
    set[int]
        Conjunto de anos encontrados.

    Examples
    --------
    >>> extrair_anos("caixa_001_2016_02_fevereiro_relatorio_cidades_20160229_v2")
    {2016}
    """
    return {int(m) for m in re.findall(r"20\d{2}", nome)}


def extrair_prefixo_institucional(nome: str) -> Optional[str]:
    """Extrai o prefixo institucional (caixa, bb) do nome da tabela.

    Parameters
    ----------
    nome : str
        Nome da tabela.

    Returns
    -------
    str or None
        ``"caixa"``, ``"bb"``, ou ``None`` se não identificado.

    Examples
    --------
    >>> extrair_prefixo_institucional("caixa_001_2016_02_fevereiro_relatorio")
    'caixa'

    >>> extrair_prefixo_institucional("bb_003_2017_bext_out2017")
    'bb'

    >>> extrair_prefixo_institucional("gefus_anteriores_snh_pmcmv")
    """
    nome = nome.strip().strip('"').lower()
    if re.match(r"^(caixa|bb)_\d{3}_", nome) or re.match(r"^(caixa|bb)_af_", nome):
        m = re.match(r"^(caixa|bb)", nome)
        if m:
            return m.group(1)
    return None
