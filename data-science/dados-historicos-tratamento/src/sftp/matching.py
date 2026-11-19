"""Matching estrutural em 3 camadas e identificação de chaves candidatas.

Implementa o coração do batimento: as três camadas progressivas de matching
(Design D1) e a extração de campos em comum, chaves candidatas e divergências.

Camadas
-------
1. **Hash exato (MD5):** tabelas com mesma definição estrutural (nome, tipo,
   notnull, default, posição de cada coluna). Confiança alta.
2. **Stem canônico:** tabelas cujo nome produz o mesmo stem após normalização
   (:func:`~sftp.normalizacao.canonicalizar_stem`), desde que a similaridade
   Jaccard das colunas seja ≥ 0.3. Confiança média.
3. **Similaridade de colunas (Jaccard):** para tabelas sem match nas camadas
   anteriores, varredura O(n×m) com limiar Jaccard ≥ 0.5. Confiança baixa.

Named tuples de resultado
--------------------------
- ``BatimentoResult``: 4 DataFrames (relacionadas, campos_em_comum, chaves, divergencias)
- ``DivergenciasResult``: tabelas só SFTP, só dump, divergentes por stem
"""

from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from typing import NamedTuple

import pandas as pd

from .normalizacao import canonicalizar_stem, extrair_anos, normalizar_nome_coluna

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Named tuples de resultado
# ---------------------------------------------------------------------------


class BatimentoResult(NamedTuple):
    """Resultado completo do batimento estrutural."""

    relacionadas: pd.DataFrame
    """Pares de tabelas relacionadas (tabela_sftp, tabela_dump, metodo, confianca,
    score_similaridade, qtd_campos_comum)."""

    campos_em_comum: pd.DataFrame
    """Colunas em comum por par (tabela_sftp, tabela_dump, campo, tipo_sftp,
    tipo_dump, match_tipo)."""

    chaves: pd.DataFrame
    """Chaves de cruzamento candidatas (chave, padrao, qtd_tabelas_sftp,
    qtd_tabelas_dump, tabelas_exemplo)."""

    divergencias: pd.DataFrame
    """Divergências estruturais (categoria, tabela, schema, familia, qtd_tabelas,
    observacao)."""


# ---------------------------------------------------------------------------
# Constantes de chave
# ---------------------------------------------------------------------------

CHAVE_PATTERNS: list[tuple[str, str]] = [
    (r"\bcpf\b", "cpf"),
    (r"\bcnpj\b", "cnpj"),
    (r"\bcontrato\b", "contrato"),
    (r"\bempreendimento_nu\b", "empreendimento_nu"),
    (r"\bnu_apf\b", "nu_apf"),
    (r"\banomes\b", "anomes"),
    (r"\bcod\b", "cod"),
    (r"\bcodigo\b", "codigo"),
    (r"\bmunicipio\b", "municipio"),
    (r"\bibge_no\b", "ibge_no"),
    (r"\buh_contratados\b", "uh_contratados"),
]

# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------


def _build_col_index(inventario: pd.DataFrame) -> dict[str, set[str]]:
    """Constrói índice: nome_tabela → conjunto de nomes de coluna normalizados."""
    idx: dict[str, set[str]] = {}
    for _, row in inventario.iterrows():
        tabela = str(row["table_name"]).strip().strip('"')
        col = normalizar_nome_coluna(str(row["column_name"]))
        idx.setdefault(tabela, set()).add(col)
    return idx


def _build_col_types(inventario: pd.DataFrame) -> dict[str, dict[str, str]]:
    """Constrói índice: nome_tabela → {nome_coluna: data_type}."""
    idx: dict[str, dict[str, str]] = {}
    for _, row in inventario.iterrows():
        tabela = str(row["table_name"]).strip().strip('"')
        col = str(row["column_name"]).strip().strip('"')
        dtype = str(row["data_type"]).strip()
        idx.setdefault(tabela, {})[col] = dtype
    return idx


def jaccard_colunas(cols_a: set[str], cols_b: set[str]) -> float:
    """Calcula o índice de Jaccard entre dois conjuntos de colunas.

    J(A, B) = |A ∩ B| / |A ∪ B|.

    Retorna 0.0 se a união for vazia.
    """
    if not cols_a and not cols_b:
        return 0.0
    intersec = len(cols_a & cols_b)
    uniao = len(cols_a | cols_b)
    return intersec / uniao if uniao > 0 else 0.0


def _agrupar_familia(nomes: set[str]) -> str:
    """Agrupa nomes de tabela em uma categoria de família para o relatório de divergências.

    Heurística baseada em prefixos comuns.
    """
    prefixes: dict[str, str] = {
        "gefus_anteriores_": "gefus_anteriores_*",
        "gefus_": "gefus_*",
        "int0": "intNNN_*",
        "_202": "_YYYYMM_* (snapshots mensais)",
        "caixa_": "caixa_*",
        "bb_": "bb_*",
    }
    amostra = list(nomes)[:10]  # amostra para determinar família
    prefixos_encontrados: dict[str, int] = defaultdict(int)
    for n in amostra:
        n = n.strip().strip('"').lower()
        for prefix_key, label in prefixes.items():
            if n.startswith(prefix_key):
                prefixos_encontrados[label] += 1
    if prefixos_encontrados:
        return max(prefixos_encontrados, key=lambda k: prefixos_encontrados[k])
    # Fallback: usa o prefixo comum mais longo entre os 3 primeiros nomes
    nomes_lista = sorted(amostra)
    if len(nomes_lista) >= 2:
        comum = nomes_lista[0]
        for n in nomes_lista[1:3]:
            i = 0
            while i < min(len(comum), len(n)) and comum[i] == n[i]:
                i += 1
            comum = comum[:i]
        if len(comum) > 3:
            return comum + "*"
    return "outros"


# ---------------------------------------------------------------------------
# Camada 1 — Hash exato
# ---------------------------------------------------------------------------


def camada1_hash_exato(
    tabelas_sftp: set[str],
    tabelas_dh: set[str],
    index_hash: dict[str, set[str]],
) -> pd.DataFrame:
    """Encontra pares de tabelas com hash estrutural idêntico (Camada 1).

    Percorre o índice de hash, identificando grupos que contêm tabelas
    de ambos os schemas (``sftp.*`` e ``dados_historicos.*``).

    Parameters
    ----------
    tabelas_sftp : set[str]
        Conjunto de nomes de tabela do SFTP (formato ``sftp.tabela`` ou só ``tabela``).
    tabelas_dh : set[str]
        Conjunto de nomes de tabela do dump.
    index_hash : dict[str, set[str]]
        Índice hash → conjunto de ``schema.tabela``.

    Returns
    -------
    pd.DataFrame
        Colunas: ``tabela_sftp, tabela_dump, hash_estrutura, metodo, confianca``.
    """
    pares: list[dict] = []

    for h, tabs in index_hash.items():
        sftp_in_group = {t for t in tabs if t.startswith("sftp.") or t in tabelas_sftp}
        dh_in_group = {
            t for t in tabs if t.startswith("dados_historicos.") or t in tabelas_dh
        }

        if sftp_in_group and dh_in_group:
            for s, d in itertools.product(sorted(sftp_in_group), sorted(dh_in_group)):
                pares.append(
                    {
                        "tabela_sftp": s.replace("sftp.", ""),
                        "tabela_dump": d.replace("dados_historicos.", ""),
                        "hash_estrutura": h,
                        "metodo": "hash_exato",
                        "confianca": "alta",
                        "score_similaridade": 1.0,
                    }
                )

    if pares:
        logger.info(
            "Camada 1 (hash exato): %d pares encontrados em %d grupos",
            len(pares),
            len({p["hash_estrutura"] for p in pares}),
        )
    else:
        logger.info("Camada 1 (hash exato): nenhum par encontrado")

    return pd.DataFrame(
        pares,
        columns=[
            "tabela_sftp",
            "tabela_dump",
            "hash_estrutura",
            "metodo",
            "confianca",
            "score_similaridade",
        ],
    )


# ---------------------------------------------------------------------------
# Camada 2 — Stem canônico
# ---------------------------------------------------------------------------


def camada2_stem_canonico(
    tabelas_sftp_sem_match: set[str],
    tabelas_dh: set[str],
    colunas_sftp: dict[str, set[str]],
    colunas_dh: dict[str, set[str]],
) -> pd.DataFrame:
    """Encontra pares por stem canônico com validação de colunas (Camada 2).

    Agrupa tabelas pelo stem e, para cada grupo cross-schema, calcula
    Jaccard de colunas. Só registra pares com Jaccard ≥ 0.3.

    Parameters
    ----------
    tabelas_sftp_sem_match : set[str]
        Tabelas SFTP que não tiveram match na camada 1.
    tabelas_dh : set[str]
        Todas as tabelas do dump (a camada 2 pode reusar tabelas já matched
        na camada 1, pois o objetivo é encontrar relações semânticas).
    colunas_sftp : dict[str, set[str]]
        Índice: nome_tabela → conjunto de colunas normalizadas (SFTP).
    colunas_dh : dict[str, set[str]]
        Índice: nome_tabela → conjunto de colunas normalizadas (dump).

    Returns
    -------
    pd.DataFrame
        Colunas: ``tabela_sftp, tabela_dump, stem, metodo, confianca, score_similaridade``.
    """
    # Agrupar por stem
    stem_sftp: dict[str, set[str]] = defaultdict(set)
    stem_dh: dict[str, set[str]] = defaultdict(set)

    for t in tabelas_sftp_sem_match:
        s = canonicalizar_stem(t)
        if s:
            stem_sftp[s].add(t)

    for t in tabelas_dh:
        s = canonicalizar_stem(t)
        if s:
            stem_dh[s].add(t)

    # Encontrar stems comuns
    stems_comuns = set(stem_sftp.keys()) & set(stem_dh.keys())
    logger.info("Camada 2 (stem): %d stems comuns entre schemas", len(stems_comuns))

    pares: list[dict] = []
    for stem in sorted(stems_comuns):
        sftp_tabs = stem_sftp[stem]
        dh_tabs = stem_dh[stem]

        for t_sftp in sorted(sftp_tabs):
            cols_s = colunas_sftp.get(t_sftp, set())
            best_score = 0.0
            best_match: str | None = None
            for t_dh in sorted(dh_tabs):
                cols_d = colunas_dh.get(t_dh, set())
                j = jaccard_colunas(cols_s, cols_d)
                if j > best_score:
                    best_score = j
                    best_match = t_dh

            if best_match and best_score >= 0.3:
                pares.append(
                    {
                        "tabela_sftp": t_sftp,
                        "tabela_dump": best_match,
                        "stem": stem,
                        "metodo": "stem_canonico",
                        "confianca": "media",
                        "score_similaridade": round(best_score, 4),
                    }
                )

    logger.info("Camada 2 (stem): %d pares registrados (Jaccard ≥ 0.3)", len(pares))
    return pd.DataFrame(
        pares,
        columns=[
            "tabela_sftp",
            "tabela_dump",
            "stem",
            "metodo",
            "confianca",
            "score_similaridade",
        ],
    )


# ---------------------------------------------------------------------------
# Camada 3 — Similaridade de colunas (Jaccard)
# ---------------------------------------------------------------------------


def camada3_jaccard_colunas(
    tabelas_sftp_sem_match: set[str],
    colunas_sftp: dict[str, set[str]],
    colunas_dh: dict[str, set[str]],
    limiar: float = 0.5,
) -> pd.DataFrame:
    """Varredura O(n×m) por similaridade de colunas (Camada 3).

    Para cada tabela SFTP sem match, varre todas as tabelas dump e retorna
    o par com maior Jaccard, desde que ≥ limiar.

    Parameters
    ----------
    tabelas_sftp_sem_match : set[str]
        Tabelas SFTP sem match nas camadas 1 e 2.
    colunas_sftp : dict[str, set[str]]
        Índice de colunas SFTP.
    colunas_dh : dict[str, set[str]]
        Índice de colunas dump.
    limiar : float
        Limiar mínimo de Jaccard (default 0.5).

    Returns
    -------
    pd.DataFrame
        Colunas: ``tabela_sftp, tabela_dump, metodo, confianca, score_similaridade``.
    """
    dh_tabelas = sorted(colunas_dh.keys())
    pares: list[dict] = []
    comparacoes = 0

    for t_sftp in sorted(tabelas_sftp_sem_match):
        cols_s = colunas_sftp.get(t_sftp, set())
        if not cols_s:
            continue
        best_score = 0.0
        best_match: str | None = None
        for t_dh in dh_tabelas:
            cols_d = colunas_dh.get(t_dh, set())
            j = jaccard_colunas(cols_s, cols_d)
            comparacoes += 1
            if j > best_score:
                best_score = j
                best_match = t_dh

        if best_match and best_score >= limiar:
            pares.append(
                {
                    "tabela_sftp": t_sftp,
                    "tabela_dump": best_match,
                    "metodo": "jaccard_colunas",
                    "confianca": "baixa",
                    "score_similaridade": round(best_score, 4),
                }
            )

    logger.info(
        "Camada 3 (Jaccard): %d pares registrados (%d comparações, limiar=%.1f)",
        len(pares),
        comparacoes,
        limiar,
    )
    return pd.DataFrame(
        pares,
        columns=[
            "tabela_sftp",
            "tabela_dump",
            "metodo",
            "confianca",
            "score_similaridade",
        ],
    )


# ---------------------------------------------------------------------------
# Identificação de campos em comum e chaves
# ---------------------------------------------------------------------------


def campos_em_comum_por_par(
    tabela_sftp: str,
    tabela_dump: str,
    colunas_sftp: dict[str, set[str]],
    colunas_dh: dict[str, set[str]],
    tipos_sftp: dict[str, dict[str, str]],
    tipos_dh: dict[str, dict[str, str]],
) -> pd.DataFrame:
    """Calcula os campos em comum para um par de tabelas.

    Compara nomes normalizados (interseção) e classifica o match:
    - ``exato``: mesmo nome original (case-insensitive)
    - ``normalizado``: nomes diferentes mas mesmo nome normalizado

    Parameters
    ----------
    tabela_sftp, tabela_dump : str
        Nomes das tabelas.
    colunas_sftp, colunas_dh : dict
        Índices de colunas normalizadas.
    tipos_sftp, tipos_dh : dict
        Índices de tipos originais.

    Returns
    -------
    pd.DataFrame
        Colunas: ``tabela_sftp, tabela_dump, campo, tipo_sftp, tipo_dump, match_tipo``.
    """
    cs = colunas_sftp.get(tabela_sftp, set())
    cd = colunas_dh.get(tabela_dump, set())
    ts = tipos_sftp.get(tabela_sftp, {})
    td = tipos_dh.get(tabela_dump, {})

    # Construir mapeamento reverso: nome normalizado → nome original
    rev_sftp: dict[str, str] = {}
    for orig in ts:
        rev_sftp[normalizar_nome_coluna(orig)] = orig
    rev_dh: dict[str, str] = {}
    for orig in td:
        rev_dh[normalizar_nome_coluna(orig)] = orig

    linhas: list[dict] = []
    for col_norm in sorted(cs & cd):
        orig_s = rev_sftp.get(col_norm, col_norm)
        orig_d = rev_dh.get(col_norm, col_norm)
        match_tipo = "exato" if orig_s.lower() == orig_d.lower() else "normalizado"
        linhas.append(
            {
                "tabela_sftp": tabela_sftp,
                "tabela_dump": tabela_dump,
                "campo": col_norm,
                "tipo_sftp": ts.get(orig_s, ""),
                "tipo_dump": td.get(orig_d, ""),
                "match_tipo": match_tipo,
            }
        )

    return pd.DataFrame(
        linhas,
        columns=[
            "tabela_sftp",
            "tabela_dump",
            "campo",
            "tipo_sftp",
            "tipo_dump",
            "match_tipo",
        ],
    )


def identificar_chaves_candidatas(df_campos_em_comum: pd.DataFrame) -> pd.DataFrame:
    """Filtra campos em comum que batem com CHAVE_PATTERNS.

    Parameters
    ----------
    df_campos_em_comum : pd.DataFrame
        DataFrame de campos em comum (todos os pares).

    Returns
    -------
    pd.DataFrame
        Colunas: ``chave, padrao, qtd_tabelas_sftp, qtd_tabelas_dump, tabelas_exemplo``.
    """
    if df_campos_em_comum.empty:
        return pd.DataFrame(
            columns=[
                "chave",
                "padrao",
                "qtd_tabelas_sftp",
                "qtd_tabelas_dump",
                "tabelas_exemplo",
            ]
        )

    chaves: list[dict] = []
    import re as _re

    for pattern_str, label in CHAVE_PATTERNS:
        pat = _re.compile(pattern_str)
        matches = df_campos_em_comum[
            df_campos_em_comum["campo"].str.contains(pat, na=False)
        ]
        if not matches.empty:
            tabs_sftp = matches["tabela_sftp"].nunique()
            tabs_dh = matches["tabela_dump"].nunique()
            exemplos = matches["tabela_sftp"].drop_duplicates().head(3).tolist()
            chaves.append(
                {
                    "chave": label,
                    "padrao": pattern_str,
                    "qtd_tabelas_sftp": tabs_sftp,
                    "qtd_tabelas_dump": tabs_dh,
                    "tabelas_exemplo": "; ".join(exemplos),
                }
            )

    return pd.DataFrame(
        chaves,
        columns=[
            "chave",
            "padrao",
            "qtd_tabelas_sftp",
            "qtd_tabelas_dump",
            "tabelas_exemplo",
        ],
    )


# ---------------------------------------------------------------------------
# Identificação de divergências
# ---------------------------------------------------------------------------


def identificar_divergencias(
    tabelas_sftp: set[str],
    tabelas_dh: set[str],
    df_relacionadas: pd.DataFrame,
) -> pd.DataFrame:
    """Identifica divergências estruturais: tabelas exclusivas e distribuição temporal.

    Parameters
    ----------
    tabelas_sftp : set[str]
        Todas as tabelas do SFTP.
    tabelas_dh : set[str]
        Todas as tabelas do dump.
    df_relacionadas : pd.DataFrame
        DataFrame de tabelas relacionadas.

    Returns
    -------
    pd.DataFrame
        Colunas: ``categoria, tabela, schema, familia, qtd_tabelas, observacao``.
    """
    matched_sftp: set[str] = (
        set(df_relacionadas["tabela_sftp"].unique())
        if not df_relacionadas.empty
        else set()
    )
    matched_dh: set[str] = (
        set(df_relacionadas["tabela_dump"].unique())
        if not df_relacionadas.empty
        else set()
    )

    so_sftp = tabelas_sftp - matched_sftp
    so_dh = tabelas_dh - matched_dh

    linhas: list[dict] = []

    # Agrupar tabelas só SFTP por família
    if so_sftp:
        familia = _agrupar_familia(so_sftp)
        for t in sorted(so_sftp)[:20]:  # amostra de 20 para não inflar
            anos = extrair_anos(t)
            linhas.append(
                {
                    "categoria": "so_sftp",
                    "tabela": t,
                    "schema": "sftp",
                    "familia": familia,
                    "qtd_tabelas": len(so_sftp),
                    "observacao": f"Anos: {sorted(anos) if anos else 'N/A'}",
                }
            )

    # Agrupar tabelas só dump por família
    if so_dh:
        familia = _agrupar_familia(so_dh)
        for t in sorted(so_dh)[:20]:
            anos = extrair_anos(t)
            linhas.append(
                {
                    "categoria": "so_dump",
                    "tabela": t,
                    "schema": "dados_historicos",
                    "familia": familia,
                    "qtd_tabelas": len(so_dh),
                    "observacao": f"Anos: {sorted(anos) if anos else 'N/A'}",
                }
            )

    if linhas:
        logger.info("Divergências: %d só SFTP, %d só dump", len(so_sftp), len(so_dh))
    else:
        logger.info("Divergências: nenhuma tabela exclusiva encontrada")

    return pd.DataFrame(
        linhas,
        columns=[
            "categoria",
            "tabela",
            "schema",
            "familia",
            "qtd_tabelas",
            "observacao",
        ],
    )


# ---------------------------------------------------------------------------
# Orquestrador principal
# ---------------------------------------------------------------------------


def executar_batimento(
    inv_sftp: pd.DataFrame,
    inv_dh: pd.DataFrame,
    df_estrutura_sftp: pd.DataFrame,
    df_estrutura_dh: pd.DataFrame,
    df_comparacao: pd.DataFrame,
    schema_dump: str = "dados_historicos",
) -> BatimentoResult:
    """Executa o batimento estrutural completo (3 camadas).

    Fluxo
    -----
    1. Constrói índices de colunas e hashes.
    2. Camada 1: hash exato.
    3. Camada 2: stem canônico (para tabelas sem match na camada 1).
    4. Camada 3: Jaccard de colunas (para tabelas sem match nas camadas 1 e 2).
    5. Calcula campos em comum, chaves candidatas e divergências.

    Parameters
    ----------
    inv_sftp : pd.DataFrame
        Inventário de colunas do SFTP.
    inv_dh : pd.DataFrame
        Inventário de colunas do dump.
    df_estrutura_sftp : pd.DataFrame
        Estrutura (metadados) das tabelas SFTP.
    df_estrutura_dh : pd.DataFrame
        Estrutura (metadados) das tabelas dump.
    df_comparacao : pd.DataFrame
        Comparação de hashes entre schemas.
    schema_dump : str
        Schema alvo do dump (``"dados_historicos"`` ou
        ``"dados_historicos_formatados"``). Usado para referência em
        logs e futura passagem para carga de artefatos.

    Returns
    -------
    BatimentoResult
        Named tuple com 4 DataFrames.
    """
    # Construir índices
    colunas_sftp = _build_col_index(inv_sftp)
    colunas_dh = _build_col_index(inv_dh)
    tipos_sftp = _build_col_types(inv_sftp)
    tipos_dh = _build_col_types(inv_dh)
    index_hash = _index_from_comparacao(df_comparacao)

    tabelas_sftp: set[str] = set(colunas_sftp.keys())
    tabelas_dh: set[str] = set(colunas_dh.keys())

    logger.info(
        "Batimento: %d tabelas SFTP, %d tabelas dump",
        len(tabelas_sftp),
        len(tabelas_dh),
    )

    # --- Camada 1: Hash exato ---
    df_c1 = camada1_hash_exato(tabelas_sftp, tabelas_dh, index_hash)
    matched_sftp_c1: set[str] = (
        set(df_c1["tabela_sftp"].unique()) if not df_c1.empty else set()
    )

    # --- Camada 2: Stem canônico ---
    sem_match_c1 = tabelas_sftp - matched_sftp_c1
    df_c2 = camada2_stem_canonico(sem_match_c1, tabelas_dh, colunas_sftp, colunas_dh)
    matched_sftp_c2: set[str] = (
        set(df_c2["tabela_sftp"].unique()) if not df_c2.empty else set()
    )

    # --- Camada 3: Jaccard de colunas ---
    sem_match_c2 = tabelas_sftp - matched_sftp_c1 - matched_sftp_c2
    df_c3 = camada3_jaccard_colunas(sem_match_c2, colunas_sftp, colunas_dh, limiar=0.5)

    # --- Consolidar relacionadas ---
    frames_rel: list[pd.DataFrame] = []
    if not df_c1.empty:
        frames_rel.append(
            df_c1[
                [
                    "tabela_sftp",
                    "tabela_dump",
                    "metodo",
                    "confianca",
                    "score_similaridade",
                ]
            ]
        )
    if not df_c2.empty:
        frames_rel.append(
            df_c2[
                [
                    "tabela_sftp",
                    "tabela_dump",
                    "metodo",
                    "confianca",
                    "score_similaridade",
                ]
            ]
        )
    if not df_c3.empty:
        frames_rel.append(
            df_c3[
                [
                    "tabela_sftp",
                    "tabela_dump",
                    "metodo",
                    "confianca",
                    "score_similaridade",
                ]
            ]
        )

    df_relacionadas = (
        pd.concat(frames_rel, ignore_index=True)
        if frames_rel
        else pd.DataFrame(
            columns=[
                "tabela_sftp",
                "tabela_dump",
                "metodo",
                "confianca",
                "score_similaridade",
            ]
        )
    )

    # Adicionar qtd_campos_comum
    if not df_relacionadas.empty:
        qtd_campos: list[int] = []
        for _, row in df_relacionadas.iterrows():
            cs = colunas_sftp.get(row["tabela_sftp"], set())
            cd = colunas_dh.get(row["tabela_dump"], set())
            qtd_campos.append(len(cs & cd))
        df_relacionadas["qtd_campos_comum"] = qtd_campos

    # --- Campos em comum (todos os pares) ---
    frames_campos: list[pd.DataFrame] = []
    if not df_relacionadas.empty:
        for _, row in df_relacionadas.iterrows():
            df_cc = campos_em_comum_por_par(
                row["tabela_sftp"],
                row["tabela_dump"],
                colunas_sftp,
                colunas_dh,
                tipos_sftp,
                tipos_dh,
            )
            if not df_cc.empty:
                frames_campos.append(df_cc)

    df_campos = (
        pd.concat(frames_campos, ignore_index=True)
        if frames_campos
        else pd.DataFrame(
            columns=[
                "tabela_sftp",
                "tabela_dump",
                "campo",
                "tipo_sftp",
                "tipo_dump",
                "match_tipo",
            ]
        )
    )

    # --- Chaves candidatas ---
    df_chaves = identificar_chaves_candidatas(df_campos)

    # --- Divergências ---
    df_div = identificar_divergencias(tabelas_sftp, tabelas_dh, df_relacionadas)

    logger.info(
        "Batimento concluído: %d pares (c1=%d, c2=%d, c3=%d), %d campos em comum, %d chaves",
        len(df_relacionadas),
        len(df_c1) if not df_c1.empty else 0,
        len(df_c2) if not df_c2.empty else 0,
        len(df_c3) if not df_c3.empty else 0,
        len(df_campos),
        len(df_chaves),
    )

    return BatimentoResult(
        relacionadas=df_relacionadas,
        campos_em_comum=df_campos,
        chaves=df_chaves,
        divergencias=df_div,
    )


def _index_from_comparacao(df_comparacao: pd.DataFrame) -> dict[str, set[str]]:
    """Constrói índice hash a partir do DataFrame de comparação."""
    from .leitura_artefatos import indexar_por_hash

    return indexar_por_hash(df_comparacao)
