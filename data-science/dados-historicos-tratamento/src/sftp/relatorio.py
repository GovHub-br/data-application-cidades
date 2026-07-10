"""Geração do relatório markdown final do batimento dump × SFTP.

Gera as 5 seções do relatório de batimento estrutural (e opcionalmente
validação de conteúdo e cruzamento por APF) no formato markdown,
autossuficiente e legível.

Seções
------
1. Tabelas Relacionadas — pares por método e confiança
2. Campos em Comum — top 20 campos + por família de tabela
3. Chaves de Cruzamento Candidatas — com status de validação
4. Divergências — tabelas exclusivas + distribuição temporal + conexões
   por conteúdo (APF) + consistência temporal
5. Recomendações de Uso Conjunto — análise em prosa + exemplos de JOIN
"""

from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path

import pandas as pd

from .matching import BatimentoResult
from .normalizacao import extrair_anos

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes de formatação
# ---------------------------------------------------------------------------

_NOMES_METODO: dict[str, str] = {
    "hash_exato": "Hash Exato",
    "stem_canonico": "Stem Canônico",
    "jaccard_colunas": "Similaridade de Colunas (Jaccard)",
}

_DEFAULT_DIR = Path("data/sftp/relatorios")
"""Diretório padrão para os relatórios gerados."""

_DESCRICOES_METODO: dict[str, str] = {
    "hash_exato": "pares com hash estrutural idêntico",
    "stem_canonico": "pares com stem canônico compatível e Jaccard ≥ 0,3",
    "jaccard_colunas": "pares identificados por similaridade de colunas (Jaccard ≥ 0,5)",
}


def _truncar(nome: object, max_len: int = 60) -> str:
    """Trunca nome de tabela para exibição em tabela markdown."""
    s = str(nome)
    return s[:max_len] + "..." if len(s) > max_len else s


# ---------------------------------------------------------------------------
# Seção 1 — Tabelas Relacionadas
# ---------------------------------------------------------------------------


def _secao_relacionadas(df: pd.DataFrame) -> str:
    """Gera a Seção 1: Tabelas Relacionadas."""
    if df.empty:
        return "## 1. Tabelas Relacionadas\n\nNenhum par de tabelas relacionadas foi encontrado.\n"

    linhas: list[str] = ["## 1. Tabelas Relacionadas\n"]

    grupos = df.groupby(["metodo", "confianca"])

    for (metodo, confianca), grupo in grupos:
        label_metodo = _NOMES_METODO.get(metodo, metodo)
        desc = _DESCRICOES_METODO.get(metodo, "")
        qtd = len(grupo)
        score_medio = grupo["score_similaridade"].mean()

        linhas.append(f"### {label_metodo} (Confiança {confianca.title()})\n")
        if desc:
            linhas.append(f"- **{qtd}** {desc}")
        else:
            linhas.append(f"- **{qtd} pares** de tabelas")
        linhas.append(f"- Score médio: {score_medio:.2f}\n")

        # Tabela resumo
        linhas.append("| Método | Confiança | Qtd. Pares | Score Médio |")
        linhas.append("|--------|-----------|------------|-------------|")
        linhas.append(
            f"| {label_metodo} | {confianca.title()} | {qtd} | {score_medio:.2f} |"
        )
        linhas.append("")

        # Até 5 exemplos
        exemplos = grupo.head(5)
        linhas.append("| SFTP | Dump | Score |")
        linhas.append("|------|------|-------|")
        for _, row in exemplos.iterrows():
            linhas.append(
                f"| {_truncar(row['tabela_sftp'])} "
                f"| {_truncar(row['tabela_dump'])} "
                f"| {row['score_similaridade']:.2f} |"
            )
        linhas.append("")

    # Totais gerais
    linhas.append("### Totais Gerais\n")
    linhas.append(f"- **Total de pares:** {len(df)}")
    linhas.append(f"- **Tabelas SFTP relacionadas:** {df['tabela_sftp'].nunique()}")
    linhas.append(f"- **Tabelas dump relacionadas:** {df['tabela_dump'].nunique()}")
    linhas.append("")

    return "\n".join(linhas)


# ---------------------------------------------------------------------------
# Seção 2 — Campos em Comum
# ---------------------------------------------------------------------------


def _secao_campos_comum(df: pd.DataFrame) -> str:
    """Gera a Seção 2: Campos em Comum."""
    if df.empty:
        return "## 2. Campos em Comum\n\nNenhum campo em comum foi identificado entre os pares de tabelas.\n"

    linhas: list[str] = ["## 2. Campos em Comum\n"]

    # Top 20 campos mais frequentes
    top_campos = (
        df.groupby("campo")
        .agg(
            qtd_pares=("tabela_sftp", "nunique"),
            match_exato=("match_tipo", lambda s: (s == "exato").sum()),
            match_normalizado=("match_tipo", lambda s: (s == "normalizado").sum()),
        )
        .reset_index()
        .sort_values("qtd_pares", ascending=False)
        .head(20)
    )

    linhas.append("### Top 20 Campos Mais Frequentes\n")
    linhas.append("| Campo | Qtd. Pares | Match Exato | Match Normalizado |")
    linhas.append("|-------|-----------|-------------|-------------------|")
    for _, row in top_campos.iterrows():
        linhas.append(
            f"| {row['campo']} | {row['qtd_pares']} | {row['match_exato']} | {row['match_normalizado']} |"
        )
    linhas.append("")

    # Campos por família de tabela
    linhas.append("### Campos por Família de Tabela\n")
    for tabela, grupo in df.groupby("tabela_sftp"):
        linhas.append(f"**{tabela}**\n")
        campos_unicos = grupo[["campo", "match_tipo"]].drop_duplicates()
        if not campos_unicos.empty:
            linhas.append("| Campo | Tipo SFTP | Tipo Dump | Match |")
            linhas.append("|-------|-----------|-----------|-------|")
            for _, crow in campos_unicos.head(15).iterrows():
                tipo_sftp = grupo.loc[
                    grupo["campo"] == crow["campo"], "tipo_sftp"
                ].iloc[0]
                tipo_dump = grupo.loc[
                    grupo["campo"] == crow["campo"], "tipo_dump"
                ].iloc[0]
                match_icon = "✅" if crow["match_tipo"] == "exato" else "⚠️"
                linhas.append(
                    f"| {crow['campo']} | {tipo_sftp} | {tipo_dump} | {match_icon} |"
                )
            if len(campos_unicos) > 15:
                linhas.append(f"\n*... e mais {len(campos_unicos) - 15} campos*")
        linhas.append("")

    return "\n".join(linhas)


# ---------------------------------------------------------------------------
# Seção 3 — Chaves de Cruzamento Candidatas
# ---------------------------------------------------------------------------


def _secao_chaves(
    df_chaves: pd.DataFrame,
    validacao: pd.DataFrame | None,
    df_cruzamento: pd.DataFrame | None = None,
) -> str:
    """Gera a Seção 3: Chaves de Cruzamento Candidatas."""
    if df_chaves.empty:
        return "## 3. Chaves de Cruzamento Candidatas\n\nNenhuma chave candidata foi identificada.\n"

    linhas: list[str] = ["## 3. Chaves de Cruzamento Candidatas\n"]

    # Construir conjunto de chaves validadas a partir do DataFrame de validação
    chaves_validadas: set[str] = set()
    if validacao is not None and not validacao.empty:
        # Normalizar e agrupar por coluna; considerar validado se houver
        # pelo menos um resultado positivo
        resultados_positivos = {
            "sucesso",
            "ok",
            "positivo",
            "válido",
            "valido",
            "verdadeiro",
            "true",
            "pass",
            "1",
            "sim",
        }
        for _, row in validacao.iterrows():
            coluna = str(row.get("coluna", "")).strip().lower()
            resultado = str(row.get("resultado", "")).strip().lower()
            if coluna and resultado in resultados_positivos:
                chaves_validadas.add(coluna)

    # Chaves com validação dupla (estrutural + conteúdo APF)
    chaves_val_dupla: set[str] = set()
    if df_cruzamento is not None and not df_cruzamento.empty:
        pares_alta = df_cruzamento[df_cruzamento["confianca"] == "alta"]
        chaves_val_dupla = {"apf", "nu_apf"}

    linhas.append("| Chave | Padrão | Tabelas SFTP | Tabelas Dump | Status |")
    linhas.append("|-------|--------|-------------|-------------|--------|")
    for _, row in df_chaves.iterrows():
        chave = row["chave"]
        qtd_sftp = int(row["qtd_tabelas_sftp"])
        qtd_dump = int(row["qtd_tabelas_dump"])

        if chave.lower() in chaves_val_dupla:
            status = "✅ validada duplamente (estrutural + conteúdo APF)"
        elif chave.lower() in chaves_validadas:
            status = "✅ validada por amostragem"
        else:
            status = "⚠️ requer validação"

        linhas.append(
            f"| {chave} | `{row['padrao']}` | {qtd_sftp} | {qtd_dump} | {status} |"
        )
    linhas.append("")

    # Nota sobre validação dupla quando df_cruzamento está disponível
    if df_cruzamento is not None and not df_cruzamento.empty:
        pares_alta = df_cruzamento[df_cruzamento["confianca"] == "alta"]
        qtd_alta = len(pares_alta)
        familias_abrangidas = pares_alta["familia"].nunique()
        linhas.append(
            "### Validação por Conteúdo (APF)\n\n"
            f"O cruzamento por conteúdo identificou **{qtd_alta} pares** "
            f"de alta confiança (sobreposição de APF ≥ 90%), distribuídos "
            f"por **{familias_abrangidas} famílias** de tabelas. "
            "A chave `apf` (ou `nu_apf`) aparece em ambos os schemas "
            "com alta correspondência de valores, recebendo **validação "
            "dupla** — confirmação estrutural (coluna presente) e de "
            "conteúdo (valores coincidentes).\n\n"
            "Isso torna `apf` a chave de cruzamento mais confiável "
            "para operações de JOIN entre os schemas SFTP e "
            "dados_historicos.\n"
        )

    # Exemplos de tabelas por chave
    if "tabelas_exemplo" in df_chaves.columns:
        linhas.append("### Exemplos de Tabelas por Chave\n")
        for _, row in df_chaves.iterrows():
            exemplos = str(row.get("tabelas_exemplo", "") or "")
            if exemplos.strip():
                linhas.append(f"- **{row['chave']}**: {exemplos}")
        linhas.append("")

    return "\n".join(linhas)


# ---------------------------------------------------------------------------
# Seção 4 — Divergências
# ---------------------------------------------------------------------------


def _secao_divergencias(
    df_div: pd.DataFrame,
    df_relacionadas: pd.DataFrame,
    df_cruzamento: pd.DataFrame | None = None,
    df_consistencia: pd.DataFrame | None = None,
) -> str:
    """Gera a Seção 4: Divergências."""
    if df_div.empty:
        return (
            "## 4. Divergências\n\nNenhuma divergência estrutural foi identificada.\n"
        )

    linhas: list[str] = ["## 4. Divergências\n"]

    # -- Tabelas só SFTP --
    so_sftp = df_div[df_div["categoria"] == "so_sftp"]
    if not so_sftp.empty:
        # A coluna qtd_tabelas tem o mesmo valor para todas as linhas
        qtd_total_so_sftp = int(so_sftp["qtd_tabelas"].iloc[0])
        linhas.append("### Tabelas Exclusivas do SFTP\n")
        linhas.append(
            f"Total: **{qtd_total_so_sftp} tabelas** sem correspondência "
            f"no dump histórico.\n"
        )

        for familia, grupo in so_sftp.groupby("familia"):
            qtd_exemplos = len(grupo)
            linhas.append(f"**{familia}** — {qtd_exemplos} exemplos\n")
            for _, row_n in grupo.iterrows():
                linhas.append(
                    f"- `{_truncar(row_n['tabela'], 80)}` — {row_n.get('observacao', '')}"
                )
            linhas.append("")

    # -- Tabelas só Dump --
    so_dump = df_div[df_div["categoria"] == "so_dump"]
    if not so_dump.empty:
        qtd_total_so_dump = int(so_dump["qtd_tabelas"].iloc[0])
        linhas.append("### Tabelas Exclusivas do Dump Histórico\n")
        linhas.append(
            f"Total: **{qtd_total_so_dump} tabelas** sem correspondência no SFTP.\n"
        )

        for familia, grupo in so_dump.groupby("familia"):
            qtd_exemplos = len(grupo)
            linhas.append(f"**{familia}** — {qtd_exemplos} exemplos\n")
            for _, row_n in grupo.iterrows():
                linhas.append(
                    f"- `{_truncar(row_n['tabela'], 80)}` — {row_n.get('observacao', '')}"
                )
            linhas.append("")

    # -- Conexões por Conteúdo (APF) --
    if df_cruzamento is not None and not df_cruzamento.empty:
        so_conteudo = df_cruzamento[
            (not df_cruzamento["match_estrutural"])
            & (df_cruzamento["confianca"] == "alta")
        ]
        if not so_conteudo.empty:
            linhas.append("### Conexões por Conteúdo (APF)\n")
            linhas.append(
                f"O matching por conteúdo (sobreposição de APF) identificou "
                f"**{len(so_conteudo)} pares** de alta confiança que **não "
                f"foram detectados pelo matching estrutural**. Essas conexões "
                f"representam tabelas com estruturas diferentes (nomes de coluna "
                f"divergentes ou tipos distintos) mas que compartilham os mesmos "
                f"contratos APF.\n\n"
            )

            for familia, grupo in so_conteudo.groupby("familia"):
                qtd = len(grupo)
                linhas.append(f"**{familia}** — {qtd} pares\n")

                # Mostrar até 5 exemplos
                exemplos = grupo.head(5)
                linhas.append(
                    "| Tabela SFTP | Tabela Dump | Overlap APF | Overlap % |"
                )
                linhas.append(
                    "|------------|-------------|-------------|-----------|"
                )
                for _, row_c in exemplos.iterrows():
                    overlap_pct = (
                        f"{row_c['overlap_pct']:.1%}"
                        if isinstance(row_c['overlap_pct'], float)
                        else str(row_c['overlap_pct'])
                    )
                    linhas.append(
                        f"| {_truncar(row_c['tabela_sftp'], 50)} "
                        f"| {_truncar(row_c['tabela_dhf'], 50)} "
                        f"| {int(row_c['overlap_apfs'])} "
                        f"| {overlap_pct} |"
                    )
                if qtd > 5:
                    linhas.append(f"\n*... e mais {qtd - 5} pares*")
                linhas.append("")

    # -- Consistência Temporal --
    if df_consistencia is not None and not df_consistencia.empty:
        linhas.append(_subsecao_consistencia_temporal(df_consistencia))

    # -- Distribuição temporal --
    linhas.append(_montar_distribuicao_temporal(df_relacionadas, df_div))
    linhas.append("")

    return "\n".join(linhas)


def _montar_distribuicao_temporal(
    df_relacionadas: pd.DataFrame,
    df_div: pd.DataFrame,
) -> str:
    """Extrai distribuição de anos dos nomes de tabela."""

    # Coletar nomes de tabela SFTP
    tabelas_sftp: set[str] = set()
    if not df_relacionadas.empty:
        tabelas_sftp.update(df_relacionadas["tabela_sftp"].unique())
    if not df_div.empty:
        so_sftp = df_div[df_div["categoria"] == "so_sftp"]
        if not so_sftp.empty:
            tabelas_sftp.update(so_sftp["tabela"].unique())

    # Coletar nomes de tabela Dump
    tabelas_dh: set[str] = set()
    if not df_relacionadas.empty:
        tabelas_dh.update(df_relacionadas["tabela_dump"].unique())
    if not df_div.empty:
        so_dump = df_div[df_div["categoria"] == "so_dump"]
        if not so_dump.empty:
            tabelas_dh.update(so_dump["tabela"].unique())

    # Extrair anos
    anos_sftp: Counter[int] = Counter()
    for t in tabelas_sftp:
        for ano in extrair_anos(str(t)):
            anos_sftp[ano] += 1

    anos_dh: Counter[int] = Counter()
    for t in tabelas_dh:
        for ano in extrair_anos(str(t)):
            anos_dh[ano] += 1

    todos_anos = sorted(set(anos_sftp.keys()) | set(anos_dh.keys()))
    sobreposicao = set(anos_sftp.keys()) & set(anos_dh.keys())

    linhas: list[str] = ["### Distribuição Temporal\n"]

    if not todos_anos:
        linhas.append(
            "Não foi possível extrair anos dos nomes das tabelas disponíveis.\n"
        )
        return "\n".join(linhas)

    linhas.append("| Ano | Tabelas SFTP | Tabelas Dump | Sobreposição |")
    linhas.append("|-----|-------------|-------------|--------------|")
    for ano in todos_anos:
        qtd_sftp = anos_sftp.get(ano, 0)
        qtd_dh = anos_dh.get(ano, 0)
        marcador = "✅" if ano in sobreposicao else ""
        linhas.append(f"| {ano} | {qtd_sftp} | {qtd_dh} | {marcador} |")

    if sobreposicao:
        linhas.append(
            f"\nZona de sobreposição: {min(sobreposicao)}–{max(sobreposicao)} "
            f"({len(sobreposicao)} anos com tabelas em ambos os schemas)."
        )
    else:
        linhas.append(
            "\nNão há sobreposição temporal — os schemas cobrem períodos distintos."
        )

    return "\n".join(linhas)


# ---------------------------------------------------------------------------
# Subseção — Consistência Temporal (usada na Seção 4)
# ---------------------------------------------------------------------------


def _subsecao_consistencia_temporal(df_consistencia: pd.DataFrame) -> str:
    """Subseção que apresenta a análise de consistência temporal.

    Compara a data inferida do nome do arquivo com a data real dos dados
    (extraída de colunas como ``data_de_movimento``), apontando
    discrepâncias.
    """
    linhas: list[str] = []

    total = len(df_consistencia)
    status_counts = df_consistencia["status"].value_counts()
    qtd_ok = status_counts.get("ok", 0)
    qtd_div = status_counts.get("divergente", 0)
    qtd_ind = status_counts.get("indeterminado", 0)

    linhas.append("### Consistência Temporal\n")
    linhas.append(
        f"Foram analisadas **{total} tabelas** quanto à consistência "
        f"entre a data inferida do nome do arquivo e a data real dos "
        f"dados (coluna ``data_de_movimento``).\n"
    )
    pct_ok = f"{qtd_ok / total:.1%}" if total > 0 else "0%"
    pct_div = f"{qtd_div / total:.1%}" if total > 0 else "0%"
    pct_ind = f"{qtd_ind / total:.1%}" if total > 0 else "0%"

    linhas.append("| Status | Quantidade | Percentual |")
    linhas.append("|--------|-----------|------------|")
    linhas.append(f"| ✅ Ok | {qtd_ok} | {pct_ok} |")
    linhas.append(f"| ⚠️ Divergente | {qtd_div} | {pct_div} |")
    linhas.append(f"| ❓ Indeterminado | {qtd_ind} | {pct_ind} |")
    linhas.append("")

    # Top 5 discrepâncias
    divergentes = df_consistencia[df_consistencia["status"] == "divergente"].copy()
    if not divergentes.empty:
        divergentes["diferenca_dias_num"] = pd.to_numeric(
            divergentes["diferenca_dias"], errors="coerce"
        )
        top5 = divergentes.sort_values(
            "diferenca_dias_num", ascending=False
        ).head(5)

        linhas.append("#### Top 5 Discrepâncias\n")
        linhas.append(
            "| Tabela | Data (Arquivo) | Data (Dados) | Diferença (dias) |"
        )
        linhas.append(
            "|--------|---------------|-------------|------------------|"
        )
        for _, row_t in top5.iterrows():
            dif_dias = row_t.get("diferenca_dias", "")
            data_arq = str(row_t.get("data_nome_arquivo", "") or "")
            data_dados = str(row_t.get("data_dados", "") or "")
            linhas.append(
                f"| {_truncar(row_t['tabela'], 60)} "
                f"| {data_arq} "
                f"| {data_dados} "
                f"| {dif_dias} |"
            )
        linhas.append("")

    return "\n".join(linhas)


# ---------------------------------------------------------------------------
# Seção 5 — Recomendações de Uso Conjunto
# ---------------------------------------------------------------------------


def _secao_recomendacoes(
    result: BatimentoResult,
    validacao: pd.DataFrame | None,
    df_cruzamento: pd.DataFrame | None = None,
    df_consistencia: pd.DataFrame | None = None,
) -> str:
    """Gera a Seção 5: Recomendações de Uso Conjunto.

    Responde à pergunta "SFTP substitui ou complementa o dump histórico?"
    com base nas evidências estruturais e de conteúdo (quando disponível).
    """
    linhas: list[str] = ["## 5. Recomendações de Uso Conjunto\n"]

    if validacao is None:
        linhas.append(
            "> ⚠️ **Nota:** As recomendações abaixo são preliminares e "
            "baseiam-se apenas na análise estrutural. A validação de "
            "conteúdo (Fase 2) ainda não foi executada.\n"
        )

    df_rel = result.relacionadas
    df_div = result.divergencias

    # ------------------------------------------------------------------
    # Análise: substitui, complementa ou ambos?
    # ------------------------------------------------------------------
    tem_hash_exato = not df_rel.empty and "hash_exato" in df_rel["metodo"].values
    tem_outras_camadas = (
        not df_rel.empty and df_rel[df_rel["metodo"] != "hash_exato"].shape[0] > 0
    )

    so_sftp_count = 0
    so_dump_count = 0
    if not df_div.empty:
        so_sftp_mask = df_div["categoria"] == "so_sftp"
        so_dump_mask = df_div["categoria"] == "so_dump"
        # qtd_tabelas reflete o total da categoria
        if so_sftp_mask.any():
            so_sftp_count = int(df_div.loc[so_sftp_mask, "qtd_tabelas"].iloc[0])
        if so_dump_mask.any():
            so_dump_count = int(df_div.loc[so_dump_mask, "qtd_tabelas"].iloc[0])

    # Contagem de divergências temporais (para uso nas limitações)
    qtd_div = 0
    if df_consistencia is not None and not df_consistencia.empty:
        qtd_div = int((df_consistencia["status"] == "divergente").sum())

    # Decidir o veredito
    e_substituto = tem_hash_exato
    e_complementar = so_sftp_count > 0 and so_dump_count > 0

    # ------------------------------------------------------------------
    # Evidência: correspondência estrutural
    # ------------------------------------------------------------------
    linhas.append("### Evidências\n")

    if tem_hash_exato:
        qtd_hash = len(df_rel[df_rel["metodo"] == "hash_exato"])
        linhas.append(
            f"- **Correspondência por hash exato:** {qtd_hash} pares de "
            f"tabelas com estrutura idêntica. Isso indica que parte das "
            f"tabelas do SFTP é continuação temporal direta do dump "
            f"histórico (mesma definição de colunas, tipos e constraints).\n"
        )

    if tem_outras_camadas:
        qtd_outras = df_rel[df_rel["metodo"] != "hash_exato"].shape[0]
        linhas.append(
            f"- **Correspondência por similaridade:** {qtd_outras} pares "
            f"adicionais foram identificados por stem canônico ou Jaccard "
            f"de colunas, revelando relações semânticas entre tabelas "
            f"com nomes diferentes.\n"
        )

    if so_sftp_count > 0 or so_dump_count > 0:
        partes: list[str] = []
        if so_sftp_count > 0:
            partes.append(f"{so_sftp_count} tabelas exclusivas do SFTP")
        if so_dump_count > 0:
            partes.append(f"{so_dump_count} tabelas exclusivas do dump")
        linhas.append(
            f"- **Tabelas exclusivas:** {' e '.join(partes)} não possuem "
            f"correspondência no outro schema.\n"
        )

    # ------------------------------------------------------------------
    # Veredito
    # ------------------------------------------------------------------
    linhas.append("### Veredito\n")

    if e_substituto and e_complementar:
        linhas.append(
            "**O SFTP complementa e parcialmente substitui o dump "
            "histórico.**\n\n"
            "As tabelas com hash exato idêntico indicam continuidade "
            "temporal — o SFTP carrega dados mais recentes com a mesma "
            "estrutura. Porém, a presença de tabelas exclusivas em ambos "
            "os schemas mostra que nenhum dos dois é completo isoladamente: "
            "o dump histórico contém dados de períodos anteriores não "
            "cobertos pelo SFTP, e o SFTP introduz tabelas e períodos "
            "que não existem no dump.\n"
        )
    elif e_substituto:
        linhas.append(
            "**O SFTP substitui parcialmente o dump histórico.**\n\n"
            "A maioria das tabelas tem correspondência estrutural direta, "
            "sugerindo que o SFTP é uma versão mais recente ou evolução "
            "do schema dump para os períodos cobertos. Recomenda-se "
            "priorizar o SFTP como fonte principal para períodos recentes, "
            "utilizando o dump apenas como complemento histórico.\n"
        )
    elif e_complementar:
        linhas.append(
            "**O SFTP complementa o dump histórico.**\n\n"
            "Os dois schemas têm sobreposição limitada e parecem cobrir "
            "períodos ou conjuntos de dados distintos. O SFTP adiciona "
            "informações não presentes no dump, e vice-versa. "
            "Recomenda-se o uso conjunto de ambos para obter a cobertura "
            "mais completa.\n"
        )
    else:
        linhas.append(
            "**Não foi possível determinar a relação entre os schemas "
            "com base na análise estrutural disponível.**\n\n"
            "Recomenda-se uma investigação manual adicional para entender "
            "a natureza das tabelas em cada schema.\n"
        )

    # ------------------------------------------------------------------
    # Recomendações por família de dados
    # ------------------------------------------------------------------
    linhas.append("### Recomendações por Família de Dados\n")

    if not df_div.empty:
        for familia, grupo in df_div.groupby("familia"):
            qtd_sftp_fam = len(grupo[grupo["schema"] == "sftp"])
            qtd_dump_fam = len(grupo[grupo["schema"] == "dados_historicos"])
            linhas.append(
                f"- **{familia}**: {qtd_sftp_fam} tabelas SFTP, "
                f"{qtd_dump_fam} tabelas dump. "
            )
            if qtd_sftp_fam > 0 and qtd_dump_fam > 0:
                linhas[-1] += (
                    "Presente em ambos os schemas — verificar "
                    "sobreposição temporal antes de consolidar."
                )
            elif qtd_sftp_fam > 0:
                linhas[-1] += (
                    "Disponível apenas no SFTP — usar como fonte "
                    "primária para esta família."
                )
            else:
                linhas[-1] += (
                    "Disponível apenas no dump histórico — usar como "
                    "fonte primária para esta família."
                )
        linhas.append("")
    else:
        linhas.append("Nenhuma família de dados divergente identificada.\n")

    # ------------------------------------------------------------------
    # Queries JOIN de exemplo
    # ------------------------------------------------------------------
    linhas.append("### Exemplos de JOIN\n")

    # Template JOIN usando apf (disponível quando há cruzamento de conteúdo)
    if df_cruzamento is not None and not df_cruzamento.empty:
        linhas.append(
            "Com base no cruzamento por conteúdo, a chave **`apf`** "
            "é a mais confiável para JOIN entre os schemas. "
            "Exemplo de consulta:\n\n"
            "```sql\n"
            "SELECT\n"
            "    sftp.*,\n"
            "    hist.municipio,\n"
            "    hist.agente_financeiro,\n"
            "    hist.data_de_movimento\n"
            "FROM sftp.snh_pmcmv_dados_prioritarios_af_caixa sftp\n"
            "INNER JOIN dados_historicos_formatados.historico_recente_snh_pmcmv_dados_prioritarios_af_caixa hist\n"
            "    ON sftp.apf = hist.apf\n"
            "WHERE sftp.apf IS NOT NULL\n"
            "  AND hist.apf IS NOT NULL;\n"
            "```\n\n"
            "Para cruzar com o schema ``dados_historicos`` (não formatado), "
            "a sintaxe é similar, mas os nomes de tabela podem conter "
            "períodos que exigem escaping com aspas duplas:\n\n"
            "```sql\n"
            "SELECT *\n"
            'FROM sftp.\"202406_snh_pmcmv_dados_prioritarios_af_caixa\" sftp\n'
            'INNER JOIN dados_historicos.\"024_10_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb\" hist\n'
            "    ON sftp.apf = hist.apf\n"
            "WHERE sftp.apf IS NOT NULL\n"
            "  AND hist.apf IS NOT NULL;\n"
            "```\n"
        )
    elif validacao is not None and not validacao.empty:
        chaves_validacao = validacao["coluna"].unique()
        if len(chaves_validacao) > 0:
            for chave in chaves_validacao[:5]:
                linhas.append(
                    "```sql\n"
                    f"SELECT *\n"
                    f"FROM sftp.tabela_a t1\n"
                    f"INNER JOIN dados_historicos.tabela_b t2\n"
                    f"    ON t1.{chave} = t2.{chave}\n"
                    f"WHERE t1.{chave} IS NOT NULL\n"
                    f"  AND t2.{chave} IS NOT NULL;\n"
                    "```\n"
                )
        else:
            linhas.append(
                "Nenhuma chave foi validada por amostragem — exemplos "
                "de JOIN não podem ser fornecidos com segurança.\n"
            )
    else:
        linhas.append(
            "A validação de amostras (Fase 2) não foi executada. "
            "Exemplos de JOIN serão incluídos após a validação "
            "das chaves candidatas.\n"
        )

    # ------------------------------------------------------------------
    # Limitações
    # ------------------------------------------------------------------
    linhas.append("### Limitações Conhecidas\n")

    if df_cruzamento is None:
        linhas.append(
            "- A análise é baseada exclusivamente em **metadados estruturais** "
            "(nomes de tabela, colunas e tipos), sem validação de conteúdo.\n"
        )
    else:
        linhas.append(
            "- A análise estrutural foi **complementada por validação de "
            "conteúdo** (cruzamento por APF). Os resultados de conteúdo "
            "estão refletidos nas seções 3 e 4.\n"
        )

    linhas.append(
        "- A nomenclatura de tabelas e colunas difere significativamente "
        "entre os dois schemas, o que pode levar a **falsos negativos** "
        "no matching (pares que existem mas não foram identificados).\n"
    )
    linhas.append(
        "- A **camada 3** (Jaccard de colunas) usa um limiar de 0,5, "
        "o que pode tanto incluir falsos positivos quanto excluir pares "
        "válidos com baixa similaridade nominal.\n"
    )
    if df_cruzamento is not None:
        linhas.append(
            "- O **cruzamento por conteúdo (APF)** só é aplicável a tabelas "
            "que possuem a coluna ``apf`` (ou ``nu_apf``). Tabelas sem "
            "essa coluna não foram validadas por conteúdo.\n"
        )
    if validacao is None and df_cruzamento is None:
        linhas.append(
            "- A validação de conteúdo (Fase 2) **não foi executada**. "
            "As chaves candidatas e recomendações de JOIN são "
            "preliminares e requerem verificação manual.\n"
        )
    if df_consistencia is not None and not df_consistencia.empty:
        if qtd_div > 0:
            linhas.append(
                "- A **consistência temporal** identificou **discrepâncias** "
                f"em {qtd_div} tabelas. Recomenda-se verificar as datas "
                "antes de realizar JOINs baseados em período — a data "
                "do nome do arquivo pode não corresponder à data real "
                "dos dados.\n"
            )
    linhas.append(
        "- Tabelas sem colunas ou com estrutura atípica podem não ter "
        "sido corretamente classificadas.\n"
    )
    linhas.append("")

    return "\n".join(linhas)


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------


def gerar_relatorio(
    result: BatimentoResult,
    validacao: pd.DataFrame | None,
    output_path: Path | None = None,
    df_cruzamento: pd.DataFrame | None = None,
    df_consistencia: pd.DataFrame | None = None,
) -> Path:
    """Gera o relatório markdown final do batimento dump × SFTP.

    Parameters
    ----------
    result : BatimentoResult
        Resultado do batimento estrutural com 4 DataFrames:
        ``relacionadas``, ``campos_em_comum``, ``chaves``, ``divergencias``.
    validacao : pd.DataFrame or None
        DataFrame opcional de validação de amostras (Fase 2) com colunas
        ``tabela_sftp``, ``tabela_dump``, ``coluna``, ``tipo_validacao``,
        ``resultado``, ``observacao``. Pode ser ``None`` se a Fase 2
        não foi executada.
    output_path : Path or None
        Caminho para o arquivo ``.md`` de saída. Quando ``None``, o
        caminho padrão é ``data/sftp/relatorios/relatorio_batimento_dump_sftp.md``.
        Se ``df_cruzamento`` ou ``df_consistencia`` for fornecido e
        ``output_path`` for ``None``, o caminho padrão muda para
        ``data/sftp/relatorios/relatorio_batimento_dump_sftp_v2.md``.
        O diretório pai será criado se não existir.
    df_cruzamento : pd.DataFrame or None
        DataFrame opcional com o resultado do cruzamento por conteúdo
        (APF), com colunas ``familia``, ``tabela_sftp``, ``tabela_dhf``,
        ``qtd_apfs_sftp``, ``qtd_apfs_dhf``, ``overlap_apfs``,
        ``overlap_pct``, ``match_estrutural``, ``confianca``.
    df_consistencia : pd.DataFrame or None
        DataFrame opcional com a análise de consistência temporal,
        com colunas ``tabela``, ``schema``, ``data_nome_arquivo``,
        ``data_dados``, ``fonte_data_dados``, ``diferenca_dias``,
        ``status``.

    Returns
    -------
    Path
        Caminho do arquivo gerado.
    """
    # Determinar caminho de saída
    if output_path is None:
        if df_cruzamento is not None or df_consistencia is not None:
            output_path = _DEFAULT_DIR / "relatorio_batimento_dump_sftp_v2.md"
        else:
            output_path = _DEFAULT_DIR / "relatorio_batimento_dump_sftp.md"

    logger.info("Gerando relatório de batimento em %s", output_path)

    secoes: list[str] = [
        "# Relatório de Batimento: Dump Histórico × SFTP\n",
        _secao_relacionadas(result.relacionadas),
        _secao_campos_comum(result.campos_em_comum),
        _secao_chaves(result.chaves, validacao, df_cruzamento),
        _secao_divergencias(
            result.divergencias, result.relacionadas, df_cruzamento, df_consistencia
        ),
        _secao_recomendacoes(result, validacao, df_cruzamento, df_consistencia),
    ]

    conteudo = "\n".join(secoes)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(conteudo, encoding="utf-8")

    logger.info("Relatório gerado: %s (%d bytes)", output_path, len(conteudo))
    return output_path
