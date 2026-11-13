"""Aba **Rural** do painel "Perguntas do MCID" no Superset: definição + construção.

Mesmo desenho do `build_boletim.py`, do qual reaproveita a sessão autenticada:

  1. **O que a aba mostra** — `secoes()` devolve as seis seções (visão executiva,
     portarias, contratos/empreendimentos/EOs, execução, Curva S, qualidade), cada uma
     com seus textos e gráficos. Para incluir ou tirar um gráfico, mexe-se só aí.
  2. **Como isso vira dashboard** — datasets físicos sobre as ouros do Rural,
     charts (criados ou atualizados pelo nome), layout da aba e filtros nativos
     com escopo só na aba Rural.

Escopo do que o script altera no dashboard `mcid-perguntas`:
  - SÓ a aba cujo título é "Rural": as outras abas ficam exatamente como estão
    publicadas (o script lê o layout atual e troca apenas a subárvore da aba);
  - nos filtros nativos, SÓ os de id `NATIVE_FILTER-RURAL-*`; os demais são
    preservados;
  - nunca apaga chart nem dataset. Charts antigos da aba que não entram no
    novo layout continuam existindo no Superset, só deixam de aparecer na aba.

Dados (dbt, schema `ouro`):
  - `ouro_rural_painel_portaria`  — dataset principal: uma linha por APF × tipo de portaria
    (Seleção, Autorização de contratação, Trabalho Social), com vínculos, execução
    física/financeira e marcas de qualidade. O filtro "Tipo de portaria" é obrigatório e
    de seleção única, então cada APF aparece uma vez em qualquer gráfico;
  - `ouro_rural_curva_s`          — Curva S físico-financeira do Novo Rural por mês × portaria;
  - `ouro_rural_selecao_portarias` / `ouro_rural_portarias_selecao_resumo` — funil
    seleção → autorização → contrato, por proposta e por portaria;
  - `ouro_rural_painel_operacao`  — uma linha por APF (contrato = empreendimento);
  - `ouro_rural_painel_pendencia` — uma linha por registro × regra de qualidade;
  - `ouro_rural_selecao_portaria_162`, `ouro_rural_entidade_organizadora`,
    `ouro_rural_entidade_recontratacao`, `ouro_rural_obra_situacao_mensal`,
    `ouro_mcid_cobertura_perguntas` — já existentes.

Regras de agregação usadas nos gráficos (para não inflar nem distorcer):
  - contratos/empreendimentos = COUNT(DISTINCT apf); EOs = COUNT(DISTINCT eo_cnpj);
  - valores somados a partir da linha da APF (uma linha por APF, sem join 1:N);
  - % execução financeira = Σ desembolsado ÷ Σ contratado, só nas APFs com
    contratado > 0 (nunca média de percentuais);
  - % execução física = média ponderada por UH, só nas APFs com medição.

Atenção: o filtro "Tipo de portaria" é obrigatório e de seleção única (padrão: Seleção). Sem ele o
dataset APF × tipo de portaria contaria cada APF três vezes.

Uso, na raiz do repo (fora do .venv-om):

    poetry run python scripts/superset/build_aba_rural_mcid.py --dry-run
    poetry run python scripts/superset/build_aba_rural_mcid.py

Pré-requisito: as ouros acima materializadas pelo dbt.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from dataclasses import dataclass, field

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from build_boletim import Superset  # noqa: E402  (mesma sessão por formulário)

DATABASE_NAME = "Cidades"
SCHEMA = "ouro"
SLUG = "mcid-perguntas"
ABA = "Rural"
PREFIXO = "Rural ▸ "
FILTRO_PREFIXO = "NATIVE_FILTER-RURAL-"

OPERACAO = "ouro_rural_painel_portaria"   # APF × tipo de portaria (filtro único de portaria)
CURVA_S = "ouro_rural_curva_s"
CURVA_REF = "ouro_rural_curva_s_referencia"
CURVA_DESVIO = "ouro_rural_curva_s_desvio"
FUNIL = "ouro_rural_selecao_portarias"
RESUMO_PORTARIA = "ouro_rural_portarias_selecao_resumo"
PENDENCIA = "ouro_rural_painel_pendencia"
SELECAO = "ouro_rural_selecao_portaria_162"
EO = "ouro_rural_entidade_organizadora"
SITUACAO_MENSAL = "ouro_rural_obra_situacao_mensal"
COBERTURA = "ouro_mcid_cobertura_perguntas"

#: Charts que já existem no Superset e continuam na aba (pelo id), em vez de
#: serem recriados: tabela de EOs, recontratação, estágio mês a mês, avaliação
#: GEHIS e cobertura das perguntas do Rural.
REUSO = {
    "eo_tabela": 411,
    "recontratacao_tabela": 413,
    "estagio_mensal": 420,
    "gehis_mensal": 421,
    "cobertura": 422,
}

# --------------------------------------------------------------------------
# métricas e colunas (SQL adhoc) — definidas uma vez, usadas em vários charts


def m(sql: str, rotulo: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": rotulo}


def col(sql: str, rotulo: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": rotulo}


M_CONTRATOS = m("COUNT(DISTINCT apf)", "Contratos (APF)")
M_EOS = m("COUNT(DISTINCT eo_cnpj)", "Entidades organizadoras")
M_PORTARIAS = m("COUNT(DISTINCT CASE WHEN ic_tem_portaria THEN portaria END)", "Portarias com vínculo")
M_COM_PORTARIA = m("COUNT(DISTINCT CASE WHEN ic_tem_portaria THEN apf END)", "Contratos com portaria do tipo")
M_UH = m("SUM(quantidade_uh)", "UH contratadas")
M_CONTRATADO = m("SUM(valor_contratado)", "Valor contratado (R$)")
M_DESEMBOLSADO = m("SUM(valor_desembolsado)", "Valor desembolsado (R$)")
M_PCT_FIN = m(
    "100.0 * SUM(CASE WHEN valor_contratado > 0 THEN valor_desembolsado END)"
    " / NULLIF(SUM(CASE WHEN valor_contratado > 0 THEN valor_contratado END), 0)",
    "Execução financeira (%)",
)
M_PCT_FIS = m(
    "SUM(CASE WHEN percentual_execucao_fisica IS NOT NULL AND quantidade_uh > 0"
    " THEN percentual_execucao_fisica * quantidade_uh END)"
    " / NULLIF(SUM(CASE WHEN percentual_execucao_fisica IS NOT NULL AND quantidade_uh > 0"
    " THEN quantidade_uh END), 0)",
    "Execução física (%, ponderada por UH)",
)
M_ALERTA = m("COUNT(DISTINCT CASE WHEN qt_alertas > 0 THEN apf END)", "Contratos com alerta")
M_SELECIONADOS = m(
    "COUNT(DISTINCT CASE WHEN portaria_selecao IS NOT NULL THEN apf END)",
    "Contratos com portaria de seleção",
)
M_AUTORIZADOS = m(
    "COUNT(DISTINCT CASE WHEN portaria_autorizacao IS NOT NULL THEN apf END)",
    "Contratos com autorização carregada",
)
# Curva S: sempre razão de somas (nunca média de percentuais)
M_CS_REAL = m("SUM(fis_realizado_x_uh) / NULLIF(SUM(uh_com_medicao), 0)", "Físico realizado (%)")
M_CS_PREV = m("SUM(fis_previsto_x_uh) / NULLIF(SUM(uh_com_previsao), 0)", "Físico previsto (%)")
M_CS_FIN = m("100.0 * SUM(vr_liberado_acumulado) / NULLIF(SUM(valor_contratado), 0)", "Financeiro liberado (%)")
M_CS_CONTRATOS = m("SUM(qt_contratos)", "Contratos na carteira")
# desvio da curva (dataset APF × tipo de portaria; o filtro de tipo deixa uma linha por APF)
M_FORA = m("COUNT(DISTINCT CASE WHEN faixa_curva_fisica IN ('Muito abaixo da curva', 'Abaixo da curva') THEN apf END)",
           "Empreendimentos abaixo da curva")
M_MUITO_ABAIXO = m("COUNT(DISTINCT CASE WHEN faixa_curva_fisica = 'Muito abaixo da curva' THEN apf END)",
                   "Muito abaixo da curva")
M_APF = m("COUNT(DISTINCT apf)", "Empreendimentos (APF)")
M_UH_FORA = m("SUM(CASE WHEN faixa_curva_fisica IN ('Muito abaixo da curva', 'Abaixo da curva') THEN quantidade_uh END)",
              "UH em empreendimentos abaixo da curva")
M_DESVIO_MED = m("AVG(desvio_mediana_pp)", "Desvio médio da mediana (p.p.)")
# funil (uma linha por proposta selecionada)
M_PROPOSTAS = m("COUNT(*)", "Propostas")
M_UH_SEL = m("SUM(quantidade_uh_selecionadas)", "UH selecionadas")
M_OCORRENCIAS = m("COUNT(*)", "Ocorrências")
M_APFS_PEND = m("COUNT(DISTINCT apf)", "APFs envolvidas")

C_PORTARIA = col("portaria", "Portaria")
C_TIPO = col("tipo_portaria", "Tipo de portaria")

# --------------------------------------------------------------------------
# blocos da aba


@dataclass
class Grafico:
    chave: str
    titulo: str
    dataset: str
    viz: str
    params: dict
    descricao: str = ""
    largura: int = 12
    altura: int = 50


@dataclass
class Reuso:
    chave: str
    largura: int = 12
    altura: int = 50


@dataclass
class Texto:
    md: str
    largura: int = 12
    altura: int = 20


@dataclass
class Secao:
    titulo: str
    linhas: list[list] = field(default_factory=list)


def big(chave, titulo, metrica, sub, descricao, fmt=",d", moeda=False, dataset=None) -> Grafico:
    p = {"metric": metrica, "subheader": sub, "y_axis_format": fmt, "header_font_size": 0.4,
         "subheader_font_size": 0.15}
    if moeda:
        p["currency_format"] = {"symbol": "BRL", "symbolPosition": "prefix"}
    return Grafico(chave, titulo, dataset or OPERACAO, "big_number_total", p, descricao, largura=3, altura=22)


def barra(chave, titulo, dataset, x, metricas, eixo_x, eixo_y, grupo=None, empilhar=True,
          horizontal=False, fmt=",d", descricao="", largura=12, altura=50, ordenar_por_soma=True) -> Grafico:
    p = {
        "x_axis": x,
        "metrics": metricas,
        "groupby": [grupo] if grupo else [],
        "orientation": "horizontal" if horizontal else "vertical",
        "stack": "Stack" if (empilhar and grupo) else None,
        "show_legend": True,
        "show_value": True,
        "y_axis_format": fmt,
        "row_limit": 1000,
        "x_axis_title": eixo_x,
        "y_axis_title": eixo_y,
        "x_axis_title_margin": 30,
        "y_axis_title_margin": 50,
        "y_axis_title_position": "Left",
        "rich_tooltip": True,
        "truncateYAxis": False,
    }
    if ordenar_por_soma:
        p.update({"x_axis_sort_series": "sum", "x_axis_sort_series_ascending": horizontal})
    return Grafico(chave, titulo, dataset, "echarts_timeseries_bar", p, descricao, largura, altura)


def tabela_agregada(chave, titulo, dataset, grupos, metricas, config=None, descricao="",
                    ordenar=None, largura=12, altura=50, filtro_sql=None) -> Grafico:
    p = {
        "query_mode": "aggregate",
        "groupby": grupos,
        "metrics": metricas,
        "include_search": True,
        "show_cell_bars": True,
        "align_pn": False,
        "color_pn": True,
        "allow_rearrange_columns": True,
        "column_config": config or {},
        "row_limit": 1000,
        "order_desc": True,
    }
    if ordenar:
        p["timeseries_limit_metric"] = ordenar
    if filtro_sql:
        p["adhoc_filters"] = [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": filtro_sql}]
    return Grafico(chave, titulo, dataset, "table", p, descricao, largura, altura)


def tabela_linhas(chave, titulo, dataset, colunas, config=None, condicionais=None, ordem=None,
                  descricao="", largura=12, altura=70, filtro_sql=None) -> Grafico:
    p = {
        "query_mode": "raw",
        "all_columns": colunas,
        "include_search": True,
        "show_cell_bars": True,
        "align_pn": False,
        "color_pn": True,
        "allow_rearrange_columns": True,
        "column_config": config or {},
        "conditional_formatting": condicionais or [],
        "order_by_cols": [json.dumps(o) for o in (ordem or [])],
        "row_limit": 5000,
        "server_page_length": 25,
        "page_length": 25,
    }
    if filtro_sql:
        p["adhoc_filters"] = [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": filtro_sql}]
    return Grafico(chave, titulo, dataset, "table", p, descricao, largura, altura)


N = lambda f=",d", barras=False: {"d3NumberFormat": f, "showCellBars": barras, "horizontalAlign": "right"}  # noqa: E731
TXT = lambda w=None: {k: v for k, v in {"horizontalAlign": "left", "showCellBars": False, "columnWidth": w}.items() if v}  # noqa: E731
VERM, AMAR, VERDE = "#E04355", "#FCC700", "#5AC189"


def cond(coluna, op, valor, cor):
    return {"column": coluna, "operator": op, "targetValue": valor, "colorScheme": cor}


# --------------------------------------------------------------------------
# 1. o que a aba mostra


def secoes() -> list[Secao]:
    s1 = Secao("Visão executiva", [
        [Texto(
            "## Rural — visão executiva\n"
            "**Contrato e empreendimento têm a mesma chave no Rural: a APF.** Cada APF é o contrato do "
            "empreendimento com o agente financeiro (CAIXA ou BB); não há, nas bases, um código de "
            "empreendimento separado. Por isso os cartões contam **contratos (APF)** uma vez só.\n\n"
            "**Portaria: escolha o tipo no filtro do topo.** Cada contrato tem até três portarias, uma de cada "
            "tipo, que não se somam: **Seleção** (162/2018 no PNHR; 354/2024 e 697/2026 no Novo Rural; 173/2024, "
            "840/2025 e 1.207/2025 na Calamidade RS), **Autorização de contratação** (Seleção 2023: 5 das 46 "
            "carregadas; Seleção 2025: 1.362/2026 e 1.449/2026) e **Trabalho Social** (21/2014, 464/2018 ou "
            "75/2025). O filtro *Tipo de "
            "portaria* aceita um tipo por vez; com ele, cada contrato é contado uma vez. Contrato sem portaria "
            "do tipo aparece com o motivo (ex. *Seleção 2023 sem autorização carregada*).\n\n"
            "Todos os números respondem aos filtros da aba (tipo de portaria, portaria, agente, programa, UF, "
            "EO, status, ano).",
            altura=40)],
        [
            big("k_contratos", "Contratos (APF)", M_CONTRATOS, "contrato = empreendimento",
                "COUNT(DISTINCT apf) na ouro_rural_painel_operacao."),
            big("k_eos", "Entidades organizadoras", M_EOS, "CNPJs distintos com 14 dígitos",
                "COUNT(DISTINCT eo_cnpj). Operações sem EO ou com CNPJ inválido não entram."),
            big("k_portarias", "Portarias com vínculo", M_PORTARIAS, "do tipo escolhido no filtro",
                "Portarias distintas do tipo escolhido que têm ao menos um contrato vinculado."),
            big("k_alerta", "Contratos com alerta", M_ALERTA, "sem EO válida, desembolso > contratado ou EO ≠ seleção",
                "APFs com qt_alertas > 0 (regras R01, R02, R05 e R07)."),
        ],
        [
            big("k_uh", "UH contratadas", M_UH, "unidades habitacionais", "SUM(quantidade_uh)."),
            big("k_contratado", "Valor contratado", M_CONTRATADO, "soma por APF, sem aporte",
                "SUM(valor_contratado).", fmt=",.0f", moeda=True),
            big("k_fin", "Execução financeira", M_PCT_FIN, "Σ desembolsado ÷ Σ contratado",
                "Só APFs com valor contratado > 0.", fmt=",.1f"),
            big("k_fis", "Execução física", M_PCT_FIS, "média ponderada por UH (%)",
                "Σ(% físico × UH) ÷ Σ UH, só APFs com medição.", fmt=",.1f"),
        ],
    ])

    s2 = Secao("Análise por portaria", [
        [Texto(
            "### Como ler\n"
            "A tabela compara as portarias **do tipo escolhido no filtro** (seleção, autorização de contratação "
            "ou Trabalho Social) e mostra, na mesma linha, quantos contratos não têm portaria daquele tipo e por "
            "quê. Valores são **contratado** e **desembolsado**; percentuais recalculados no agregado, nunca "
            "somados.\n\n"
            "O **funil** mostra cada proposta das portarias de seleção: só selecionada, autorizada a contratar ou "
            "já contratada. A ligação seleção → autorização é exata (mesmo número de proposta); a ligação com o "
            "contrato é por APF (162/2018) ou por CNPJ da EO + município (354/2024, 697/2026), com a confiança "
            "indicada. A 697/2026 é de jun/2026 e ainda não tem contratos.", altura=30)],
        [tabela_agregada(
            "t_portaria", "Comparação entre portarias do tipo escolhido", OPERACAO, [C_TIPO, C_PORTARIA],
            [M_CONTRATOS, M_EOS, M_UH, M_CONTRATADO, M_DESEMBOLSADO, M_PCT_FIN, M_PCT_FIS, M_ALERTA],
            {"Tipo de portaria": TXT(170), "Portaria": TXT(260), "Contratos (APF)": N(barras=True),
             "Entidades organizadoras": N(), "UH contratadas": N(barras=True),
             "Valor contratado (R$)": N(",.0f", True), "Valor desembolsado (R$)": N(",.0f"),
             "Execução financeira (%)": N(",.1f"), "Execução física (%, ponderada por UH)": N(",.1f"),
             "Contratos com alerta": N()},
            "Uma linha por portaria do tipo escolhido; contagens distintas por APF e CNPJ.",
            ordenar=M_CONTRATOS, altura=45)],
        [
            barra("b_portaria_valor", "Valor contratado e desembolsado por portaria", OPERACAO,
                  C_PORTARIA, [M_CONTRATADO, M_DESEMBOLSADO], "Portaria", "Valor (R$)",
                  empilhar=False, fmt="SMART_NUMBER", largura=6, altura=50),
            barra("b_funil", "Funil das portarias de seleção: selecionada → autorizada → contratada", FUNIL,
                  col("replace(portaria_codigo, '_', '/')", "Portaria de seleção"), [M_PROPOSTAS],
                  "Portaria de seleção", "Propostas", grupo=col("situacao_funil", "Situação no funil"),
                  horizontal=True, largura=6, altura=50, ordenar_por_soma=False),
        ],
        [tabela_linhas(
            "t_resumo_portarias", "Portarias do ciclo de seleção: o que cada uma lista e o que virou contrato",
            RESUMO_PORTARIA,
            [col("portaria", "Portaria"), col("tipo_portaria", "Tipo"), col("ciclo_selecao", "Ciclo"),
             col("dt_portaria", "Data"), col("situacao_anexo", "Anexo"), col("qt_propostas", "Propostas"),
             col("uh_propostas", "UH"), col("qt_autorizadas", "Autorizadas"), col("qt_contratadas", "Contratadas"),
             col("pct_contratadas", "% contratadas"), col("dias_medios_ate_contrato", "Dias até o contrato"),
             col("qt_vinculo_exato + qt_vinculo_alta", "Vínculo exato/alto"),
             col("qt_vinculo_media + qt_vinculo_baixa", "Vínculo médio/baixo"), col("fonte_lista", "Fonte do anexo")],
            {"Portaria": TXT(90), "Tipo": TXT(170), "Ciclo": TXT(120), "Anexo": TXT(150), "Propostas": N(),
             "UH": N(), "Autorizadas": N(), "Contratadas": N(barras=True), "% contratadas": N(",.1f"),
             "Dias até o contrato": N(), "Vínculo exato/alto": N(), "Vínculo médio/baixo": N(),
             "Fonte do anexo": TXT(320)},
            [cond("Anexo", "==", "Anexo não carregado", AMAR)],
            ordem=[["tipo_portaria", True], ["dt_portaria", True]],
            descricao="Uma linha por portaria do catálogo, inclusive as que ainda não têm anexo carregado.",
            altura=60)],
    ])

    s3 = Secao("Contratos, empreendimentos e entidades organizadoras", [
        [Texto(
            "### Como ler\n"
            "**Situação do vínculo da EO:** *EO do cadastro de EO* (Cad PJ/INT065/INT057, confiável); "
            "*EO a confirmar* (veio do campo \"construtora/entidade\" do arquivo AF CAIXA — falta confirmar "
            "com a CAIXA); *CNPJ inválido na origem* (só zeros); *Sem EO informada*.\n\n"
            "**Situação da portaria de TS** diferencia *não informada* (a planilha tem a APF, o campo está "
            "vazio) de *sem registro* (a APF não está na planilha). A tabela de EOs agrega todas as operações "
            "de cada EO e não responde aos filtros de agente, programa e status.", altura=28)],
        [
            barra("b_vinculo_eo", "Contratos por situação do vínculo da EO", OPERACAO,
                  col("situacao_vinculo_eo", "Situação do vínculo da EO"), [M_CONTRATOS],
                  "Situação do vínculo da EO", "Contratos (APF)",
                  grupo=col("agente_financeiro", "Agente financeiro"), horizontal=True, largura=6, altura=45),
            barra("b_vinculo_ts", "Contratos por situação da portaria de Trabalho Social", OPERACAO,
                  col("situacao_portaria_ts", "Situação da portaria de TS"), [M_CONTRATOS],
                  "Situação da portaria de TS", "Contratos (APF)",
                  grupo=col("programa", "Programa"), horizontal=True, largura=6, altura=45),
        ],
        [Reuso("eo_tabela", altura=70)],
        [tabela_linhas(
            "t_operacoes", "Contratos/empreendimentos com seus vínculos", OPERACAO,
            [col("apf", "APF"), col("nome_empreendimento", "Empreendimento"), col("municipio", "Município"),
             col("uf", "UF"), col("agente_financeiro", "Agente"), col("programa", "Programa"),
             col("eo_nome", "Entidade organizadora"), col("eo_cnpj", "CNPJ da EO"),
             col("situacao_vinculo_eo", "Vínculo da EO"), col("portaria_selecao", "Portaria de seleção"),
             col("confianca_vinculo_selecao", "Confiança do vínculo"),
             col("portaria_autorizacao", "Portaria de autorização"), col("portaria_ts", "Portaria de TS"),
             col("status_execucao", "Status"),
             col("quantidade_uh", "UH"), col("valor_contratado", "Contratado (R$)"),
             col("valor_desembolsado", "Desembolsado (R$)"),
             col("percentual_execucao_fisica", "% físico"), col("percentual_execucao_financeira", "% financeiro"),
             col("qt_alertas", "Alertas")],
            {"Empreendimento": TXT(220), "Entidade organizadora": TXT(240), "UH": N(),
             "Contratado (R$)": N(",.0f", True), "Desembolsado (R$)": N(",.0f"),
             "% físico": N(",.1f", True), "% financeiro": N(",.1f"), "Alertas": N()},
            [cond("Alertas", ">", 0, VERM)],
            ordem=[["qt_alertas", False], ["valor_contratado", False]],
            descricao="Uma linha por APF. Ordenada por alertas e valor contratado.", altura=75)],
        [Reuso("recontratacao_tabela", altura=60)],
    ])

    s4 = Secao("Execução física e financeira", [
        [Texto(
            "### Como ler\n"
            "**% físico** = medição mais recente do agente (0 a 100). **% financeiro** = desembolsado ÷ "
            "contratado × 100 (nulo se o contratado for zero ou nulo). **Diferença** = financeiro − físico, em "
            "pontos percentuais: positiva, pagou mais do que a obra avançou; negativa, a obra está à frente "
            "do pagamento. No agregado, físico é ponderado por UH e financeiro é Σ desembolsado ÷ Σ contratado.\n\n"
            "A evolução mês a mês usa os retratos mensais da CAIXA (Novo Rural) e a avaliação GEHIS.", altura=26)],
        [
            barra("b_exec_programa", "Execução física × financeira por programa e agente", OPERACAO,
                  col("programa || ' · ' || agente_financeiro", "Programa · agente"), [M_PCT_FIS, M_PCT_FIN],
                  "Programa · agente", "Execução (%)", empilhar=False, fmt=",.1f", largura=6, altura=50,
                  ordenar_por_soma=False),
            barra("b_ritmo", "Ritmo físico-financeiro dos contratos", OPERACAO,
                  col("ritmo_fisico_financeiro", "Ritmo físico-financeiro"), [M_CONTRATOS],
                  "Ritmo físico-financeiro", "Contratos (APF)",
                  grupo=col("status_execucao", "Status de execução"), horizontal=True, largura=6, altura=50),
        ],
        [barra("b_ano", "Contratos e valor contratado por ano de contratação", OPERACAO,
               col("ano_contratacao", "Ano de contratação"), [M_CONTRATOS],
               "Ano de contratação", "Contratos (APF)", grupo=col("programa", "Programa"),
               largura=12, altura=45, ordenar_por_soma=False)],
        [Reuso("estagio_mensal", largura=6, altura=55), Reuso("gehis_mensal", largura=6, altura=55)],
        [tabela_linhas(
            "t_descompasso", "Maiores descompassos entre execução física e financeira", OPERACAO,
            [col("apf", "APF"), col("nome_empreendimento", "Empreendimento"), col("uf", "UF"),
             col("eo_nome", "Entidade organizadora"), col("status_execucao", "Status"),
             col("percentual_execucao_fisica", "% físico"), col("percentual_execucao_financeira", "% financeiro"),
             col("diferenca_financeiro_fisico_pp", "Diferença (p.p.)"),
             col("valor_contratado", "Contratado (R$)"), col("valor_desembolsado", "Desembolsado (R$)"),
             col("consistencia_desembolso", "Consistência do desembolso"),
             col("dt_referencia_execucao_fisica", "Medição física de")],
            {"Empreendimento": TXT(220), "Entidade organizadora": TXT(220), "% físico": N(",.1f", True),
             "% financeiro": N(",.1f"), "Diferença (p.p.)": N(",.1f"), "Contratado (R$)": N(",.0f"),
             "Desembolsado (R$)": N(",.0f")},
            [cond("Diferença (p.p.)", ">", 20, AMAR), cond("Diferença (p.p.)", "<", -20, VERM),
             cond("% financeiro", ">", 100, VERM)],
            ordem=[["diferenca_financeiro_fisico_pp", False]],
            filtro_sql="diferenca_financeiro_fisico_pp IS NOT NULL",
            descricao="Só APFs com os dois percentuais. Ordenada pela diferença.", altura=65)],
    ])

    s_curva = Secao("Curva S físico-financeira (Novo MCMV Rural)", [
        [Texto(
            "### Como ler\n"
            "Carteira do **Novo MCMV Rural** (CAIXA) assinada até cada mês. **Físico realizado** e **previsto**: "
            "retratos mensais do MONIT_MOV_OBRA, só a partir de **dez/2025** (antes não há série física); média "
            "ponderada por UH. **Financeiro**: liberações do MONIT_MOV_FINANC acumuladas desde jul/2024, líquidas "
            "de devoluções, ÷ valor contratado. O financeiro inclui obra, Trabalho Social, ATEC, cisternas e custos "
            "indiretos, por isso tende a andar à frente do físico. O gráfico começa em jan/2025: antes disso eram "
            "menos de 100 contratos e um único contrato movia a curva.\n\n"
            "Os três percentuais são razões de somas (respondem aos filtros de portaria, UF e agente). O PNHR "
            "(histórico) fica fora da curva: não tem medição física mensal.", altura=30)],
        [Grafico("g_curva_s", "Curva S: físico realizado, físico previsto e financeiro liberado", CURVA_S,
                 "echarts_timeseries_line",
                 {"x_axis": "mes", "time_grain_sqla": "P1M", "metrics": [M_CS_PREV, M_CS_REAL, M_CS_FIN],
                  "groupby": [], "y_axis_format": ",.1f", "show_legend": True, "rich_tooltip": True,
                  "markerEnabled": True, "x_axis_title": "Mês", "y_axis_title": "Execução acumulada (%)",
                  "x_axis_title_margin": 30, "y_axis_title_margin": 50, "y_axis_title_position": "Left",
                  "truncateYAxis": False, "y_axis_bounds": [0, 100], "row_limit": 1000,
                  "adhoc_filters": [{"expressionType": "SQL", "clause": "WHERE",
                                     "sqlExpression": "mes >= DATE '2025-01-01'"}]},
                 "A partir de jan/2025 (antes disso a carteira tinha poucos contratos e a curva oscila). Razões de somas por mês: Σ(% × UH) ÷ Σ UH para o físico; Σ liberado ÷ Σ contratado para o financeiro.",
                 largura=8, altura=55),
         Grafico("g_curva_carteira", "Contratos na carteira por mês", CURVA_S, "echarts_timeseries_bar",
                 {"x_axis": "mes", "time_grain_sqla": "P1M", "metrics": [M_CS_CONTRATOS], "groupby": [],
                  "y_axis_format": ",d", "show_legend": False, "x_axis_title": "Mês",
                  "y_axis_title": "Contratos", "x_axis_title_margin": 30, "y_axis_title_margin": 50,
                  "y_axis_title_position": "Left", "row_limit": 1000},
                 "Contratos do Novo Rural assinados até o mês.", largura=4, altura=55)],
        [tabela_agregada(
            "t_curva_portaria", "Curva S no último mês, por portaria do tipo escolhido", CURVA_S, [C_PORTARIA],
            [M_CS_CONTRATOS, M_CS_PREV, M_CS_REAL, M_CS_FIN],
            {"Portaria": TXT(260), "Contratos na carteira": N(barras=True), "Físico previsto (%)": N(",.1f"),
             "Físico realizado (%)": N(",.1f", True), "Financeiro liberado (%)": N(",.1f")},
            "Último mês da série (filtro fixo no SQL).", ordenar=M_CS_CONTRATOS, altura=40,
            filtro_sql="ic_ultimo_mes")],
    ])

    s_fora = Secao("Empreendimentos fora da curva", [
        [Texto(
            "### Como ler\n"
            "A **curva de referência** é o comportamento típico dos empreendimentos do Novo Rural **na mesma idade "
            "de contrato** (meses desde a contratação): a linha do meio é a mediana do % físico realizado e a faixa "
            "entre o quartil inferior (p25) e o superior (p75) concentra metade dos empreendimentos. Comparar pela "
            "idade, e não pelo mês do calendário, evita julgar um contrato de 3 meses junto com um de 20.\n\n"
            "Cada empreendimento é comparado no seu retrato mais recente: **muito abaixo** = no pior décimo da sua "
            "idade ou 20 p.p. ou mais abaixo da mediana; **abaixo** = abaixo do p25; **acima** = acima do p75. "
            "*Desvio do cronograma* compara com o previsto do próprio contrato. Fonte: MONIT_MOV_OBRA (CAIXA), "
            "retratos de dez/2025 em diante.", altura=32)],
        [
            big("k_fora", "Abaixo da curva", M_FORA, "abaixo do p25 da mesma idade",
                "Empreendimentos nas faixas Abaixo e Muito abaixo da curva.", dataset=CURVA_DESVIO),
            big("k_muito_abaixo", "Muito abaixo da curva", M_MUITO_ABAIXO, "pior décimo ou −20 p.p. da mediana",
                "Empreendimentos na faixa Muito abaixo da curva.", dataset=CURVA_DESVIO),
            big("k_uh_fora", "UH abaixo da curva", M_UH_FORA, "unidades nesses empreendimentos",
                "Soma das UH dos empreendimentos abaixo da curva.", dataset=CURVA_DESVIO),
            big("k_desvio", "Desvio médio", M_DESVIO_MED, "p.p. em relação à mediana da idade",
                "Média do desvio da mediana (negativo = carteira atrás da referência).", fmt=",.1f", dataset=CURVA_DESVIO),
        ],
        [
            Grafico("g_curva_ref", "Curva de referência por tempo de contrato (% físico realizado)", CURVA_REF,
                    "echarts_timeseries_line",
                    {"x_axis": "idade_meses", "metrics": [
                        m("MAX(fis_p25)", "Quartil inferior (p25)"), m("MAX(fis_mediana)", "Mediana"),
                        m("MAX(fis_p75)", "Quartil superior (p75)"),
                        m("MAX(fis_previsto_mediana)", "Previsto no cronograma (mediana)")],
                     "groupby": [], "y_axis_format": ",.1f", "show_legend": True, "rich_tooltip": True,
                     "markerEnabled": True, "x_axis_title": "Meses desde a contratação",
                     "y_axis_title": "% físico realizado", "x_axis_title_margin": 30, "y_axis_title_margin": 50,
                     "y_axis_title_position": "Left", "y_axis_bounds": [0, 100], "row_limit": 1000,
                     "x_axis_sort": "idade_meses", "x_axis_sort_asc": True,
                     "adhoc_filters": [{"expressionType": "SQL", "clause": "WHERE",
                                        "sqlExpression": "ic_referencia_valida"}]},
                    "Por idade de contrato, com pelo menos 10 retratos na idade.", largura=7, altura=55),
            barra("b_faixa_uf", "Empreendimentos por faixa da curva e UF", CURVA_DESVIO,
                  col("uf", "UF"), [M_APF], "UF", "Empreendimentos (APF)",
                  grupo=col("faixa_curva_fisica", "Faixa da curva"), largura=5, altura=55),
        ],
        [tabela_linhas(
            "t_fora_curva", "Empreendimentos abaixo da curva (piores primeiro)", CURVA_DESVIO,
            [col("apf", "APF"), col("nome_empreendimento", "Empreendimento"), col("municipio", "Município"),
             col("uf", "UF"), col("eo_nome", "Entidade organizadora"), col("faixa_curva_fisica", "Faixa"),
             col("idade_meses", "Meses de contrato"), col("pct_fisico_realizado", "% físico"),
             col("fis_mediana", "Mediana da idade"), col("desvio_mediana_pp", "Desvio da mediana (p.p.)"),
             col("pct_fisico_previsto", "% previsto (cronograma)"),
             col("desvio_cronograma_pp", "Desvio do cronograma (p.p.)"), col("pct_financeiro", "% financeiro"),
             col("situacao_operacao", "Situação"), col("quantidade_uh", "UH"),
             col("valor_contratado", "Contratado (R$)"), col("portaria", "Portaria (tipo do filtro)"),
             col("dt_ultimo_retrato", "Retrato de")],
            {"Empreendimento": TXT(220), "Entidade organizadora": TXT(220), "Faixa": TXT(170),
             "% físico": N(",.1f", True), "Mediana da idade": N(",.1f"), "Desvio da mediana (p.p.)": N(",.1f"),
             "% previsto (cronograma)": N(",.1f"), "Desvio do cronograma (p.p.)": N(",.1f"),
             "% financeiro": N(",.1f"), "UH": N(), "Contratado (R$)": N(",.0f")},
            [cond("Desvio da mediana (p.p.)", "<", -20, VERM), cond("Desvio da mediana (p.p.)", "<", 0, AMAR),
             cond("Desvio do cronograma (p.p.)", "<", -20, VERM)],
            ordem=[["desvio_mediana_pp", True]],
            filtro_sql="faixa_curva_fisica IN ('Muito abaixo da curva', 'Abaixo da curva')",
            descricao="Só empreendimentos abaixo do quartil inferior da sua idade. Ordenada pelo desvio da mediana.",
            altura=75)],
    ])

    s5 = Secao("Qualidade e integridade dos dados", [
        [Texto(
            "### Como ler\n"
            "Cada pendência tem uma **classificação**, para não tratar dúvida como erro:\n"
            "- **Inconsistência comprovada** — contradiz uma regra objetiva (CNPJ \"0\"; desembolso > contratado + aporte);\n"
            "- **Associação ausente** — o vínculo esperado não existe (sem EO; selecionado sem contrato identificável);\n"
            "- **Informação ausente** — campo vazio na origem (portaria de TS não informada; APF fora das planilhas de TS);\n"
            "- **Requer investigação** — sinal que depende de confirmação (EO vinda do AF CAIXA; EO diferente da selecionada; "
            "mesmo CNPJ com nomes diferentes; registro de TS sem contrato na visão consolidada; contrato assinado antes da "
            "portaria de autorização).\n\n"
            "A tabela conta **ocorrências**: uma APF pode estar em mais de uma regra. Duplicidade de APF foi "
            "verificada na construção da ouro (chave única) e não há.", altura=34)],
        [
            tabela_agregada(
                "t_regras", "Pendências por regra", PENDENCIA,
                [col("regra_codigo", "Regra"), col("regra", "Descrição"), col("classificacao", "Classificação"),
                 col("entidade", "Entidade")],
                [M_OCORRENCIAS, M_APFS_PEND],
                {"Descrição": TXT(320), "Classificação": TXT(170), "Entidade": TXT(200),
                 "Ocorrências": N(barras=True), "APFs envolvidas": N()},
                "Uma linha por regra.", ordenar=M_OCORRENCIAS, largura=7, altura=50),
            barra("b_classificacao", "Ocorrências por classificação", PENDENCIA,
                  col("classificacao", "Classificação"), [M_OCORRENCIAS], "Classificação", "Ocorrências",
                  grupo=col("entidade", "Entidade"), horizontal=True, largura=5, altura=50),
        ],
        [tabela_linhas(
            "t_pendencias", "Pendências registro a registro", PENDENCIA,
            [col("regra_codigo", "Regra"), col("classificacao", "Classificação"), col("entidade", "Entidade"),
             col("chave", "Registro"), col("eo_nome", "Entidade organizadora"), col("uf", "UF"),
             col("municipio", "Município"), col("agente_financeiro", "Agente"), col("detalhe", "Detalhe")],
            {"Classificação": TXT(170), "Entidade organizadora": TXT(220), "Detalhe": TXT(420)},
            [], ordem=[["regra_codigo", True]],
            descricao="Use a busca para achar uma APF, CNPJ ou município.", altura=70)],
        [Reuso("cobertura", altura=55)],
    ])
    return [s1, s2, s3, s4, s_curva, s_fora, s5]


# --------------------------------------------------------------------------
# 2. filtros nativos (escopo: só a aba Rural)

#: (chave, nome, coluna, opções). O tipo de portaria é obrigatório e de seleção única — com ele cada APF
#: aparece uma vez no dataset APF × tipo de portaria; a portaria depende do tipo escolhido.
FILTROS = [
    ("tipo_portaria", "Tipo de portaria", "tipo_portaria",
     {"unico": True, "obrigatorio": True, "padrao": "Seleção"}),
    ("portaria", "Portaria", "portaria", {"pai": "tipo_portaria"}),
    ("agente", "Agente financeiro", "agente_financeiro", {}),
    ("programa", "Programa", "programa", {}),
    ("uf", "UF", "uf", {}),
    ("vinculo_eo", "Situação do vínculo da EO", "situacao_vinculo_eo", {}),
    ("status", "Status de execução", "status_execucao", {}),
    ("ano", "Ano de contratação", "ano_contratacao", {}),
    ("eo", "Entidade organizadora", "eo_nome", {}),
]


def filtro(chave, nome, coluna, dataset_id, tab_id, excluidos, opcoes=None) -> dict:
    opcoes = opcoes or {}
    padrao = opcoes.get("padrao")
    mascara = {}
    if padrao:
        mascara = {
            "filterState": {"value": [padrao]},
            "extraFormData": {"filters": [{"col": coluna, "op": "IN", "val": [padrao]}]},
        }
    return {
        "id": f"{FILTRO_PREFIXO}{chave}",
        "name": nome,
        "filterType": "filter_select",
        "type": "NATIVE_FILTER",
        "targets": [{"column": {"name": coluna}, "datasetId": dataset_id}],
        "defaultDataMask": mascara,
        "controlValues": {"multiSelect": not opcoes.get("unico", False),
                          "enableEmptyFilter": bool(opcoes.get("obrigatorio")),
                          "defaultToFirstItem": False,
                          "searchAllOptions": chave in ("eo", "portaria"), "inverseSelection": False},
        "scope": {"rootPath": [tab_id], "excluded": sorted(excluidos)},
        "cascadeParentIds": [f"{FILTRO_PREFIXO}{opcoes['pai']}"] if opcoes.get("pai") else [],
        "description": f"Filtra os gráficos da aba Rural cujo dataset tem a coluna `{coluna}`.",
    }


# --------------------------------------------------------------------------
# 3. construção


def garantir_dataset(api: Superset, cache: dict, tabela: str) -> int:
    ds = cache["datasets"].get((SCHEMA, tabela))
    if ds is None:
        ds = api.criar("dataset", {"database": cache["db"], "schema": SCHEMA, "table_name": tabela})
        cache["datasets"][(SCHEMA, tabela)] = ds
    if ds != -1 and not api.dry_run:
        api.s.put(f"{api.base}/api/v1/dataset/{ds}/refresh", headers=api.h, timeout=120)
    return ds


def colunas_do_dataset(api: Superset, ds: int) -> set[str]:
    if ds == -1:
        return set()
    return {c["column_name"] for c in (api.get("dataset", ds) or {}).get("columns", [])}


def garantir_grafico(api: Superset, cache: dict, g: Grafico) -> int:
    ds = garantir_dataset(api, cache, g.dataset)
    params = {"datasource": f"{ds}__table", "viz_type": g.viz, "adhoc_filters": [],
              "time_range": "No filter", **g.params}
    corpo = {
        "slice_name": PREFIXO + g.titulo,
        "viz_type": g.viz,
        "datasource_id": ds,
        "datasource_type": "table",
        "description": g.descricao,
        "params": json.dumps(params),
    }
    nome = PREFIXO + g.titulo
    cid = cache["charts"].get(nome)
    if cid:
        api.atualizar("chart", cid, corpo)
    else:
        cid = api.criar("chart", corpo)
        cache["charts"][nome] = cid
    cache["chart_dataset"][cid] = ds
    return cid


def dataset_do_chart(info: dict) -> int | None:
    """Id do dataset de um chart existente, lido do `params` (`"<id>__table"`)."""
    if info.get("datasource_id"):
        return info["datasource_id"]
    try:
        return int(json.loads(info.get("params") or "{}")["datasource"].split("__")[0])
    except (KeyError, ValueError, AttributeError):
        return None


def subarvore(layout: dict, raiz: str) -> set[str]:
    vistos, pilha = set(), [raiz]
    while pilha:
        n = pilha.pop()
        if n in vistos or not isinstance(layout.get(n), dict):
            continue
        vistos.add(n)
        pilha.extend(layout[n].get("children", []))
    return vistos


def monta_aba(layout: dict, tab_id: str, blocos: list[list[dict]]) -> dict:
    """Troca só os filhos da aba Rural; o resto do layout fica intacto."""
    pais = layout[tab_id].get("parents", []) + [tab_id]
    for n in subarvore(layout, tab_id) - {tab_id}:
        layout.pop(n, None)
    layout[tab_id]["children"] = []
    for i, linha in enumerate(blocos):
        if len(linha) == 1 and linha[0]["tipo"] == "HEADER":
            hid = f"HEADER-RURAL-{i:02d}"
            layout[tab_id]["children"].append(hid)
            layout[hid] = {"id": hid, "type": "HEADER", "children": [], "parents": pais,
                           "meta": {"text": linha[0]["texto"], "headerSize": "MEDIUM_HEADER",
                                    "background": "BACKGROUND_TRANSPARENT"}}
            continue
        row = f"ROW-RURAL-{i:02d}"
        layout[tab_id]["children"].append(row)
        layout[row] = {"id": row, "type": "ROW", "children": [], "parents": pais,
                       "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for j, no in enumerate(linha):
            if no["tipo"] == "CHART":
                nid = f"CHART-RURAL-{i:02d}-{j}"
                meta = {"chartId": no["chart"], "width": no["largura"], "height": no["altura"],
                        "sliceName": no.get("titulo", "")}
            else:
                nid = f"MARKDOWN-RURAL-{i:02d}-{j}"
                meta = {"width": no["largura"], "height": no["altura"], "code": no["md"]}
            layout[row]["children"].append(nid)
            layout[nid] = {"id": nid, "type": no["tipo"], "children": [], "parents": pais + [row], "meta": meta}
    return layout


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    api = Superset(dry_run=args.dry_run)

    cache = {
        "db": next(d["id"] for d in api.listar("database") if d["database_name"] == DATABASE_NAME),
        "datasets": {(d.get("schema"), d["table_name"]): d["id"] for d in api.listar("dataset")},
        "charts": {c["slice_name"]: c["id"] for c in api.listar("chart")},
        "chart_dataset": {},
    }

    painel = api.get("dashboard", SLUG)
    if not painel:
        raise SystemExit(f"dashboard '{SLUG}' não encontrado — este script só reconstrói a aba {ABA}.")
    
    layout = json.loads(painel.get("position_json") or "{}")

    # O position_json também pode conter metadados que não são nós.
    tab_id = next(
        (
            k for k, v in layout.items()
            if isinstance(v, dict)
            and v.get("type") == "TAB"
            and (v.get("meta") or {}).get("text") == ABA
        ),
        None,
    )
    if not tab_id:
        raise SystemExit(f"aba '{ABA}' não encontrada no dashboard '{SLUG}'.")

    blocos: list[list[dict]] = []
    for sec in secoes():
        print(f"  {sec.titulo}")
        blocos.append([{"tipo": "HEADER", "texto": sec.titulo}])
        for linha in sec.linhas:
            nos = []
            for item in linha:
                if isinstance(item, Texto):
                    nos.append({"tipo": "MARKDOWN", "md": item.md, "largura": item.largura, "altura": item.altura})
                elif isinstance(item, Reuso):
                    cid = REUSO[item.chave]
                    info = api.get("chart", cid) or {}
                    cache["chart_dataset"][cid] = dataset_do_chart(info)
                    nos.append({"tipo": "CHART", "chart": cid, "largura": item.largura, "altura": item.altura,
                                "titulo": info.get("slice_name", "")})
                else:
                    cid = garantir_grafico(api, cache, item)
                    nos.append({"tipo": "CHART", "chart": cid, "largura": item.largura, "altura": item.altura,
                                "titulo": PREFIXO + item.titulo})
                    print(f"    {item.titulo} -> chart {cid}")
            blocos.append(nos)

    if api.dry_run:
        print("\n[dry-run] layout e filtros não gravados.")
        return

    novo_layout = monta_aba(layout, tab_id, blocos)

    # filtros: alvo no dataset principal; exclui do escopo os charts cujo dataset não tem a coluna
    ds_op = garantir_dataset(api, cache, OPERACAO)
    charts_aba = [n["chart"] for linha in blocos for n in linha if n.get("tipo") == "CHART"]
    colunas_por_ds = {ds: colunas_do_dataset(api, ds) for ds in set(cache["chart_dataset"].values()) if ds}
    meta = json.loads(painel.get("json_metadata") or "{}")
    outros = [f for f in meta.get("native_filter_configuration", [])
              if not str(f.get("id", "")).startswith(FILTRO_PREFIXO)]
    rurais = []
    for chave, nome, coluna, opcoes in FILTROS:
        excl = [c for c in charts_aba if coluna not in colunas_por_ds.get(cache["chart_dataset"].get(c), set())]
        rurais.append(filtro(chave, nome, coluna, ds_op, tab_id, excl, opcoes))
    meta["native_filter_configuration"] = outros + rurais
    meta["cross_filters_enabled"] = True

    api.atualizar("dashboard", painel["id"], {"position_json": json.dumps(novo_layout),
                                               "json_metadata": json.dumps(meta)})
    for cid in charts_aba:
        info = api.get("chart", cid) or {}
        atuais = {d["id"] for d in (info.get("dashboards") or [])}
        if painel["id"] not in atuais:
            api.atualizar("chart", cid, {"dashboards": sorted(atuais | {painel["id"]})})
    print(f"\naba {ABA} reconstruída no dashboard {painel['id']}: {len(charts_aba)} charts, {len(rurais)} filtros.")
    print(f"URL: {api.base}/superset/dashboard/{SLUG}/")


if __name__ == "__main__":
    main()
