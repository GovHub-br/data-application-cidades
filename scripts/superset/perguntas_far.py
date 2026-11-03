#!/usr/bin/env python3
"""Provisiona o painel "Perguntas: FAR" no Superset.

Mesmo formato do painel ``mcid-perguntas``: uma aba por bloco de perguntas, os
títulos de seção são as perguntas, cada gráfico vem com um "Como ler" ao lado e
a última aba traz a matriz do que os dados respondem e do que falta.

Idempotente: datasets, gráficos e painel são encontrados pelo nome e
atualizados. Só lê os datasets que já existiam (ex. ``ouro_far_serie_mensal_apf``,
do painel FAR); refresh de colunas só nos datasets criados para este painel.
Use ``--dry-run`` para ver o que seria criado sem escrever nada.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bootstrap_conjuntura import Superset, env, get_or_create_database  # noqa: E402

SCHEMA = "ouro"
DASHBOARD_TITLE = "Perguntas: FAR"
DASHBOARD_SLUG = "perguntas-far"
PREFIXO = "FAR ▸ "

CONTRATO = "ouro_far_beneficiario_contrato"
MUNICIPIO = "ouro_far_beneficiario_municipio"
EMPREENDIMENTO = "ouro_far_empreendimento_perguntas"
CONTRAPARTIDA = "ouro_far_contrapartida"
COBERTURA = "ouro_far_cobertura_perguntas"
SERIE = "ouro_far_serie_mensal_apf"

DATASETS = [CONTRATO, MUNICIPIO, EMPREENDIMENTO, CONTRAPARTIDA, COBERTURA, SERIE]
# Datasets que outros painéis já usam: este script só os lê.
DATASETS_COMPARTILHADOS = {SERIE}

NOVO = "geracao = 'Novo MCMV'"
ATIVOS = "ic_ativo"


def metrica(rotulo: str, sql: str) -> dict:
    return {"expressionType": "SQL", "label": rotulo, "sqlExpression": sql}


def coluna(rotulo: str, sql: str) -> dict:
    return {"expressionType": "SQL", "label": rotulo, "sqlExpression": sql}


def onde(*condicoes: str) -> list[dict]:
    return [{"clause": "WHERE", "expressionType": "SQL", "sqlExpression": c} for c in condicoes]


CONTRATOS = metrica("Contratos", "COUNT(*)")
TAXA_INADIMPLENCIA = metrica(
    "Inadimplência (% dos ativos)",
    "SUM(CASE WHEN ic_inadimplente THEN 1 ELSE 0 END)::numeric"
    " / NULLIF(SUM(CASE WHEN ic_ativo THEN 1 ELSE 0 END), 0)",
)


def kpi(chave, titulo, dataset, metrica_, subtitulo, formato=",d", filtros=()):
    return {"key": chave, "title": titulo, "dataset": dataset, "viz_type": "big_number_total",
            "params": {"metric": metrica_, "subheader": subtitulo, "y_axis_format": formato,
                       "adhoc_filters": onde(*filtros), "time_range": "No filter"}}


def pizza(chave, titulo, dataset, grupo, metrica_, filtros=(), formato=",d"):
    return {"key": chave, "title": titulo, "dataset": dataset, "viz_type": "pie",
            "params": {"groupby": [grupo], "metric": metrica_, "adhoc_filters": onde(*filtros),
                       "donut": True, "show_labels": True, "show_legend": True,
                       "label_type": "key_percent", "number_format": formato,
                       "row_limit": 20, "time_range": "No filter"}}


def barras(chave, titulo, dataset, eixo, metricas, grupo=(), filtros=(), formato=",d",
           empilhado=True, percentual=False, eixo_titulo="", y_titulo="", tempo=None,
           ordenar_por_valor=False, horizontal=False, limite=1000):
    params = {
        "x_axis": eixo, "groupby": list(grupo), "metrics": metricas,
        "adhoc_filters": onde(*filtros), "row_limit": limite, "time_range": "No filter",
        "orientation": "horizontal" if horizontal else "vertical",
        "show_legend": bool(grupo) or len(metricas) > 1, "rich_tooltip": True,
        "y_axis_format": ".0%" if percentual else formato, "truncateYAxis": False,
        "x_axis_title": eixo_titulo, "x_axis_title_margin": 30 if eixo_titulo else 15,
        "y_axis_title": y_titulo, "y_axis_title_margin": 50 if y_titulo else 15,
        "y_axis_title_position": "Left", "x_axis_sort_asc": True,
    }
    if empilhado and (grupo or len(metricas) > 1):
        params["stack"] = "Stack"
    if percentual:
        params["contributionMode"] = "row"
    if tempo:
        params["time_grain_sqla"] = tempo
        params["x_axis_time_format"] = "%m/%Y"
    if ordenar_por_valor:
        params["x_axis_sort_series"] = "sum"
        params["x_axis_sort_series_ascending"] = False
    return {"key": chave, "title": titulo, "dataset": dataset, "viz_type": "echarts_timeseries_bar", "params": params}


def linhas(chave, titulo, dataset, eixo, metricas, grupo=(), filtros=(), formato=",d", tempo="P1M", y_titulo=""):
    return {"key": chave, "title": titulo, "dataset": dataset, "viz_type": "echarts_timeseries_line",
            "params": {"x_axis": eixo, "groupby": list(grupo), "metrics": metricas,
                       "adhoc_filters": onde(*filtros), "time_grain_sqla": tempo,
                       "x_axis_time_format": "%m/%Y", "row_limit": 10000, "time_range": "No filter",
                       "show_legend": True, "rich_tooltip": True, "markerEnabled": True,
                       "y_axis_format": formato, "y_axis_title": y_titulo, "y_axis_title_margin": 50,
                       "y_axis_title_position": "Left", "truncateYAxis": False}}


def tabela(chave, titulo, dataset, colunas, ordem, filtros=(), limite=1000):
    return {"key": chave, "title": titulo, "dataset": dataset, "viz_type": "table",
            "params": {"query_mode": "raw", "all_columns": colunas,
                       "order_by_cols": [json.dumps(o) for o in ordem],
                       "adhoc_filters": onde(*filtros), "row_limit": limite,
                       "server_pagination": False, "page_length": 25, "include_search": True,
                       "time_range": "No filter"}}


def tabela_agregada(chave, titulo, dataset, grupos, metricas, filtros=(), ordem_desc=True, limite=100):
    return {"key": chave, "title": titulo, "dataset": dataset, "viz_type": "table",
            "params": {"query_mode": "aggregate", "groupby": grupos, "metrics": metricas,
                       "adhoc_filters": onde(*filtros), "row_limit": limite,
                       "timeseries_limit_metric": metricas[0], "order_desc": ordem_desc,
                       "include_search": False, "time_range": "No filter"}}


def pct(rotulo: str, condicao: str, base: str = "true") -> dict:
    return metrica(rotulo, f"AVG(CASE WHEN {condicao} THEN 1.0 WHEN {base} THEN 0.0 END)")


CHARTS = [
    # Comece aqui
    kpi("kpi_familias", "Contratos de famílias", CONTRATO, CONTRATOS, "Novo MCMV + legado (CAIXA e BB)"),
    kpi("kpi_familias_novo", "Contratos de famílias no Novo MCMV", CONTRATO, CONTRATOS, "assinados desde 2025", filtros=[NOVO]),
    kpi("kpi_empreendimentos", "Empreendimentos do Novo MCMV contratados", EMPREENDIMENTO, metrica("Empreendimentos", "COUNT(*)"), "APFs no cadastro FAR"),
    kpi("kpi_inadimplencia", "Inadimplência da carteira ativa", CONTRATO, TAXA_INADIMPLENCIA, "ativos com mais de 90 dias de atraso", formato=".1%"),

    # 1. Beneficiário
    kpi("b_kpi_mulher", "Titular mulher", CONTRATO, pct("% titular mulher", "sexo_responsavel = 'Feminino'", "sexo_responsavel <> 'Não informado'"), "dos contratos com sexo informado", formato=".1%"),
    kpi("b_kpi_coobrigado", "Com coobrigado", CONTRATO, pct("% com coobrigado", "ic_coobrigado", "ic_coobrigado IS NOT NULL"), "dos contratos com a informação", formato=".1%"),
    kpi("b_kpi_idoso", "Titular idoso (Novo MCMV)", CONTRATO, pct("% titular idoso", "ic_responsavel_idoso", "ic_responsavel_idoso IS NOT NULL"), "dos contratos do Novo MCMV", formato=".1%", filtros=[NOVO]),
    kpi("b_kpi_renda", "Renda familiar média", CONTRATO, metrica("Renda média", "AVG(NULLIF(renda_familiar, 0))"), "contratos com renda comprovada > 0", formato="$,.0f"),
    barras("b_ano", "Contratos de famílias por ano de assinatura", CONTRATO, "ano_assinatura", [CONTRATOS], grupo=["geracao"], eixo_titulo="Ano de assinatura", y_titulo="Contratos"),
    barras("b_uf", "Contratos de famílias por UF", CONTRATO, "uf", [CONTRATOS], grupo=["geracao"], ordenar_por_valor=True, eixo_titulo="UF", y_titulo="Contratos"),
    pizza("b_modalidade", "Contratos por modalidade", CONTRATO, "modalidade", CONTRATOS),
    tabela("b_municipios", "Municípios com mais contratos de famílias", MUNICIPIO,
           [coluna("Município", "municipio_uf"), coluna("Geração", "geracao"), coluna("Contratos", "qt_contratos"),
            coluna("Ativos", "qt_ativos"), coluna("% titular mulher", "pct_responsavel_mulher"),
            coluna("Renda média (R$)", "renda_media"), coluna("% sem renda comprovada", "pct_sem_renda")],
           [["qt_contratos", False]], limite=200),
    barras("b_sexo", "Sexo do titular", CONTRATO, "geracao", [CONTRATOS], grupo=["sexo_responsavel"], percentual=True, filtros=["sexo_responsavel <> 'Não informado'"], eixo_titulo="Geração"),
    barras("b_estado_civil", "Estado civil do titular", CONTRATO, "geracao", [CONTRATOS], grupo=["estado_civil"], percentual=True, filtros=["estado_civil <> 'Não informado'"], eixo_titulo="Geração"),
    barras("b_coobrigado", "Contratos com e sem coobrigado", CONTRATO, "geracao", [CONTRATOS],
           grupo=[coluna("Coobrigado", "CASE WHEN ic_coobrigado THEN 'Com coobrigado' ELSE 'Sem coobrigado' END")],
           percentual=True, filtros=["ic_coobrigado IS NOT NULL"], eixo_titulo="Geração"),
    barras("b_idoso", "Titular idoso no Novo MCMV", CONTRATO, "uf", [CONTRATOS],
           grupo=[coluna("Titular", "CASE WHEN ic_responsavel_idoso THEN 'Idoso' ELSE 'Não idoso' END")],
           filtros=[NOVO], ordenar_por_valor=True, eixo_titulo="UF", y_titulo="Contratos"),
    barras("b_renda", "Contratos por faixa de renda familiar", CONTRATO, "faixa_renda", [CONTRATOS], grupo=["geracao"], percentual=True,
           filtros=["faixa_renda <> 'Não informada'"], eixo_titulo="Renda familiar comprovada"),

    # 2. Contratação e situação
    pizza("c_instrumento", "Doação ou alienação fiduciária (Novo MCMV)", CONTRATO, "tipo_instrumento", CONTRATOS, filtros=[NOVO]),
    barras("c_mes", "Contratos do Novo MCMV por mês de assinatura", CONTRATO, "dt_assinatura", [CONTRATOS], grupo=["tipo_instrumento"],
           filtros=[NOVO], tempo="P1M", eixo_titulo="Mês de assinatura", y_titulo="Contratos"),
    barras("c_situacao", "Situação dos contratos", CONTRATO, "agente_financeiro", [CONTRATOS], grupo=["situacao_contrato"], percentual=True,
           eixo_titulo="Agente financeiro"),
    tabela_agregada("c_taxas", "Taxas de ativos, liquidação, distrato e cancelamento", CONTRATO, ["geracao", "agente_financeiro"], [
        CONTRATOS,
        pct("% ativos", "situacao_contrato = 'Ativo'"),
        pct("% liquidados", "situacao_contrato = 'Liquidado'"),
        pct("% liquidados por sinistro (MIP/DFI)", "situacao_contrato IN ('Liquidado por sinistro (MIP/DFI)', 'Liquidado por MIP', 'Liquidado por DFI')"),
        pct("% distratados", "situacao_contrato = 'Distratado'"),
        pct("% cancelados", "situacao_contrato = 'Cancelado'"),
        pct("% sub-rogados", "situacao_contrato = 'Sub-rogado'"),
    ]),

    # 3. Inadimplência e território
    kpi("i_kpi_ativos", "Contratos ativos", CONTRATO, CONTRATOS, "com prestação a pagar", filtros=[ATIVOS]),
    kpi("i_kpi_inadimplentes", "Contratos inadimplentes", CONTRATO, CONTRATOS, "ativos com mais de 90 dias de atraso", filtros=["ic_inadimplente"]),
    kpi("i_kpi_taxa", "Taxa de inadimplência", CONTRATO, TAXA_INADIMPLENCIA, "inadimplentes ÷ ativos", formato=".1%"),
    kpi("i_kpi_valor", "Valor em atraso", CONTRATO, metrica("Valor em atraso", "SUM(valor_em_atraso)"), "dos contratos inadimplentes", formato="$,.0f", filtros=["ic_inadimplente"]),
    barras("i_agente", "Inadimplência por geração e agente financeiro", CONTRATO, "agente_financeiro", [TAXA_INADIMPLENCIA], grupo=["geracao"],
           empilhado=False, formato=".0%", eixo_titulo="Agente financeiro", y_titulo="% dos contratos ativos"),
    barras("i_renda", "Inadimplência por faixa de renda", CONTRATO, "faixa_renda", [TAXA_INADIMPLENCIA], grupo=["geracao"],
           empilhado=False, formato=".0%", filtros=["faixa_renda <> 'Não informada'"], eixo_titulo="Renda familiar comprovada", y_titulo="% dos contratos ativos"),
    barras("i_uf", "Inadimplência por UF", CONTRATO, "uf", [TAXA_INADIMPLENCIA], empilhado=False, formato=".0%",
           ordenar_por_valor=True, eixo_titulo="UF", y_titulo="% dos contratos ativos"),
    tabela("i_municipios", "Municípios com maior inadimplência (100 ou mais contratos ativos)", MUNICIPIO,
           [coluna("Município", "municipio_uf"), coluna("Geração", "geracao"), coluna("Ativos", "qt_ativos"),
            coluna("Inadimplentes", "qt_inadimplentes"), coluna("% inadimplência", "pct_inadimplencia"),
            coluna("Valor em atraso (R$)", "valor_em_atraso"), coluna("Renda média (R$)", "renda_media"),
            coluna("Distratados", "qt_distratados"), coluna("Retomados", "qt_retomados")],
           [["pct_inadimplencia", False]], filtros=["qt_ativos >= 100"], limite=300),

    # 4. Natureza e destino
    kpi("n_kpi_uh", "Unidades contratadas", EMPREENDIMENTO, metrica("UH", "SUM(qt_uh)"), "nos empreendimentos do Novo MCMV"),
    kpi("n_kpi_terreno", "Terreno doado pelo ente", EMPREENDIMENTO, pct("% terreno doado", "ic_terreno_doado"), "dos empreendimentos", formato=".1%"),
    kpi("n_kpi_valor_uh", "Valor médio por unidade", EMPREENDIMENTO, metrica("R$/UH", "SUM(valor_contratado) / NULLIF(SUM(qt_uh), 0)"), "valor contratado ÷ UH", formato="$,.0f"),
    kpi("n_kpi_mudou", "Mudaram de demanda", EMPREENDIMENTO, metrica("Empreendimentos", "SUM(CASE WHEN ic_demanda_mudou THEN 1 ELSE 0 END)"), "entre enquadramento, contratação e monitoramento"),
    barras("n_tipologia", "Tipo de edificação e de empreendimento", EMPREENDIMENTO, "tipo_edificacao", [metrica("Empreendimentos", "COUNT(*)")],
           grupo=["tipo_empreendimento"], ordenar_por_valor=True, eixo_titulo="Tipo de edificação", y_titulo="Empreendimentos"),
    pizza("n_demanda", "Destino do atendimento: tipo de demanda", EMPREENDIMENTO, "tipo_demanda", metrica("UH", "SUM(qt_uh)")),
    barras("n_origem", "Origem da contratação × tipo de demanda", EMPREENDIMENTO, "originacao", [metrica("Empreendimentos", "COUNT(*)")],
           grupo=["tipo_demanda"], eixo_titulo="Originação", y_titulo="Empreendimentos"),
    tabela("n_mudou", "Empreendimentos que mudaram de demanda", EMPREENDIMENTO,
           [coluna("APF", "apf"), coluna("Empreendimento", "nome_empreendimento"), coluna("Município", "municipio || '/' || uf"),
            coluna("Demanda no enquadramento", "demanda_enquadramento"), coluna("Demanda na contratação", "demanda_contratacao"),
            coluna("Demanda no monitoramento", "tipo_demanda"), coluna("UH", "qt_uh"), coluna("Situação da obra", "situacao_obra")],
           [["apf", True]], filtros=["ic_demanda_mudou"]),
    tabela_agregada("n_entorno", "Entorno declarado na proposta (% dos empreendimentos)", EMPREENDIMENTO, ["tipo_demanda"], [
        metrica("Empreendimentos", "COUNT(*) FILTER (WHERE ic_entorno_agua IS NOT NULL)"),
        pct("Água", "ic_entorno_agua", "ic_entorno_agua IS NOT NULL"),
        pct("Esgoto e lixo", "ic_entorno_esgoto_lixo", "ic_entorno_agua IS NOT NULL"),
        pct("Pavimentação", "ic_entorno_pavimentacao", "ic_entorno_agua IS NOT NULL"),
        pct("Drenagem", "ic_entorno_drenagem", "ic_entorno_agua IS NOT NULL"),
        pct("Educação infantil", "ic_entorno_educacao_infantil", "ic_entorno_agua IS NOT NULL"),
        pct("Ensino fundamental", "ic_entorno_fundamental_1", "ic_entorno_agua IS NOT NULL"),
        pct("UBS", "ic_entorno_ubs", "ic_entorno_agua IS NOT NULL"),
        pct("CRAS", "ic_entorno_cras", "ic_entorno_agua IS NOT NULL"),
        pct("Comércio", "ic_entorno_comercio", "ic_entorno_agua IS NOT NULL"),
    ], filtros=["ic_entorno_agua IS NOT NULL"]),

    # 5. Execução da obra
    kpi("e_kpi_ritmo", "Em ritmo", EMPREENDIMENTO, metrica("Empreendimentos", "COUNT(*)"), "obras avançando dentro do previsto", filtros=["alerta_execucao = 'Em ritmo'"]),
    kpi("e_kpi_atrasada", "Atrasadas (informado)", EMPREENDIMENTO, metrica("Empreendimentos", "COUNT(*)"), "situação informada pelo agente", filtros=["alerta_execucao = 'Atrasada (informada)'"]),
    kpi("e_kpi_sinal", "Sinal de paralisação", EMPREENDIMENTO, metrica("Empreendimentos", "COUNT(*)"), "3+ meses sem avanço físico ou ritmo abaixo do previsto",
        filtros=["alerta_execucao IN ('Sinal de paralisação: 3+ meses sem avanço', 'Ritmo abaixo do previsto')"]),
    kpi("e_kpi_paralisada", "Paralisadas (informado)", EMPREENDIMENTO, metrica("Empreendimentos", "COUNT(*)"), "situação informada pelo agente", filtros=["alerta_execucao = 'Paralisada (informada)'"]),
    barras("e_alerta_uf", "Alerta de execução por UF", EMPREENDIMENTO, "uf", [metrica("Empreendimentos", "COUNT(*)")], grupo=["alerta_execucao"],
           ordenar_por_valor=True, eixo_titulo="UF", y_titulo="Empreendimentos"),
    barras("e_mensal", "Situação das obras mês a mês", SERIE, "mes", [metrica("Empreendimentos", "COUNT(DISTINCT apf)")], grupo=["situacao_obra"],
           filtros=["situacao_obra IS NOT NULL"], tempo="P1M", eixo_titulo="Mês de referência", y_titulo="Empreendimentos"),
    tabela("e_lista", "Obras com sinal de atraso ou paralisação", EMPREENDIMENTO,
           [coluna("APF", "apf"), coluna("Empreendimento", "nome_empreendimento"), coluna("Município", "municipio || '/' || uf"),
            coluna("Alerta", "alerta_execucao"), coluna("Situação informada", "situacao_obra"),
            coluna("% físico", "percentual_execucao_fisica"), coluna("Desvio do previsto (p.p.)", "desvio_fisico"),
            coluna("Meses sem avanço", "meses_sem_avanco"), coluna("Avanço em 3 meses (p.p.)", "avanco_3_meses"),
            coluna("% financeiro", "percentual_execucao_financeira"), coluna("Previsão de entrega", "to_char(dt_previsao_entrega, 'DD/MM/YYYY')"),
            coluna("UH", "qt_uh")],
           [["meses_sem_avanco", False]],
           filtros=["alerta_execucao IN ('Sinal de paralisação: 3+ meses sem avanço', 'Ritmo abaixo do previsto', 'Atrasada (informada)', 'Paralisada (informada)')"]),

    # 6. Contrapartidas e pactuação
    kpi("p_kpi_total", "Contrapartidas pactuadas", CONTRAPARTIDA, metrica("Valor", "SUM(valor)"), "terreno, infraestrutura e complementares", formato="$,.0f"),
    kpi("p_kpi_municipio", "Pactuado pelos municípios", CONTRAPARTIDA, metrica("Valor", "SUM(valor)"), "todas as modalidades", formato="$,.0f", filtros=["ente = 'Município'"]),
    kpi("p_kpi_estado", "Pactuado pelos estados", CONTRAPARTIDA, metrica("Valor", "SUM(valor)"), "todas as modalidades", formato="$,.0f", filtros=["ente = 'Estado'"]),
    kpi("p_kpi_com", "Empreendimentos com contrapartida", CONTRAPARTIDA, metrica("Empreendimentos", "COUNT(DISTINCT apf)"), "com algum valor pactuado"),
    barras("p_ente_tipo", "Contrapartidas por ente e tipo", CONTRAPARTIDA, "ente", [metrica("Valor pactuado", "SUM(valor)")], grupo=["tipo"],
           formato="$,.0f", ordenar_por_valor=True, eixo_titulo="Ente", y_titulo="R$"),
    tabela_agregada("p_uf", "Contrapartida em relação ao valor contratado, por UF", EMPREENDIMENTO, ["uf"], [
        metrica("Empreendimentos", "COUNT(*)"),
        metrica("Valor contratado (R$)", "SUM(valor_contratado)"),
        metrica("Contrapartidas (R$)", "SUM(vr_total_contrapartidas)"),
        metrica("Contrapartida ÷ contratado", "SUM(vr_total_contrapartidas) / NULLIF(SUM(valor_contratado), 0)"),
        pct("% com terreno doado", "ic_terreno_doado"),
    ], limite=30),

    # 7. Panorama das contratações
    barras("g_uf", "Unidades contratadas por UF e situação da obra", EMPREENDIMENTO, "uf", [metrica("UH", "SUM(qt_uh)")], grupo=["situacao_obra"],
           ordenar_por_valor=True, eixo_titulo="UF", y_titulo="Unidades"),
    barras("g_ano", "Empreendimentos e valor contratado por ano", EMPREENDIMENTO, "ano_contratacao",
           [metrica("Valor contratado (R$)", "SUM(valor_contratado)")], formato="$,.0f", eixo_titulo="Ano de contratação", y_titulo="R$"),
    tabela("g_ficha", "Panorama das contratações por empreendimento", EMPREENDIMENTO,
           [coluna("APF", "apf"), coluna("Empreendimento", "nome_empreendimento"), coluna("Município", "municipio || '/' || uf"),
            coluna("Portaria", "portaria"), coluna("Demanda", "tipo_demanda"), coluna("Edificação", "tipo_edificacao"),
            coluna("UH", "qt_uh"), coluna("Valor contratado (R$)", "valor_contratado"), coluna("R$/UH", "valor_por_uh"),
            coluna("Desembolsado (R$)", "valor_desembolsado"), coluna("% físico", "percentual_execucao_fisica"),
            coluna("Situação", "situacao_obra"), coluna("Contrapartidas (R$)", "vr_total_contrapartidas"),
            coluna("Terreno doado", "CASE WHEN ic_terreno_doado THEN 'Sim' ELSE 'Não' END"),
            coluna("Elevador", "CASE ic_previsao_elevador WHEN true THEN 'Sim' WHEN false THEN 'Não' ELSE 'Não informado' END"),
            coluna("Qualificação do terreno", "qualificacao_terreno"), coluna("Famílias com contrato", "qt_contratos_familias")],
           [["valor_contratado", False]]),

    # O que falta
    tabela("cobertura", "O que os dados respondem (e o que falta)", COBERTURA,
           [coluna("Bloco", "bloco"), coluna("Pergunta", "pergunta"), coluna("Situação", "situacao_dado_rotulo"),
            coluna("O que o painel mostra", "resposta_disponivel"), coluna("Fonte necessária", "fonte_necessaria"),
            coluna("Limitação", "limitacao")],
           [["ordem", True]], limite=100),
    pizza("cobertura_resumo", "Perguntas por situação do dado", COBERTURA, "situacao_dado_rotulo", metrica("Perguntas", "COUNT(*)")),
]

COMECE = """# Perguntas do MCID sobre o FAR

Este painel responde, com dados, as perguntas levantadas com o Ministério das Cidades sobre o **MCMV FAR**: quem são as famílias, que empreendimentos são esses, como andam as obras e as contrapartidas, e onde está a inadimplência.

## Como ler
- Em cada aba, os **títulos de seção são as perguntas**.
- Ao lado de cada gráfico, o quadro **Como ler** explica o que ele mostra e o que ele não mostra.
- Os filtros de cada aba valem só para aquela aba.

## Duas gerações de contratos
- **Novo MCMV**: contratos das famílias assinados desde 2025 nos empreendimentos do Novo MCMV FAR (remessa MONT_CONT_PF_FAR da CAIXA). Ainda são poucos, porque a maior parte das obras não foi entregue.
- **Legado (2009–2023)**: carteira de contratos do FAR na CAIXA (INT039) e no Banco do Brasil (INT021), com mais de 1 milhão de famílias.

Empreendimentos, obras e contrapartidas são do **Novo MCMV FAR** (cadastro e monitoramento MONIT)."""

ONDE_VER = """### O que cada pergunta já consegue responder
A última aba, **O que falta**, lista todas as perguntas da oficina e a situação do dado:
- **Respondível**: os dados respondem a pergunta.
- **Parcial**: respondem em parte, com a limitação descrita.
- **Lacuna de fonte**: a informação não chega ao banco; a coluna *Fonte necessária* diz o que pedir.

Dados pessoais (CPF, NIS, nome, endereço, data de nascimento) são mascarados antes de chegar aqui; o painel só mostra contagens e taxas."""

COMO = {
    "b_ano": """### Como ler
Cada barra é um ano de assinatura do contrato da família, separado por geração. O legado concentra os contratos de 2010 a 2020; o Novo MCMV começa a assinar com as famílias em 2025, à medida que as obras são entregues.""",
    "b_uf": """### Como ler
Contratos das famílias por UF do imóvel. Use o filtro de **geração** para ver só o Novo MCMV ou só o legado.

A tabela abaixo detalha por município, com renda média e percentual de titulares mulheres.""",
    "b_modalidade": """### Como ler
No **legado**, a modalidade é o tipo de contrato informado pela CAIXA/BB (subsídio mensal na prestação, FAR/FGTS, calamidade...).

No **Novo MCMV**, é o tipo de demanda do empreendimento (cadastro habitacional, risco, calamidade).

⚠️ O BB não informa a modalidade em parte da carteira (aparece como *Não informada*).""",
    "b_sexo": """### Como ler
Proporção de titulares por sexo em cada geração. A grande maioria dos contratos tem mulher como titular, coerente com a preferência legal à mulher no programa.

⚠️ O BB não informa sexo.""",
    "b_estado_civil": """### Como ler
Estado civil do titular na assinatura. Abaixo, a proporção de contratos com **coobrigado** (segundo responsável pelo contrato).

O BB não informa estado civil nem coobrigado.""",
    "b_idoso": """### Como ler
Titulares idosos no Novo MCMV, por UF.

⚠️ **Faixa etária não está disponível**: a data de nascimento é mascarada antes de chegar ao banco. O indicador de idoso vem pronto da remessa do Novo MCMV. **PCD, situação de rua, tamanho da família e família com idoso** existem no layout, mas chegam vazios.""",
    "b_renda": """### Como ler
Distribuição dos contratos por faixa de renda familiar comprovada (valores nominais da época da assinatura).

*Sem renda comprovada* é renda zero declarada, comum em famílias atendidas por transferência de renda. O BB não informa renda.""",
    "c_instrumento": """### Como ler
No Novo MCMV, cada contrato é **doação** (sem prestação) ou **alienação fiduciária** (com prestação subsidiada). O gráfico abaixo mostra as assinaturas mês a mês.

⚠️ O legado não informa o tipo de instrumento.""",
    "c_situacao": """### Como ler
Situação de cada contrato, por agente financeiro (100% = todos os contratos do agente). A tabela abaixo dá as taxas de ativos, liquidados, distratados, cancelados e sub-rogados.

No legado CAIXA, **liquidado** é contrato encerrado (saldo zero) e **sub-rogado** é contrato FAR/FGTS transferido ao FAR. A liquidação por sinistro não separa MIP de DFI.""",
    "i_agente": """### Como ler
**Inadimplente** = contrato **ativo** com prestação vencida há mais de 90 dias (regra do layout da CAIXA). A taxa é sobre os contratos ativos, porque liquidados e distratados não pagam mais prestação.

⚠️ No legado, 88 mil contratos já liquidados ainda registram atraso; eles não entram na taxa.""",
    "i_renda": """### Como ler
Taxa de inadimplência por faixa de renda. Responde se famílias de menor renda atrasam mais.

É uma comparação descritiva: não controla município, ano de assinatura nem modalidade. Use os filtros para isolar esses fatores.""",
    "i_uf": """### Como ler
Taxa de inadimplência dos contratos ativos por UF. A tabela abaixo lista os municípios com maior taxa, entre os que têm ao menos 100 contratos ativos (para não destacar taxas de poucos contratos).

**Retomadas**: o campo existe só no Novo MCMV, ainda sem nenhum caso. O legado não informa retomada.""",
    "n_tipologia": """### Como ler
Natureza dos empreendimentos do Novo MCMV FAR: tipo de edificação (apartamento, casa, casa sobreposta) e se é **condomínio** ou **loteamento**. Abaixo, o destino do atendimento pelo tipo de demanda, em unidades.

Todos os empreendimentos contratados estão na linha **aquisição/produção para alienação**.""",
    "n_origem": """### Como ler
A **originação** separa empreendimento **novo** de **requalificado**; as cores mostram o tipo de demanda declarado (cadastro habitacional, obra pública, risco, calamidade, agravamento).

⚠️ A remessa não informa **quem indicou** a demanda (ente, movimento social, defesa civil). 84 empreendimentos não têm proposta no consolidado e aparecem como *Não informada*.""",
    "n_mudou": """### Como ler
Empreendimentos em que a demanda declarada **mudou** entre o enquadramento da proposta, a contratação e o monitoramento — por exemplo, enquadrado como **calamidade** e contratado como **cadastro habitacional**.

Mostra a mudança de destino declarada; não acompanha quem de fato recebeu a unidade.""",
    "n_entorno": """### Como ler
Percentual dos empreendimentos cujo **entorno** tem cada serviço, segundo o que o proponente **declarou** na proposta. Não é medição independente.

Transporte não é informado. Cruzar com INEP, CNES e CadSUAS georreferenciados é uma das lacunas listadas em *O que falta*.""",
    "e_alerta_uf": """### Como ler
Cada empreendimento recebe um **alerta**, nesta ordem de prioridade:
1. Concluída
2. Paralisada (informada pelo agente)
3. Atrasada (informada pelo agente)
4. **Sinal de paralisação**: obra iniciada com o percentual físico parado há 3 meses ou mais
5. **Ritmo abaixo do previsto**: 10 p.p. ou mais abaixo do cronograma
6. Não iniciada / Em ritmo

Os itens 4 e 5 são calculados aqui e antecipam o que a origem só registra quando a situação muda.""",
    "e_mensal": """### Como ler
Quantos empreendimentos estavam em cada situação em cada mês, segundo o MONIT de obra. Mostra se o volume de obras atrasadas ou paralisadas está crescendo.

A tabela abaixo lista as obras com sinal de atraso ou paralisação, as paradas há mais tempo primeiro.""",
    "p_ente_tipo": """### Como ler
Contrapartidas **pactuadas** pelos entes em cada empreendimento, por ente (município, estado) e tipo (terreno, infraestrutura, complementar).

⚠️ A origem traz o valor **pactuado**, não o **executado**: não dá para saber se o ente já entregou. Equipamentos públicos, aporte/suplementação e contrapartidas na retomada chegam zerados.""",
    "p_uf": """### Como ler
Quanto as contrapartidas representam do valor contratado em cada UF, e em quantos empreendimentos o terreno foi doado pelo ente.""",
    "g_uf": """### Como ler
Panorama do Novo MCMV FAR: unidades contratadas por UF, coloridas pela situação da obra, e valor contratado por ano.

A tabela abaixo é a ficha de cada empreendimento: portaria, demanda, tipologia, valor por unidade, execução, contrapartidas, terreno doado, elevador e qualificação do terreno.

⚠️ A previsão de elevador só está preenchida em parte dos empreendimentos; não há estimativa de custo de condomínio.""",
    "cobertura_resumo": """### Como ler
Todas as perguntas da oficina, com a situação do dado hoje. A coluna **Fonte necessária** diz o que pedir para fechar as lacunas: raça/cor, faixa etária, PCD e situação de rua preenchidos, déficit habitacional municipal, Censo 2022, estoque de imóveis retomados e execução das contrapartidas.""",
}


ABAS = [
    ("Comece aqui", [
        ("md", COMECE, 90),
        ("charts", ["kpi_familias", "kpi_familias_novo", "kpi_empreendimentos", "kpi_inadimplencia"]),
        ("header", "O que cada pergunta já consegue responder"),
        ("md", ONDE_VER, 40),
    ]),
    ("1. Beneficiário", [
        ("charts", ["b_kpi_mulher", "b_kpi_coobrigado", "b_kpi_idoso", "b_kpi_renda"]),
        ("header", "1. Quantas famílias foram atendidas, por ano, modalidade, UF e município?"),
        ("pair", "b_ano"), ("pair", "b_uf"), ("pair", "b_modalidade"), ("chart", "b_municipios", 70),
        ("header", "2. Quem são os titulares? Sexo, estado civil, coobrigado e idade"),
        ("pair", "b_sexo"), ("pair", "b_estado_civil"), ("chart", "b_coobrigado", 45), ("pair", "b_idoso"),
        ("header", "3. Qual a renda das famílias?"),
        ("pair", "b_renda"),
    ]),
    ("2. Contratação e situação", [
        ("header", "4. Quantos contratos são doação e quantos alienação fiduciária? Quantos foram assinados por mês?"),
        ("pair", "c_instrumento"), ("chart", "c_mes", 50),
        ("header", "5. Quantas unidades estão ativas, liquidadas, distratadas, canceladas ou liquidadas por MIP/DFI?"),
        ("pair", "c_situacao"), ("chart", "c_taxas", 40),
    ]),
    ("3. Inadimplência e território", [
        ("charts", ["i_kpi_ativos", "i_kpi_inadimplentes", "i_kpi_taxa", "i_kpi_valor"]),
        ("header", "6. Quantos contratos estão inadimplentes e qual a taxa?"),
        ("pair", "i_agente"),
        ("header", "7. Famílias de menor renda são mais inadimplentes?"),
        ("pair", "i_renda"),
        ("header", "8. Quais municípios concentram contratos e têm maior inadimplência? Onde há retomadas?"),
        ("pair", "i_uf"), ("chart", "i_municipios", 70),
    ]),
    ("4. Natureza e destino", [
        ("charts", ["n_kpi_uh", "n_kpi_terreno", "n_kpi_valor_uh", "n_kpi_mudou"]),
        ("header", "9. Qual a natureza do empreendimento e qual o destino do atendimento?"),
        ("pair", "n_tipologia"), ("chart", "n_demanda", 50),
        ("header", "10. Qual a origem da contratação e de onde vem a indicação da demanda?"),
        ("pair", "n_origem"),
        ("header", "11. Obras destinadas a calamidade mudam de público depois?"),
        ("pair", "n_mudou"),
        ("header", "12. Como é o entorno dos empreendimentos?"),
        ("pair", "n_entorno"),
    ]),
    ("5. Execução da obra", [
        ("charts", ["e_kpi_ritmo", "e_kpi_atrasada", "e_kpi_sinal", "e_kpi_paralisada"]),
        ("header", "13. Qual o andamento da execução da obra, com indicativo de atraso e paralisação?"),
        ("pair", "e_alerta_uf"),
        ("header", "14. Como saber da paralisação antes de a obra parar?"),
        ("pair", "e_mensal"), ("chart", "e_lista", 70),
    ]),
    ("6. Contrapartidas", [
        ("charts", ["p_kpi_total", "p_kpi_municipio", "p_kpi_estado", "p_kpi_com"]),
        ("header", "15. Como está a pactuação das contrapartidas e compromissos do ente público?"),
        ("pair", "p_ente_tipo"), ("pair", "p_uf"),
    ]),
    ("7. Panorama das contratações", [
        ("header", "16. Panorama geral: UF, município, situação, valores, portaria, demanda, execução e contrapartidas"),
        ("pair", "g_uf"), ("chart", "g_ano", 45), ("chart", "g_ficha", 80),
    ]),
    ("O que falta", [
        ("header", "O que os dados respondem (e o que falta)"),
        ("pair", "cobertura_resumo"), ("chart", "cobertura", 90),
    ]),
]

# Filtros nativos: (nome, coluna, datasets, índice da aba).
FILTROS = [
    *[(n, c, [CONTRATO, MUNICIPIO], aba) for aba in (1, 2, 3) for n, c in (("Geração", "geracao"), ("UF", "uf"))],
    *[("Agente financeiro", "agente_financeiro", [CONTRATO], aba) for aba in (1, 2, 3)],
    *[(n, c, [EMPREENDIMENTO], aba) for aba in (4, 5, 7) for n, c in (("UF", "uf"), ("Tipo de demanda", "tipo_demanda"))],
    ("Empreendimento", "apf_municipio_empreendimento", [EMPREENDIMENTO], 7),
    ("Alerta de execução", "alerta_execucao", [EMPREENDIMENTO], 5),
    ("UF", "uf", [CONTRAPARTIDA, EMPREENDIMENTO], 6),
    ("Bloco", "bloco", [COBERTURA], 8),
]


def query_context(definicao: dict, dataset_id: int, params: dict) -> str:
    """Consulta salva junto do gráfico, para quem lê os dados pela API e não pela tela."""
    fd = definicao["params"]
    where = [f["sqlExpression"] for f in fd.get("adhoc_filters", [])]
    consulta = {"filters": [], "extras": {"where": " AND ".join(f"({w})" for w in where), "having": ""},
                "row_limit": fd.get("row_limit", 1000)}
    viz = definicao["viz_type"]
    if viz == "table" and fd.get("query_mode") == "raw":
        consulta["columns"] = fd["all_columns"]
        consulta["orderby"] = [json.loads(o) for o in fd.get("order_by_cols", [])]
    elif viz.startswith("echarts_timeseries"):
        eixo = fd["x_axis"]
        if fd.get("time_grain_sqla"):
            eixo = {"columnType": "BASE_AXIS", "sqlExpression": eixo, "label": eixo,
                    "expressionType": "SQL", "timeGrain": fd["time_grain_sqla"]}
        consulta["columns"] = [eixo, *fd.get("groupby", [])]
        consulta["metrics"] = fd["metrics"]
    else:
        consulta["columns"] = list(fd.get("groupby", []))
        consulta["metrics"] = fd.get("metrics") or [fd["metric"]]
    return json.dumps({"datasource": {"id": dataset_id, "type": "table"}, "force": False,
                       "queries": [consulta], "form_data": params,
                       "result_format": "json", "result_type": "full"})


def datasets(api: Superset, database_id: int) -> dict[str, int]:
    existentes = {(d.get("schema"), d.get("table_name")): d["id"] for d in api.list("dataset")}
    resultado = {}
    for nome in DATASETS:
        if (SCHEMA, nome) not in existentes:
            if nome in DATASETS_COMPARTILHADOS:
                raise SystemExit(f"Dataset {SCHEMA}.{nome} não existe; ele é criado pelo painel FAR.")
            existentes[(SCHEMA, nome)] = api.create(
                "dataset", {"database": database_id, "schema": SCHEMA, "table_name": nome}
            )["id"]
        resultado[nome] = existentes[(SCHEMA, nome)]
        # O dbt recria a tabela; sem refresh o Superset valida os gráficos contra colunas antigas.
        if nome not in DATASETS_COMPARTILHADOS and not api.dry_run:
            url = f"{api.base_url}/api/v1/dataset/{resultado[nome]}/refresh"
            resposta = api.session.put(url, timeout=30)
            if resposta.status_code == 405:
                resposta = api.session.post(url, timeout=30)
            resposta.raise_for_status()
    return resultado


def charts(api: Superset, ids_dataset: dict[str, int]) -> dict[str, int]:
    existentes = {c.get("slice_name"): c for c in api.list("chart")}
    resultado = {}
    for definicao in CHARTS:
        titulo = PREFIXO + definicao["title"]
        dataset_id = ids_dataset[definicao["dataset"]]
        params = {"datasource": f"{dataset_id}__table", "viz_type": definicao["viz_type"], **definicao["params"]}
        payload = {
            "slice_name": titulo,
            "viz_type": definicao["viz_type"],
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(params),
            "query_context": query_context(definicao, dataset_id, params),
        }
        atual = existentes.get(titulo)
        if atual is None:
            resultado[definicao["key"]] = api.create("chart", payload)["id"]
        else:
            resultado[definicao["key"]] = atual["id"]
            api.update("chart", atual["id"], payload)
    return resultado


def layout(ids: dict[str, int]) -> dict:
    tabs = "TABS-PERGUNTAS-FAR"
    estrutura = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": [tabs], "parents": [], "meta": {}},
        tabs: {"id": tabs, "type": "TABS", "children": [], "parents": ["ROOT_ID"], "meta": {}},
    }

    def no(id_, tipo, pais, meta, filhos=None):
        estrutura[id_] = {"id": id_, "type": tipo, "children": filhos or [], "parents": pais, "meta": meta}
        estrutura[pais[-1]]["children"].append(id_)

    for pagina, (nome, blocos) in enumerate(ABAS):
        tab = f"TAB-PERGUNTAS-FAR-{pagina}"
        no(tab, "TAB", ["ROOT_ID", tabs], {"text": nome, "defaultText": nome, "placeholder": nome})
        pais = ["ROOT_ID", tabs, tab]
        for linha, bloco in enumerate(blocos):
            tipo = bloco[0]
            sufixo = f"{pagina}-{linha}"
            if tipo == "header":
                no(f"HEADER-PERGUNTAS-FAR-{sufixo}", "HEADER", pais, {"text": bloco[1], "headerSize": "MEDIUM_HEADER", "background": "BACKGROUND_TRANSPARENT"})
                continue
            row = f"ROW-PERGUNTAS-FAR-{sufixo}"
            no(row, "ROW", pais, {"background": "BACKGROUND_TRANSPARENT"})
            filhos = pais + [row]
            if tipo == "md":
                no(f"MARKDOWN-PERGUNTAS-FAR-{sufixo}", "MARKDOWN", filhos, {"width": 12, "height": bloco[2], "code": bloco[1]})
            elif tipo == "charts":
                for posicao, chave in enumerate(bloco[1]):
                    no(f"CHART-PERGUNTAS-FAR-{sufixo}-{posicao}", "CHART", filhos,
                       {"chartId": ids[chave], "width": 12 // len(bloco[1]), "height": 22})
            elif tipo == "chart":
                no(f"CHART-PERGUNTAS-FAR-{sufixo}", "CHART", filhos, {"chartId": ids[bloco[1]], "width": 12, "height": bloco[2]})
            elif tipo == "dupla":
                for posicao, chave in enumerate(bloco[1:]):
                    no(f"CHART-PERGUNTAS-FAR-{sufixo}-{posicao}", "CHART", filhos, {"chartId": ids[chave], "width": 6, "height": 55})
            else:
                chave = bloco[1]
                no(f"MARKDOWN-PERGUNTAS-FAR-{sufixo}", "MARKDOWN", filhos, {"width": 4, "height": 55, "code": COMO[chave]})
                no(f"CHART-PERGUNTAS-FAR-{sufixo}", "CHART", filhos, {"chartId": ids[chave], "width": 8, "height": 55})
    return estrutura


def filtros_nativos(ids_dataset: dict[str, int]) -> list[dict]:
    return [
        {
            "id": f"NATIVE_FILTER-PERGUNTAS-FAR-{indice}", "name": nome,
            "filterType": "filter_select", "type": "NATIVE_FILTER",
            "targets": [{"datasetId": ids_dataset[d], "column": {"name": coluna_}} for d in alvos],
            "defaultDataMask": {"extraFormData": {}, "filterState": {"value": None}, "ownState": {}},
            "controlValues": {"multiSelect": True, "enableEmptyFilter": False, "defaultToFirstItem": False,
                              "searchAllOptions": True, "inverseSelection": False},
            "required": False, "cascadeParentIds": [],
            "scope": {"rootPath": [f"TAB-PERGUNTAS-FAR-{aba}"], "excluded": []},
        }
        for indice, (nome, coluna_, alvos, aba) in enumerate(FILTROS)
    ]


def dashboard(api: Superset, ids_chart: dict[str, int], ids_dataset: dict[str, int]) -> None:
    payload = {
        "dashboard_title": DASHBOARD_TITLE,
        "slug": DASHBOARD_SLUG,
        "published": True,
        "position_json": json.dumps(layout(ids_chart)),
        "json_metadata": json.dumps({
            "native_filter_configuration": filtros_nativos(ids_dataset),
            "show_native_filters": True, "refresh_frequency": 0,
            "cross_filters_enabled": False, "timed_refresh_immune_slices": [],
        }),
    }
    dashboard_id = api.dashboard_id(DASHBOARD_SLUG)
    if dashboard_id:
        api.update("dashboard", dashboard_id, payload)
    else:
        dashboard_id = api.create("dashboard", payload)["id"]
    if api.dry_run:
        return
    # O position_json não associa o gráfico ao painel; sem a associação ele abre vazio.
    for chart_id in ids_chart.values():
        atual = api.session.get(f"{api.base_url}/api/v1/chart/{chart_id}", timeout=30).json()["result"]
        destinos = {d["id"] for d in atual.get("dashboards") or []}
        if dashboard_id not in destinos:
            api.update("chart", chart_id, {"dashboards": sorted(destinos | {dashboard_id})})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    load_dotenv("local.env", override=False)
    load_dotenv(".env", override=False)
    api = Superset(
        env("SUPERSET_URL", "SUPERSET_HOST_PORT"),
        env("SUPERSET_USERNAME", "SUPERSET_USER"),
        env("SUPERSET_PASSWORD"),
        args.dry_run,
    )
    ids_dataset = datasets(api, get_or_create_database(api))
    ids_chart = charts(api, ids_dataset)
    dashboard(api, ids_chart, ids_dataset)
    print(f"Concluído: {len(ids_dataset)} datasets, {len(ids_chart)} gráficos e o painel /superset/dashboard/{DASHBOARD_SLUG}/.")


if __name__ == "__main__":
    main()
