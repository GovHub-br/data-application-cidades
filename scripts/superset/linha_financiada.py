#!/usr/bin/env python3
"""Provisiona datasets, gráficos e dashboard da Linha Financiada no Superset.

O script é idempotente e não contém credenciais. Use ``--dry-run`` enquanto
as Golds ainda não estiverem materializadas no PostgreSQL.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bootstrap_conjuntura import (  # noqa: E402
    Superset,
    env,
    get_or_create_database,
)

SCHEMA = "ouro"
DASHBOARD_TITLE = "Linha Financiada — Execução e Monitoramento"
DASHBOARD_SLUG = "linha-financiada"

DATASETS = [
    "ouro_linha_financiada_resumo_mensal",
    "ouro_linha_financiada_relatorio_semanal",
    "ouro_linha_financiada_mapa_execucao",
    "ouro_linha_financiada_empreendimentos",
    "ouro_linha_financiada_contrapartidas",
    "ouro_linha_financiada_execucao_orcamentaria",
    "ouro_linha_financiada_features_preditivas",
    "ouro_linha_financiada_base_pf_fundo_social",
    "ouro_linha_financiada_cobertura_perguntas",
    "ouro_linha_financiada_mapa_uf",
    "ouro_linha_financiada_projecao_anual",
    "ouro_linha_financiada_ranking_territorial",
]

# O Superset não cria métricas agregadas automaticamente ao registrar uma
# tabela. Declaramos as métricas pelo mesmo nome usado pelos gráficos, para que
# o dashboard seja reproduzível em qualquer instância.
METRICAS = {
    "ouro_linha_financiada_resumo_mensal": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__quantidade_empreendimentos", "SUM(quantidade_empreendimentos)"),
        ("sum__valor_financiamento", "SUM(valor_financiamento)"),
        ("sum__valor_descontos", "SUM(valor_desconto_fgts + valor_desconto_ogu)"),
    ],
    "ouro_linha_financiada_mapa_execucao": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__valor_financiamento", "SUM(valor_financiamento)"),
    ],
    "ouro_linha_financiada_empreendimentos": [
        ("count__empreendimentos", "COUNT(DISTINCT codigo_empreendimento)"),
        ("sum__quantidade_empreendimentos", "SUM(quantidade_unidades)"),
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__valor_investimento_pj", "SUM(valor_investimento_pj)"),
        ("sum__valor_contratado_pj", "SUM(valor_contratado_pj)"),
        ("sum__valor_financiamento", "SUM(valor_financiamento)"),
        ("sum__valor_descontos", "SUM(valor_descontos)"),
        ("avg__percentual_obra", "AVG(percentual_obra)"),
    ],
    "ouro_linha_financiada_contrapartidas": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__valor_contrapartida", "SUM(valor_contrapartida)"),
    ],
    "ouro_linha_financiada_features_preditivas": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__media_movel_contratos_3m", "SUM(media_movel_contratos_3m)"),
    ],
    "ouro_linha_financiada_execucao_orcamentaria": [
        ("sum__orcamento_atualizado", "SUM(orcamento_atualizado)"),
        ("sum__pagamentos_totais", "SUM(pagamentos_totais)"),
    ],
    "ouro_linha_financiada_mapa_uf": [("sum__quantidade_contratos", "SUM(quantidade_contratos)")],
    "ouro_linha_financiada_projecao_anual": [
        ("sum__quantidade_contratos_projetada_ano", "SUM(quantidade_contratos_projetada_ano)"),
        ("sum__valor_financiamento_projetado_ano", "SUM(valor_financiamento_projetado_ano)"),
    ],
    "ouro_linha_financiada_ranking_territorial": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__valor_financiamento", "SUM(valor_financiamento)"),
    ],
}

CHARTS = [
    {
        "key": "contratos_mes",
        "title": "Contratos por mês e fonte",
        "dataset": "ouro_linha_financiada_resumo_mensal",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "competencia_contratacao",
            "metrics": ["sum__quantidade_contratos"],
            "groupby": ["fonte_recurso", "segmento_linha_financiada"],
            "time_grain_sqla": "P1M",
            "row_limit": 10000,
        },
    },
    {
        "key": "valor_mes",
        "title": "Valor financiado por mês",
        "dataset": "ouro_linha_financiada_resumo_mensal",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "competencia_contratacao",
            "metrics": ["sum__valor_financiamento"],
            "groupby": ["fonte_recurso"],
            "time_grain_sqla": "P1M",
            "row_limit": 10000,
        },
    },
    {
        "key": "semanal",
        "title": "Relatório semanal FGTS e Fundo Social",
        "dataset": "ouro_linha_financiada_relatorio_semanal",
        "viz_type": "table",
        "params": {
            "query_mode": "raw",
            "all_columns": [
                "semana_referencia", "fonte_recurso", "segmento_linha_financiada",
                "faixa_codigo", "modalidade", "tipo_imovel", "quantidade_contratos",
                "valor_financiamento", "valor_descontos", "valor_contrapartida_informada",
            ],
            "order_by_cols": ["[\"semana_referencia\", false]"],
            "row_limit": 1000,
        },
    },
    {
        "key": "municipios",
        "title": "Execução por município",
        "dataset": "ouro_linha_financiada_mapa_execucao",
        "viz_type": "table",
        "params": {
            "query_mode": "aggregate",
            "groupby": ["uf", "municipio", "segmento_linha_financiada"],
            "metrics": ["sum__quantidade_contratos", "sum__valor_financiamento"],
            "row_limit": 10000,
        },
    },
    {
        "key": "empreendimentos",
        "title": "Empreendimentos para eventos",
        "dataset": "ouro_linha_financiada_empreendimentos",
        "viz_type": "table",
        "params": {
            "query_mode": "raw",
            "all_columns": [
                "codigo_empreendimento", "nome_empreendimento", "uf", "municipio",
                "quantidade_unidades", "quantidade_unidades_concluidas",
                "quantidade_unidades_entregues", "percentual_obra", "competencia_posicao",
            ],
            "row_limit": 5000,
        },
    },
    {
        "key": "contrapartidas",
        "title": "Cobertura das contrapartidas",
        "dataset": "ouro_linha_financiada_contrapartidas",
        "viz_type": "table",
        "params": {
            "query_mode": "aggregate",
            "groupby": ["fonte_recurso", "tipo_contrapartida", "ic_valor_informado"],
            "metrics": ["sum__quantidade_contratos", "sum__valor_contrapartida"],
            "row_limit": 1000,
        },
    },
    {
        "key": "orcamento",
        "title": "Orçamento oneroso, descontos e execução",
        "dataset": "ouro_linha_financiada_execucao_orcamentaria",
        "viz_type": "table",
        "params": {
            "query_mode": "raw",
            "all_columns": [
                "ano", "fonte_recurso", "programa", "agrupamento", "unidade_gestora",
                "orcamento_atualizado", "despesas_empenhadas", "despesas_pagas",
                "restos_a_pagar_pagos", "pagamentos_totais", "percentual_execucao_financeira",
            ],
            "row_limit": 5000,
        },
    },
    {
        "key": "previsao",
        "title": "Série preparada para análise preditiva",
        "dataset": "ouro_linha_financiada_features_preditivas",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "competencia_contratacao",
            "metrics": ["sum__quantidade_contratos", "sum__media_movel_contratos_3m"],
            "groupby": ["fonte_recurso", "segmento_linha_financiada"],
            "time_grain_sqla": "P1M",
            "row_limit": 10000,
        },
    },
    {
        "key": "cobertura",
        "title": "Cobertura das perguntas da oficina",
        "dataset": "ouro_linha_financiada_cobertura_perguntas",
        "viz_type": "table",
        "params": {
            "query_mode": "raw",
            "all_columns": ["pergunta_produto", "situacao_resposta", "justificativa"],
            "row_limit": 100,
        },
    },
]

# Versão de validação: só usa tipos já disponíveis nesta instância (table,
# ECharts temporal e mapa DeckGL). O dashboard simples permanece inalterado.
CHARTS_COMPLETO = CHARTS + [
    {"key": "mapa_brasil", "title": "Mapa do Brasil por UF", "dataset": "ouro_linha_financiada_mapa_uf", "viz_type": "country_map", "params": {"select_country": "brazil", "entity": "iso_3166_2", "metric": "sum__quantidade_contratos", "linear_color_scheme": "dark_blue"}},
    {"key": "pizza_linha", "title": "Distribuição por linha", "dataset": "ouro_linha_financiada_resumo_mensal", "viz_type": "pie", "params": {"groupby": ["segmento_linha_financiada"], "metric": "sum__quantidade_contratos", "donut": True, "row_limit": 30}},
    {"key": "pizza_fonte", "title": "Distribuição por fonte", "dataset": "ouro_linha_financiada_resumo_mensal", "viz_type": "pie", "params": {"groupby": ["fonte_recurso"], "metric": "sum__quantidade_contratos", "donut": True, "row_limit": 30}},
    {
        "key": "ranking_uf",
        "title": "Ranking de contratação por UF",
        "dataset": "ouro_linha_financiada_mapa_execucao",
        "viz_type": "table",
        "params": {"query_mode": "aggregate", "groupby": ["uf"], "metrics": ["sum__quantidade_contratos", "sum__valor_financiamento"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 27},
    },
    {
        "key": "contrapartidas_por_linha",
        "title": "Contrapartidas por linha e fonte",
        "dataset": "ouro_linha_financiada_contrapartidas",
        "viz_type": "table",
        "params": {"query_mode": "aggregate", "groupby": ["segmento_linha_financiada", "fonte_recurso", "tipo_contrapartida", "ic_valor_informado"], "metrics": ["sum__quantidade_contratos", "sum__valor_contrapartida"], "row_limit": 1000},
    },
    {
        "key": "orcamento_resumo",
        "title": "Orçamento e pagamentos por fonte",
        "dataset": "ouro_linha_financiada_execucao_orcamentaria",
        "viz_type": "table",
        "params": {"query_mode": "aggregate", "groupby": ["ano", "fonte_recurso"], "metrics": ["sum__orcamento_atualizado", "sum__pagamentos_totais"], "row_limit": 100},
    },
    {
        "key": "projecao_anual",
        "title": "Projeção anual de contratação e financiamento",
        "dataset": "ouro_linha_financiada_projecao_anual",
        "viz_type": "table",
        "params": {
            "query_mode": "raw",
            "all_columns": [
                "competencia_referencia", "fonte_recurso", "segmento_linha_financiada",
                "quantidade_contratos_realizada_ano", "quantidade_contratos_projetada_ano",
                "valor_financiamento_realizado_ano", "valor_financiamento_projetado_ano",
                "metodo_projecao",
            ],
            "order_by_cols": ['["fonte_recurso", true]'],
            "row_limit": 100,
        },
    },
    {
        "key": "ranking_territorial",
        "title": "Ranking territorial de municípios e UFs",
        "dataset": "ouro_linha_financiada_ranking_territorial",
        "viz_type": "table",
        "params": {
            "query_mode": "raw",
            "all_columns": [
                "ranking_municipio_contratos", "uf", "municipio", "fonte_recurso",
                "segmento_linha_financiada", "quantidade_contratos",
                "quantidade_empreendimentos", "valor_financiamento", "valor_descontos",
            ],
            "order_by_cols": ['["ranking_municipio_contratos", true]'],
            "row_limit": 100,
        },
    },
    {
        "key": "empreendimento_unidades",
        "title": "Registros AO1/PJ (não são empreendimentos físicos)",
        "dataset": "ouro_linha_financiada_empreendimentos",
        "viz_type": "big_number_total",
        "params": {"metric": "count__empreendimentos", "subheader": "códigos administrativos; requer qualificação"},
    },
    {
        "key": "empreendimento_desligamentos",
        "title": "Empreendimento — Contratos PF vinculados",
        "dataset": "ouro_linha_financiada_empreendimentos",
        "viz_type": "big_number_total",
        "params": {"metric": "sum__quantidade_contratos", "subheader": "contratos PF vinculados"},
    },
    {
        "key": "empreendimento_investimento",
        "title": "Empreendimento — Valor de investimento PJ",
        "dataset": "ouro_linha_financiada_empreendimentos",
        "viz_type": "big_number_total",
        "params": {"metric": "sum__valor_investimento_pj", "subheader": "valor de investimento"},
    },
    {
        "key": "empreendimento_execucao",
        "title": "Empreendimento — Execução física",
        "dataset": "ouro_linha_financiada_empreendimentos",
        "viz_type": "big_number_total",
        "params": {"metric": "avg__percentual_obra", "subheader": "% de obra executada"},
    },
    {
        "key": "empreendimento_situacao",
        "title": "Empreendimento — Situação de execução",
        "dataset": "ouro_linha_financiada_empreendimentos",
        "viz_type": "pie",
        "params": {"groupby": ["situacao_execucao"], "metric": "count__empreendimentos", "donut": True, "row_limit": 20},
    },
    {
        "key": "ficha_empreendimento",
        "title": "Ficha detalhada do empreendimento",
        "dataset": "ouro_linha_financiada_empreendimentos",
        "viz_type": "table",
        "params": {
            "query_mode": "raw",
            "all_columns": [
                "codigo_empreendimento", "nome_empreendimento", "uf_painel", "municipio_painel",
                "situacao_contrato_pj", "situacao_execucao", "competencia_posicao",
                "quantidade_unidades", "quantidade_contratos", "percentual_unidades_com_pf",
                "valor_investimento_pj", "valor_contratado_pj", "valor_financiamento", "valor_descontos",
                "percentual_obra", "entidade_pj", "data_inicio_raw", "data_termino_raw",
            ],
            "row_limit": 100,
        },
    },
]

# Painel novo: reproduz a leitura orientada a perguntas do painel
# ``mcid-perguntas``. Os números de apoio à produção PJ não são usados como
# contratação MCMV; a ficha usa exclusivamente o vínculo operação CCA/PF ↔ AO1.
CHARTS_PERGUNTAS = CHARTS_COMPLETO + [
    {"key": "q_kpi_contratos", "title": "Visão geral — contratos", "dataset": "ouro_linha_financiada_resumo_mensal", "viz_type": "big_number_total", "params": {"metric": "sum__quantidade_contratos", "subheader": "contratações PF e Fundo Social"}},
    {"key": "q_kpi_financiamento", "title": "Visão geral — valor financiado", "dataset": "ouro_linha_financiada_resumo_mensal", "viz_type": "big_number_total", "params": {"metric": "sum__valor_financiamento", "subheader": "valor contratado"}},
    {"key": "q_kpi_descontos", "title": "Visão geral — descontos", "dataset": "ouro_linha_financiada_resumo_mensal", "viz_type": "big_number_total", "params": {"metric": "sum__valor_descontos", "subheader": "FGTS e OGU identificados"}},
    {"key": "q_empreendimento_uh", "title": "Ficha — unidades financiadas", "dataset": "ouro_linha_financiada_empreendimentos", "viz_type": "big_number_total", "params": {"metric": "sum__quantidade_empreendimentos", "subheader": "unidades do empreendimento AO1"}},
    {"key": "q_empreendimento_pf", "title": "Ficha — contratos PF vinculados", "dataset": "ouro_linha_financiada_empreendimentos", "viz_type": "big_number_total", "params": {"metric": "sum__quantidade_contratos", "subheader": "vínculo por operação CCA ↔ código AO1"}},
    {"key": "q_empreendimento_financiamento", "title": "Ficha — valor financiado PF", "dataset": "ouro_linha_financiada_empreendimentos", "viz_type": "big_number_total", "params": {"metric": "sum__valor_financiamento", "subheader": "contratos PF vinculados"}},
    {"key": "q_empreendimento_execucao", "title": "Ficha — execução física", "dataset": "ouro_linha_financiada_empreendimentos", "viz_type": "big_number_total", "params": {"metric": "avg__percentual_obra", "subheader": "% informado na última posição"}},
]

# Recortes fixos para as abas de negócio. Cada gráfico recebe o filtro na
# própria consulta, evitando que o usuário tenha de aplicar manualmente a
# linha toda vez que abre o painel.
for sufixo, rotulo, coluna_frente in [
    ("cidades", "MCMV Cidades", "ic_mcmv_cidades"),
    ("classe_media", "Classe Média", "ic_classe_media"),
    ("pro_moradia", "Pró-Moradia", "ic_pro_moradia"),
]:
    for chave_base in ("contratos_mes", "valor_mes", "semanal"):
        base = next(item for item in CHARTS if item["key"] == chave_base)
        params = dict(base["params"])
        params["adhoc_filters"] = [{
            "clause": "WHERE", "expressionType": "SIMPLE", "subject": coluna_frente,
            "operator": "==", "comparator": True,
        }]
        CHARTS_COMPLETO.append({
            "key": f"{chave_base}_{sufixo}",
            "title": f"{base['title']} — {rotulo}",
            "dataset": base["dataset"], "viz_type": base["viz_type"], "params": params,
        })

# Indicadores de cada frente de negócio, no mesmo padrão de leitura rápida do
# painel FAR. São gráficos próprios, com recorte fixo, para que as abas sejam
# úteis mesmo sem nenhum filtro selecionado.
for sufixo, rotulo, coluna_frente in [
    ("cidades", "MCMV Cidades", "ic_mcmv_cidades"),
    ("classe_media", "Classe Média", "ic_classe_media"),
    ("pro_moradia", "Pró-Moradia", "ic_pro_moradia"),
]:
    filtro_frente = [{
        "clause": "WHERE", "expressionType": "SIMPLE", "subject": coluna_frente,
        "operator": "==", "comparator": True,
    }]
    for indicador, titulo, metrica, subtitulo in [
        ("contratos", "Contratos", "sum__quantidade_contratos", "contratações"),
        ("empreendimentos", "Empreendimentos", "sum__quantidade_empreendimentos", "empreendimentos vinculados"),
        ("financiamento", "Valor financiado", "sum__valor_financiamento", "financiamento contratado"),
        ("descontos", "Descontos", "sum__valor_descontos", "descontos identificados"),
    ]:
        CHARTS_COMPLETO.append({
            "key": f"frente_{indicador}_{sufixo}",
            "title": f"{titulo} — {rotulo}",
            "dataset": "ouro_linha_financiada_resumo_mensal",
            "viz_type": "big_number_total",
            "params": {"metric": metrica, "subheader": subtitulo, "adhoc_filters": filtro_frente},
        })


def datasets(api: Superset, database_id: int) -> dict[str, int]:
    existentes = {
        (item.get("schema"), item.get("table_name")): item["id"]
        for item in api.list("dataset")
    }
    resultado = {}
    for nome in DATASETS:
        chave = (SCHEMA, nome)
        if chave not in existentes:
            criado = api.create(
                "dataset", {"database": database_id, "schema": SCHEMA, "table_name": nome}
            )
            existentes[chave] = criado["id"]
        resultado[nome] = existentes[chave]

        # dbt pode recriar a tabela com novas colunas; sem refresh o Superset
        # continua validando charts contra o esquema antigo e os quebra.
        if not api.dry_run:
            atualizado = api.session.put(
                f"{api.base_url}/api/v1/dataset/{resultado[nome]}/refresh",
                timeout=30,
            )
            if atualizado.status_code == 405:
                atualizado = api.session.post(
                    f"{api.base_url}/api/v1/dataset/{resultado[nome]}/refresh",
                    timeout=30,
                )
            atualizado.raise_for_status()
    for nome, metricas in METRICAS.items():
        detalhe = api.session.get(
            f"{api.base_url}/api/v1/dataset/{resultado[nome]}", timeout=30
        )
        detalhe.raise_for_status()
        atuais = detalhe.json()["result"].get("metrics", [])
        esperadas = {nome_metrica for nome_metrica, _ in metricas}
        if {item["metric_name"] for item in atuais} == esperadas:
            continue
        # A API substitui a coleção de métricas no PUT. Antes de enviá-la,
        # removemos as métricas atuais pelo endpoint documentado, pois a API
        # recusa nomes duplicados na mesma atualização.
        if not api.dry_run:
            for metrica in atuais:
                resposta = api.session.delete(
                    f"{api.base_url}/api/v1/dataset/{resultado[nome]}/metric/{metrica['id']}",
                    timeout=30,
                )
                resposta.raise_for_status()
        payloads = []
        for nome_metrica, expressao in metricas:
            payloads.append({
                "metric_name": nome_metrica,
                "verbose_name": nome_metrica.replace("sum__", "Soma de ").replace("_", " "),
                "expression": expressao,
                "metric_type": "sum",
            })
        api.update("dataset", resultado[nome], {"metrics": payloads})
    return resultado


def charts(api: Superset, ids_dataset: dict[str, int]) -> dict[str, int]:
    existentes = {item.get("slice_name"): item for item in api.list("chart")}
    resultado = {}
    for definicao in CHARTS:
        titulo = f"Linha Financiada | {definicao['title']}"
        dataset_id = ids_dataset[definicao["dataset"]]
        params = {
            "datasource": f"{dataset_id}__table",
            "viz_type": definicao["viz_type"],
            **definicao["params"],
        }
        payload = {
            "slice_name": titulo,
            "viz_type": definicao["viz_type"],
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(params),
        }
        atual = existentes.get(titulo)
        if atual is None:
            resultado[definicao["key"]] = api.create("chart", payload)["id"]
        else:
            resultado[definicao["key"]] = atual["id"]
            # Atualiza também os parâmetros: o dashboard deve corrigir charts
            # existentes quando a definição evoluir, não apenas reapontá-los.
            api.update("chart", atual["id"], payload)
    return resultado


def layout(ids_chart: dict[str, int]) -> str:
    estrutura = {
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"], "parents": [], "meta": {}},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": [], "parents": ["ROOT_ID"], "meta": {}},
    }
    for indice, definicao in enumerate(CHARTS):
        chart_id = ids_chart[definicao["key"]]
        row_id, node_id = f"ROW-{indice:02d}", f"CHART-{chart_id}"
        estrutura["GRID_ID"]["children"].append(row_id)
        estrutura[row_id] = {"id": row_id, "type": "ROW", "children": [node_id], "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        estrutura[node_id] = {"id": node_id, "type": "CHART", "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id], "meta": {"chartId": chart_id, "width": 12, "height": 50, "index": f"{indice:03d}"}}
    return json.dumps(estrutura)


def dashboard(api: Superset, ids_chart: dict[str, int]) -> None:
    payload = {
        "dashboard_title": DASHBOARD_TITLE,
        "slug": DASHBOARD_SLUG,
        "published": True,
        "position_json": layout(ids_chart),
        "json_metadata": json.dumps({"timed_refresh_immune_slices": [], "refresh_frequency": 0}),
    }
    atual = api.dashboard_id(DASHBOARD_SLUG)
    if atual:
        api.update("dashboard", atual, payload)
        dashboard_id = atual
    else:
        dashboard_id = api.create("dashboard", payload)["id"]

    # O position_json sozinho não dá permissão nem associa o chart ao painel.
    # A associação explícita evita layout que parece pronto, mas abre sem charts.
    if api.dry_run:
        return
    for chart_id in ids_chart.values():
        resposta = api.session.get(
            f"{api.base_url}/api/v1/chart/{chart_id}", timeout=30
        )
        resposta.raise_for_status()
        chart = resposta.json().get("result", {})
        atuais = {item["id"] for item in (chart.get("dashboards") or [])}
        if dashboard_id not in atuais:
            api.update("chart", chart_id, {"dashboards": sorted(atuais | {dashboard_id})})

    ligados = api.session.get(
        f"{api.base_url}/api/v1/dashboard/{DASHBOARD_SLUG}/charts", timeout=30
    )
    ligados.raise_for_status()
    ids_ligados = {item["id"] for item in ligados.json().get("result", [])}
    # Quando um painel encolhe, o Superset não remove automaticamente a
    # associação dos gráficos que deixaram de constar no layout.
    for chart_id in ids_ligados - set(ids_chart.values()):
        resposta = api.session.get(f"{api.base_url}/api/v1/chart/{chart_id}", timeout=30)
        resposta.raise_for_status()
        dashboards = {item["id"] for item in resposta.json().get("result", {}).get("dashboards", [])}
        api.update("chart", chart_id, {"dashboards": sorted(dashboards - {dashboard_id})})
    ligados = api.session.get(
        f"{api.base_url}/api/v1/dashboard/{DASHBOARD_SLUG}/charts", timeout=30
    )
    ligados.raise_for_status()
    quantidade = len(ligados.json().get("result", []))
    if quantidade != len(ids_chart):
        raise RuntimeError(
            f"Dashboard recebeu {quantidade} charts; esperados {len(ids_chart)}."
        )


def dashboard_completo(api: Superset, ids_chart: dict[str, int]) -> None:
    """Publica a prévia completa em abas, mantendo o painel simples intacto."""
    global DASHBOARD_TITLE, DASHBOARD_SLUG
    DASHBOARD_TITLE = "Linha Financiada — Painel Completo (validação)"
    DASHBOARD_SLUG = "linha-financiada-completo"
    dashboard(api, ids_chart)
    abas = [
        ("Total", [["pizza_linha", "pizza_fonte"], ["contratos_mes"], ["valor_mes"], ["semanal"], ["previsao"]]),
        ("MCMV Cidades", [["frente_contratos_cidades", "frente_empreendimentos_cidades", "frente_financiamento_cidades", "frente_descontos_cidades"], ["contratos_mes_cidades"], ["valor_mes_cidades"], ["semanal_cidades"]]),
        ("Classe Média", [["frente_contratos_classe_media", "frente_empreendimentos_classe_media", "frente_financiamento_classe_media", "frente_descontos_classe_media"], ["contratos_mes_classe_media"], ["valor_mes_classe_media"], ["semanal_classe_media"]]),
        ("Pró-Moradia", [["frente_contratos_pro_moradia", "frente_empreendimentos_pro_moradia", "frente_financiamento_pro_moradia", "frente_descontos_pro_moradia"], ["contratos_mes_pro_moradia"], ["valor_mes_pro_moradia"], ["semanal_pro_moradia"]]),
        # PJ é apoio à produção, não contratação MCMV. A chave PF (Operação)
        # ainda não concilia com APF PJ nesta carga; por isso não expomos a
        # ficha PJ como empreendimento/entrega até haver vínculo verificável.
        ("Estados", [["mapa_brasil"], ["ranking_uf", "municipios"], ["ranking_territorial"]]),
        ("Projeção", [["previsao"], ["projecao_anual"]]),
        ("Transversal", [["contrapartidas", "contrapartidas_por_linha"], ["orcamento", "orcamento_resumo"], ["cobertura"]]),
    ]
    tabs_id = "TABS-LINHA-FINANCIADA-COMPLETO"
    estrutura = {
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": [tabs_id], "parents": [], "meta": {}},
        tabs_id: {"id": tabs_id, "type": "TABS", "children": [], "parents": ["ROOT_ID"], "meta": {}},
    }
    for pagina, (titulo, linhas) in enumerate(abas):
        tab = f"TAB-LF-COMP-{pagina:02d}"
        estrutura[tabs_id]["children"].append(tab)
        estrutura[tab] = {"id": tab, "type": "TAB", "children": [], "parents": ["ROOT_ID", tabs_id], "meta": {"text": titulo, "defaultText": titulo, "placeholder": titulo}}
        for linha, chaves in enumerate(linhas):
            row = f"ROW-LF-COMP-{pagina:02d}-{linha:02d}"
            estrutura[tab]["children"].append(row)
            estrutura[row] = {"id": row, "type": "ROW", "children": [], "parents": ["ROOT_ID", tabs_id, tab], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            largura = 12 // len(chaves)
            for posicao, chave in enumerate(chaves):
                node = f"CHART-{ids_chart[chave]}"
                estrutura[row]["children"].append(node)
                estrutura[node] = {"id": node, "type": "CHART", "children": [], "parents": ["ROOT_ID", tabs_id, tab, row], "meta": {"chartId": ids_chart[chave], "width": largura, "height": 44 if len(chaves) == 1 else 36, "index": f"{pagina:02d}{linha:02d}{posicao:02d}"}}
    por_tabela = {
        item["table_name"]: item["id"] for item in api.list("dataset")
        if item.get("schema") == SCHEMA
    }
    def seletor(indice: int, nome: str, coluna: str, tabelas: list[str], aba: int) -> dict:
        # Cada seletor fica no escopo da própria aba, como no painel FAR.
        return {
            "id": f"NATIVE_FILTER-COMP-{indice}", "name": nome,
            "filterType": "filter_select", "type": "NATIVE_FILTER",
            "targets": [{"datasetId": por_tabela[t], "column": {"name": coluna}} for t in tabelas],
            "defaultDataMask": {},
            "controlValues": {"multiSelect": True, "enableEmptyFilter": True, "defaultToFirstItem": False, "searchAllOptions": True, "inverseSelection": False},
            "required": False,
            "scope": {"rootPath": ["ROOT_ID", tabs_id, f"TAB-LF-COMP-{aba:02d}"], "excluded": []},
            "cascadeParentIds": [],
        }

    def periodo(indice: int, aba: int, alvos_tempo: list[tuple[str, str]]) -> dict:
        return {
            "id": f"NATIVE_FILTER-COMP-{indice}", "name": "Período",
            "filterType": "filter_time", "type": "NATIVE_FILTER",
            "targets": [{"datasetId": por_tabela[tabela], "column": {"name": coluna}} for tabela, coluna in alvos_tempo],
            "defaultDataMask": {"extraFormData": {"time_range": "No filter"}, "filterState": {"value": "No filter"}},
            "controlValues": {"enableEmptyFilter": True, "defaultToFirstItem": False},
            "required": False,
            "scope": {"rootPath": ["ROOT_ID", tabs_id, f"TAB-LF-COMP-{aba:02d}"], "excluded": []},
            "cascadeParentIds": [],
        }

    resumo = "ouro_linha_financiada_resumo_mensal"
    semanal = "ouro_linha_financiada_relatorio_semanal"
    features = "ouro_linha_financiada_features_preditivas"
    mapa = "ouro_linha_financiada_mapa_execucao"
    ranking = "ouro_linha_financiada_ranking_territorial"
    filtros = [
        seletor(1, "Fonte de recurso", "fonte_recurso", [resumo, semanal, features], 0),
        seletor(2, "Linha financiada", "segmento_linha_financiada", [resumo, semanal, features], 0),
        seletor(3, "Faixa", "faixa_codigo", [resumo, semanal], 0),
        periodo(4, 0, [(resumo, "competencia_contratacao"), (semanal, "semana_referencia"), (features, "competencia_contratacao")]),
        seletor(5, "Faixa", "faixa_codigo", [resumo, semanal], 1),
        periodo(6, 1, [(resumo, "competencia_contratacao"), (semanal, "semana_referencia")]),
        seletor(7, "Faixa", "faixa_codigo", [resumo, semanal], 2),
        periodo(8, 2, [(resumo, "competencia_contratacao"), (semanal, "semana_referencia")]),
        seletor(9, "Faixa", "faixa_codigo", [resumo, semanal], 3),
        periodo(10, 3, [(resumo, "competencia_contratacao"), (semanal, "semana_referencia")]),
        seletor(11, "UF", "uf", [mapa, ranking], 4),
        seletor(12, "Fonte de recurso", "fonte_recurso", [mapa, ranking], 4),
        seletor(13, "Linha financiada", "segmento_linha_financiada", [mapa, ranking], 4),
        seletor(14, "Fonte de recurso", "fonte_recurso", [features, "ouro_linha_financiada_projecao_anual"], 5),
        seletor(15, "Linha financiada", "segmento_linha_financiada", [features, "ouro_linha_financiada_projecao_anual"], 5),
        periodo(16, 5, [(features, "competencia_contratacao"), ("ouro_linha_financiada_projecao_anual", "competencia_referencia")]),
    ]
    api.update("dashboard", api.dashboard_id(DASHBOARD_SLUG), {"position_json": json.dumps(estrutura), "json_metadata": json.dumps({"native_filter_configuration": filtros, "show_native_filters": True})})


def dashboard_perguntas(api: Superset, ids: dict[str, int]) -> None:
    """Painel no padrão pergunta + explicação à esquerda + gráfico à direita."""
    slug, titulo, tabs = "perguntas-linha-financiada", "Perguntas: Linha Financiada", "TABS-PERGUNTAS-LF"
    estrutura = {"ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": [tabs], "parents": [], "meta": {}}, tabs: {"id": tabs, "type": "TABS", "children": [], "parents": ["ROOT_ID"], "meta": {}}}
    paginas = [
        ("Comece aqui", [("charts", ["q_kpi_contratos", "q_kpi_financiamento", "q_kpi_descontos"]), ("heading", "## O que esta base integrada permite acompanhar"), ("pair", """### Como ler

Integra contratações PF do **FGTS**, operações do **Fundo Social** e recortes de **MCMV Cidades**, **Classe Média** e **Pró-Moradia**.

Valores são contratações e descontos informados. **Apoio à produção PJ não é contratação MCMV**; só compõe a ficha quando houver vínculo verificável com a operação PF.""", "pizza_linha")]),
        ("Previsão e contratação", [("heading", "## 1. É possível fazer análise preditiva da contratação mensal e da execução orçamentária?"), ("pair", """### Como ler

A série mensal reúne contratos e média móvel de três meses, preparada para modelagem preditiva. Ela permite estimar patamares futuros, mas não substitui modelo treinado.

Filtre fonte, linha e período nesta aba para comparar FGTS e Fundo Social.""", "previsao"), ("pair", """### Orçamento oneroso e descontos

Orçamento, empenho, pagamentos e restos a pagar são apresentados quando constam na fonte. Descontos FGTS/OGU dos contratos não são automaticamente pagamento orçamentário.""", "orcamento")]),
        ("Contrapartidas", [("heading", "## 2. Quais contrapartidas foram aportadas e quais contratos foram beneficiados?"), ("pair", """### Como ler

Mostra a cobertura do campo de contrapartida e separa valor informado de ausência de informação.

Ainda não existe base consolidada de contrapartidas além dos campos recebidos. Ente aportante, instrumento e contrato beneficiado dependem de estruturação com o agente financeiro.""", "contrapartidas_por_linha")]),
        ("Base integrada e semanal", [("heading", "## 3. Como garantir dados atualizados, unificados e disponibilizados de forma uniforme?"), ("pair", """### Como ler

O relatório semanal consolida fonte, linha, faixa, modalidade, tipo de imóvel, contratos, financiamento, descontos e contrapartida informada em uma estrutura comum.

As rotinas preservam origem e referência temporal para atualização automatizada e consumo em BI.""", "semanal"), ("pair", """### Contratação por mês

Cada ponto representa contratos por competência. Não some apoio à produção PJ como se fosse contratação de famílias.""", "contratos_mes")]),
        ("Estados e municípios", [("heading", "## 4. Onde estão as contratações? Mapa do Brasil e leitura territorial"), ("pair", """### Como ler

O mapa agrega contratos por UF. Use os filtros desta aba para recortar por UF, linha e fonte. Municípios sem localidade na fonte permanecem fora da territorialização.""", "mapa_brasil"), ("pair", """### Ranking territorial

Ordena municípios por contratações e mostra valor financiado, descontos e empreendimentos vinculados quando houver chave disponível.""", "ranking_territorial")]),
        ("Empreendimentos", [("heading", "## 5. Ficha do empreendimento — selecione um empreendimento para consultar suas características"), ("charts", ["q_empreendimento_uh", "q_empreendimento_pf", "q_empreendimento_financiamento", "q_empreendimento_execucao"]), ("pair", """### Como ler a ficha

Selecione o **empreendimento** e, se necessário, UF ou município. A ficha segue a lógica do relatório PJ: identificação, unidades, contratos PF vinculados, situação e posição de obra.

O vínculo usa **Operação CCA = código AO1**. Datas de término são previsão/posição de obra, não prova de entrega.""", "ficha_empreendimento"), ("pair", """### Situação da execução

Usa a última posição de obra disponível no canal AO1. Ausência de posição não significa obra parada; apenas ausência de status físico na carga.""", "empreendimento_situacao")]),
    ]
    for pagina, (nome, blocos) in enumerate(paginas):
        tab = f"TAB-PERGUNTAS-LF-{pagina}"
        estrutura[tabs]["children"].append(tab)
        estrutura[tab] = {"id": tab, "type": "TAB", "children": [], "parents": ["ROOT_ID", tabs], "meta": {"text": nome, "defaultText": nome, "placeholder": nome}}
        for linha, bloco in enumerate(blocos):
            row = f"ROW-PERGUNTAS-LF-{pagina}-{linha}"
            estrutura[tab]["children"].append(row)
            estrutura[row] = {"id": row, "type": "ROW", "children": [], "parents": ["ROOT_ID", tabs, tab], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            tipo, conteudo, *resto = bloco
            if tipo == "heading":
                node = f"MARKDOWN-PERGUNTAS-LF-{pagina}-{linha}"
                estrutura[row]["children"].append(node)
                estrutura[node] = {"id": node, "type": "MARKDOWN", "children": [], "parents": ["ROOT_ID", tabs, tab, row], "meta": {"width": 12, "height": 10, "code": conteudo}}
            elif tipo == "charts":
                for posicao, chave in enumerate(conteudo):
                    node = f"CHART-PERGUNTAS-LF-{ids[chave]}-{pagina}-{linha}"
                    estrutura[row]["children"].append(node)
                    estrutura[node] = {"id": node, "type": "CHART", "children": [], "parents": ["ROOT_ID", tabs, tab, row], "meta": {"chartId": ids[chave], "width": 12 // len(conteudo), "height": 28, "index": f"{pagina}{linha}{posicao}"}}
            else:
                markdown, chart = f"MARKDOWN-PERGUNTAS-LF-{pagina}-{linha}", f"CHART-PERGUNTAS-LF-{ids[resto[0]]}-{pagina}-{linha}"
                estrutura[row]["children"].extend([markdown, chart])
                estrutura[markdown] = {"id": markdown, "type": "MARKDOWN", "children": [], "parents": ["ROOT_ID", tabs, tab, row], "meta": {"width": 4, "height": 50, "code": conteudo}}
                estrutura[chart] = {"id": chart, "type": "CHART", "children": [], "parents": ["ROOT_ID", tabs, tab, row], "meta": {"chartId": ids[resto[0]], "width": 8, "height": 50, "index": f"{pagina}{linha}1"}}
    por_tabela = {item["table_name"]: item["id"] for item in api.list("dataset") if item.get("schema") == SCHEMA}
    def filtro(indice, nome, coluna, tabelas, aba):
        return {"id": f"NATIVE-FILTRO-LF-{indice}", "name": nome, "filterType": "filter_select", "type": "NATIVE_FILTER", "targets": [{"datasetId": por_tabela[t], "column": {"name": coluna}} for t in tabelas], "defaultDataMask": {"extraFormData": {}, "filterState": {"value": []}, "ownState": {}}, "controlValues": {"multiSelect": True, "enableEmptyFilter": True, "defaultToFirstItem": False, "searchAllOptions": True}, "required": False, "scope": {"rootPath": ["ROOT_ID", tabs, f"TAB-PERGUNTAS-LF-{aba}"], "excluded": []}, "cascadeParentIds": []}
    def periodo(indice, aba, alvos):
        return {"id": f"NATIVE-TEMPO-LF-{indice}", "name": "Período", "filterType": "filter_time", "type": "NATIVE_FILTER", "targets": [{"datasetId": por_tabela[t], "column": {"name": c}} for t, c in alvos], "defaultDataMask": {"extraFormData": {"time_range": "No filter"}, "filterState": {"value": "No filter"}}, "controlValues": {"enableEmptyFilter": True}, "required": False, "scope": {"rootPath": ["ROOT_ID", tabs, f"TAB-PERGUNTAS-LF-{aba}"], "excluded": []}, "cascadeParentIds": []}
    resumo, semanal, features = "ouro_linha_financiada_resumo_mensal", "ouro_linha_financiada_relatorio_semanal", "ouro_linha_financiada_features_preditivas"
    filtros = [filtro(1, "Fonte de recurso", "fonte_recurso", [features, "ouro_linha_financiada_execucao_orcamentaria"], 1), filtro(2, "Linha financiada", "segmento_linha_financiada", [features], 1), periodo(3, 1, [(features, "competencia_contratacao")]), filtro(4, "Fonte de recurso", "fonte_recurso", ["ouro_linha_financiada_contrapartidas"], 2), filtro(5, "Linha financiada", "segmento_linha_financiada", ["ouro_linha_financiada_contrapartidas"], 2), filtro(6, "Fonte de recurso", "fonte_recurso", [resumo, semanal], 3), filtro(7, "Linha financiada", "segmento_linha_financiada", [resumo, semanal], 3), periodo(8, 3, [(resumo, "competencia_contratacao"), (semanal, "semana_referencia")]), filtro(9, "UF", "uf", ["ouro_linha_financiada_mapa_execucao", "ouro_linha_financiada_ranking_territorial"], 4), filtro(10, "Fonte de recurso", "fonte_recurso", ["ouro_linha_financiada_mapa_execucao", "ouro_linha_financiada_ranking_territorial"], 4), filtro(11, "Linha financiada", "segmento_linha_financiada", ["ouro_linha_financiada_mapa_execucao", "ouro_linha_financiada_ranking_territorial"], 4), filtro(12, "Empreendimento", "nome_empreendimento", ["ouro_linha_financiada_empreendimentos"], 5), filtro(13, "UF", "uf_painel", ["ouro_linha_financiada_empreendimentos"], 5), filtro(14, "Município", "municipio_painel", ["ouro_linha_financiada_empreendimentos"], 5)]
    payload = {"dashboard_title": titulo, "slug": slug, "published": True, "position_json": json.dumps(estrutura), "json_metadata": json.dumps({"refresh_frequency": 0, "show_native_filters": True, "native_filter_configuration": filtros})}
    dashboard_id = api.dashboard_id(slug)
    if dashboard_id: api.update("dashboard", dashboard_id, payload)
    else: dashboard_id = api.create("dashboard", payload)["id"]
    if not api.dry_run:
        for chart_id in ids.values():
            atual = api.session.get(f"{api.base_url}/api/v1/chart/{chart_id}", timeout=30).json()["result"]
            destinos = {item["id"] for item in atual.get("dashboards", [])}
            if dashboard_id not in destinos: api.update("chart", chart_id, {"dashboards": sorted(destinos | {dashboard_id})})


def main() -> None:
    global CHARTS
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--completo", action="store_true")
    parser.add_argument("--perguntas", action="store_true")
    args = parser.parse_args()
    load_dotenv("local.env", override=False)
    load_dotenv(".env", override=False)
    api = Superset(
        env("SUPERSET_URL", "SUPERSET_HOST_PORT"),
        env("SUPERSET_USERNAME"),
        env("SUPERSET_PASSWORD"),
        args.dry_run,
    )
    if args.perguntas:
        CHARTS = CHARTS_PERGUNTAS
    elif args.completo:
        CHARTS = CHARTS_COMPLETO
    ids_dataset = datasets(api, get_or_create_database(api))
    ids_chart = charts(api, ids_dataset)
    if args.perguntas:
        dashboard_perguntas(api, ids_chart)
    elif args.completo:
        dashboard_completo(api, ids_chart)
    else:
        dashboard(api, ids_chart)
    print(f"Concluído: {len(ids_dataset)} datasets, {len(ids_chart)} charts e 1 dashboard.")


if __name__ == "__main__":
    main()
