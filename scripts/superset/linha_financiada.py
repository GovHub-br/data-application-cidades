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
]

# O Superset não cria métricas agregadas automaticamente ao registrar uma
# tabela. Declaramos as métricas pelo mesmo nome usado pelos gráficos, para que
# o dashboard seja reproduzível em qualquer instância.
METRICAS = {
    "ouro_linha_financiada_resumo_mensal": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__valor_financiamento", "SUM(valor_financiamento)"),
    ],
    "ouro_linha_financiada_mapa_execucao": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__valor_financiamento", "SUM(valor_financiamento)"),
    ],
    "ouro_linha_financiada_empreendimentos": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
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
        "key": "mapa",
        "title": "Mapa de empreendimentos",
        "dataset": "ouro_linha_financiada_empreendimentos",
        "viz_type": "deck_scatter",
        "params": {
            "spatial": {"type": "latlong", "latCol": "latitude", "lonCol": "longitude"},
            "size": "sum__quantidade_contratos",
            "point_radius_fixed": {"type": "fix", "value": 5000},
            "row_limit": 50000,
            "mapbox_style": "mapbox://styles/mapbox/light-v9",
            "viewport": {"latitude": -14.2, "longitude": -51.9, "zoom": 3.4},
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
]


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
        ("Nacional", [["contratos_mes"], ["valor_mes"], ["semanal"], ["previsao"]]),
        ("Estados e municípios", [["mapa"], ["ranking_uf", "municipios"]]),
        ("Por linha", [["contrapartidas", "contrapartidas_por_linha"], ["orcamento", "orcamento_resumo"], ["cobertura"]]),
        ("Empreendimentos", [["empreendimentos"]]),
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
    alvos = {
        "Fonte de recurso": ("fonte_recurso", ["ouro_linha_financiada_resumo_mensal", "ouro_linha_financiada_relatorio_semanal", "ouro_linha_financiada_mapa_execucao", "ouro_linha_financiada_contrapartidas", "ouro_linha_financiada_execucao_orcamentaria", "ouro_linha_financiada_features_preditivas"]),
        "Linha financiada": ("segmento_linha_financiada", ["ouro_linha_financiada_resumo_mensal", "ouro_linha_financiada_relatorio_semanal", "ouro_linha_financiada_mapa_execucao", "ouro_linha_financiada_contrapartidas", "ouro_linha_financiada_features_preditivas"]),
        "UF": ("uf", ["ouro_linha_financiada_mapa_execucao", "ouro_linha_financiada_empreendimentos"]),
        "Faixa": ("faixa_codigo", ["ouro_linha_financiada_resumo_mensal", "ouro_linha_financiada_relatorio_semanal", "ouro_linha_financiada_mapa_execucao"]),
    }
    filtros = []
    for indice, (nome, (coluna, tabelas)) in enumerate(alvos.items(), 1):
        # Sem valor inicial: nesta versão do Superset `value: []` significa
        # conjunto vazio, e não "todos". O filtro deve abrir sem restringir.
        filtros.append({"id": f"NATIVE_FILTER-COMP-{indice}", "name": nome, "filterType": "filter_select", "type": "NATIVE_FILTER", "targets": [{"datasetId": por_tabela[t], "column": {"name": coluna}} for t in tabelas], "defaultDataMask": {}, "controlValues": {"multiSelect": True, "enableEmptyFilter": True, "defaultToFirstItem": False, "searchAllOptions": True, "inverseSelection": False}, "scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "cascadeParentIds": []})
    api.update("dashboard", api.dashboard_id(DASHBOARD_SLUG), {"position_json": json.dumps(estrutura), "json_metadata": json.dumps({"native_filter_configuration": filtros, "show_native_filters": True})})


def main() -> None:
    global CHARTS
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--completo", action="store_true")
    args = parser.parse_args()
    load_dotenv("local.env", override=False)
    load_dotenv(".env", override=False)
    api = Superset(
        env("SUPERSET_URL", "SUPERSET_HOST_PORT"),
        env("SUPERSET_USERNAME"),
        env("SUPERSET_PASSWORD"),
        args.dry_run,
    )
    if args.completo:
        CHARTS = CHARTS_COMPLETO
    ids_dataset = datasets(api, get_or_create_database(api))
    ids_chart = charts(api, ids_dataset)
    if args.completo:
        dashboard_completo(api, ids_chart)
    else:
        dashboard(api, ids_chart)
    print(f"Concluído: {len(ids_dataset)} datasets, {len(ids_chart)} charts e 1 dashboard.")


if __name__ == "__main__":
    main()
