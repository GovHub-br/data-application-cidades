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
        ("sum__quantidade_empreendimentos", "SUM(quantidade_empreendimentos)"),
        ("sum__valor_desconto_ogu", "SUM(valor_desconto_ogu)"),
    ],
    "ouro_linha_financiada_mapa_execucao": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__valor_financiamento", "SUM(valor_financiamento)"),
    ],
    "ouro_linha_financiada_empreendimentos": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__quantidade_unidades", "SUM(quantidade_unidades)"),
        ("sum__quantidade_unidades_entregues", "SUM(quantidade_unidades_entregues)"),
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
        "key": "kpi_contratos",
        "title": "Contratos na carteira",
        "dataset": "ouro_linha_financiada_resumo_mensal",
        "viz_type": "big_number_total",
        "params": {"metric": "sum__quantidade_contratos", "subheader": "FGTS e Fundo Social"},
    },
    {
        "key": "kpi_valor",
        "title": "Valor financiado",
        "dataset": "ouro_linha_financiada_resumo_mensal",
        "viz_type": "big_number_total",
        "params": {"metric": "sum__valor_financiamento", "y_axis_format": ".2s"},
    },
    {
        "key": "fonte_pizza",
        "title": "Contratos por fonte de recurso",
        "dataset": "ouro_linha_financiada_resumo_mensal",
        "viz_type": "pie",
        "params": {"groupby": ["fonte_recurso"], "metric": "sum__quantidade_contratos", "donut": True, "row_limit": 50},
    },
    {
        "key": "segmento_pizza",
        "title": "Contratos por linha",
        "dataset": "ouro_linha_financiada_resumo_mensal",
        "viz_type": "pie",
        "params": {"groupby": ["segmento_linha_financiada"], "metric": "sum__quantidade_contratos", "donut": True, "row_limit": 50},
    },
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
            "size": "sum__quantidade_unidades",
            "point_radius_fixed": {"type": "fix", "value": 5000},
            "row_limit": 50000,
            "mapbox_style": "mapbox://styles/mapbox/light-v9",
            "viewport": {"latitude": -14.2, "longitude": -51.9, "zoom": 3.4},
        },
    },
    {
        "key": "uf_barra",
        "title": "Contratação por UF",
        "dataset": "ouro_linha_financiada_mapa_execucao",
        "viz_type": "dist_bar",
        "params": {"groupby": ["uf"], "metrics": ["sum__quantidade_contratos"], "row_limit": 27, "order_desc": True},
    },
    {
        "key": "situacao_empreendimento",
        "title": "Situação dos empreendimentos",
        "dataset": "ouro_linha_financiada_empreendimentos",
        "viz_type": "pie",
        "params": {"groupby": ["situacao_execucao"], "metric": "sum__quantidade_unidades", "donut": True, "row_limit": 20},
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
        "key": "contrapartida_pizza",
        "title": "Cobertura de contrapartidas por tipo",
        "dataset": "ouro_linha_financiada_contrapartidas",
        "viz_type": "pie",
        "params": {"groupby": ["tipo_contrapartida"], "metric": "sum__quantidade_contratos", "donut": True, "row_limit": 30},
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
        "key": "orcamento_barra",
        "title": "Orçamento atualizado e pagamentos",
        "dataset": "ouro_linha_financiada_execucao_orcamentaria",
        "viz_type": "dist_bar",
        "params": {"groupby": ["ano", "fonte_recurso"], "metrics": ["sum__orcamento_atualizado", "sum__pagamentos_totais"], "row_limit": 100},
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


PAGINAS = [
    ("Visão geral", [["kpi_contratos", "kpi_valor", "fonte_pizza", "segmento_pizza"], ["contratos_mes"], ["valor_mes"], ["semanal"]]),
    ("Mapa e território", [["mapa"], ["uf_barra", "municipios"]]),
    ("Empreendimentos", [["situacao_empreendimento"], ["empreendimentos"]]),
    ("Contrapartidas e orçamento", [["contrapartida_pizza", "contrapartidas"], ["orcamento_barra"], ["orcamento"]]),
    ("Predição e cobertura", [["previsao"], ["cobertura"]]),
]


def layout(ids_chart: dict[str, int]) -> str:
    """Organiza o painel em abas e linhas responsivas, sem iframe ou código UI."""
    tabs_id = "TABS-LINHA-FINANCIADA"
    estrutura = {
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": [tabs_id], "parents": [], "meta": {}},
        tabs_id: {"id": tabs_id, "type": "TABS", "children": [], "parents": ["ROOT_ID"], "meta": {}},
    }
    for pagina, (titulo, linhas) in enumerate(PAGINAS):
        tab_id = f"TAB-LF-{pagina:02d}"
        estrutura[tabs_id]["children"].append(tab_id)
        estrutura[tab_id] = {"id": tab_id, "type": "TAB", "children": [], "parents": ["ROOT_ID", tabs_id], "meta": {"text": titulo, "defaultText": titulo, "placeholder": titulo}}
        for indice, chaves in enumerate(linhas):
            row_id = f"ROW-LF-{pagina:02d}-{indice:02d}"
            estrutura[tab_id]["children"].append(row_id)
            estrutura[row_id] = {"id": row_id, "type": "ROW", "children": [], "parents": ["ROOT_ID", tabs_id, tab_id], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            largura = max(1, 12 // len(chaves))
            for posicao, chave in enumerate(chaves):
                chart_id = ids_chart[chave]
                node_id = f"CHART-{chart_id}"
                estrutura[row_id]["children"].append(node_id)
                estrutura[node_id] = {"id": node_id, "type": "CHART", "children": [], "parents": ["ROOT_ID", tabs_id, tab_id, row_id], "meta": {"chartId": chart_id, "width": largura, "height": 34 if len(chaves) > 1 else 55, "index": f"{pagina:02d}{indice:02d}{posicao:02d}"}}
    return json.dumps(estrutura)


def filtros_nativos(ids_dataset: dict[str, int]) -> list[dict]:
    """Filtros compartilhados somente onde a coluna existe em cada Gold."""
    definicoes = [
        ("Período de contratação", "filter_time", "competencia_contratacao", ["ouro_linha_financiada_resumo_mensal", "ouro_linha_financiada_features_preditivas"]),
        ("Fonte de recurso", "filter_select", "fonte_recurso", ["ouro_linha_financiada_resumo_mensal", "ouro_linha_financiada_relatorio_semanal", "ouro_linha_financiada_mapa_execucao", "ouro_linha_financiada_contrapartidas", "ouro_linha_financiada_execucao_orcamentaria", "ouro_linha_financiada_features_preditivas"]),
        ("Linha financiada", "filter_select", "segmento_linha_financiada", ["ouro_linha_financiada_resumo_mensal", "ouro_linha_financiada_relatorio_semanal", "ouro_linha_financiada_mapa_execucao", "ouro_linha_financiada_contrapartidas", "ouro_linha_financiada_features_preditivas"]),
        ("UF", "filter_select", "uf", ["ouro_linha_financiada_mapa_execucao", "ouro_linha_financiada_empreendimentos"]),
        ("Município", "filter_select", "municipio", ["ouro_linha_financiada_mapa_execucao", "ouro_linha_financiada_empreendimentos"]),
        ("Faixa", "filter_select", "faixa_codigo", ["ouro_linha_financiada_resumo_mensal", "ouro_linha_financiada_relatorio_semanal", "ouro_linha_financiada_mapa_execucao"]),
        ("Modalidade", "filter_select", "modalidade", ["ouro_linha_financiada_resumo_mensal", "ouro_linha_financiada_relatorio_semanal", "ouro_linha_financiada_mapa_execucao"]),
        ("Tipo de imóvel", "filter_select", "tipo_imovel", ["ouro_linha_financiada_resumo_mensal", "ouro_linha_financiada_relatorio_semanal", "ouro_linha_financiada_mapa_execucao"]),
    ]
    filtros = []
    for indice, (nome, tipo, coluna, tabelas) in enumerate(definicoes, start=1):
        filtros.append({"id": f"NATIVE_FILTER-LF-{indice:02d}", "name": nome, "filterType": tipo, "type": "NATIVE_FILTER", "targets": [{"datasetId": ids_dataset[t], "column": {"name": coluna}} for t in tabelas], "defaultDataMask": {}, "controlValues": {"multiSelect": tipo == "filter_select", "enableEmptyFilter": True, "defaultToFirstItem": False, "searchAllOptions": True, "inverseSelection": False}, "scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "cascadeParentIds": []})
    return filtros


def dashboard(api: Superset, ids_chart: dict[str, int], ids_dataset: dict[str, int]) -> None:
    payload = {
        "dashboard_title": DASHBOARD_TITLE,
        "slug": DASHBOARD_SLUG,
        "published": True,
        "position_json": layout(ids_chart),
        "json_metadata": json.dumps({"timed_refresh_immune_slices": [], "refresh_frequency": 0, "native_filter_configuration": filtros_nativos(ids_dataset)}),
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
    quantidade = len(ligados.json().get("result", []))
    if quantidade != len(ids_chart):
        raise RuntimeError(
            f"Dashboard recebeu {quantidade} charts; esperados {len(ids_chart)}."
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    load_dotenv("local.env", override=False)
    load_dotenv(".env", override=False)
    api = Superset(
        env("SUPERSET_URL", "SUPERSET_HOST_PORT"),
        env("SUPERSET_USERNAME"),
        env("SUPERSET_PASSWORD"),
        args.dry_run,
    )
    ids_dataset = datasets(api, get_or_create_database(api))
    ids_chart = charts(api, ids_dataset)
    dashboard(api, ids_chart, ids_dataset)
    print(f"Concluído: {len(ids_dataset)} datasets, {len(ids_chart)} charts e 1 dashboard.")


if __name__ == "__main__":
    main()
