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
            "all_columns": [],
            "order_by_cols": ["[\"semana_contratacao\", false]"],
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
            "size": "quantidade_contratos",
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
            "all_columns": [],
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
            "groupby": ["fonte_recurso", "tipo_contrapartida", "situacao_cobertura"],
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
            "all_columns": [],
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
        "params": {"query_mode": "raw", "all_columns": [], "row_limit": 100},
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
            if atual.get("datasource_id") != dataset_id:
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
    else:
        api.create("dashboard", payload)


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
    dashboard(api, ids_chart)
    print(f"Concluído: {len(ids_dataset)} datasets, {len(ids_chart)} charts e 1 dashboard.")


if __name__ == "__main__":
    main()
