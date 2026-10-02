#!/usr/bin/env python3
"""Provisiona o dashboard do Reforma Casa Brasil no Superset.

Não transforma lacunas de acesso, obra ou pós-obra em indicadores zerados.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bootstrap_conjuntura import Superset, env, get_or_create_database  # noqa: E402

SCHEMA = "ouro"
DASHBOARD_TITLE = "Reforma Casa Brasil — Monitoramento e Evidências"
DASHBOARD_SLUG = "reforma-casa-brasil"

DATASETS = [
    "ouro_reforma_casa_brasil_implementacao_dash",
    "ouro_reforma_casa_brasil_monitoramento_recursos_dash",
    "ouro_reforma_casa_brasil_cobertura_perguntas_dash",
]

METRICAS = {
    "ouro_reforma_casa_brasil_implementacao_dash": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__valor_financiado_total", "SUM(valor_financiado_total)"),
        ("sum__valor_investimento_total", "SUM(valor_investimento_total)"),
    ],
    "ouro_reforma_casa_brasil_monitoramento_recursos_dash": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__valor_financiado_total", "SUM(valor_financiado_total)"),
        ("sum__quantidade_primeira_ocorrencia_serie", "SUM(quantidade_primeira_ocorrencia_serie)"),
    ],
}

CHARTS = [
    {"key": "contratos_uf", "title": "Contratos por UF", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["uf"], "metrics": ["sum__quantidade_contratos", "sum__valor_financiado_total"], "row_limit": 30}},
    {"key": "financiamento_faixa", "title": "Financiamento por faixa", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "pie", "params": {"groupby": ["faixa_renda"], "metric": "sum__valor_financiado_total", "donut": True, "row_limit": 30}},
    {"key": "implementacao", "title": "Implementação por município", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "table", "params": {"query_mode": "raw", "all_columns": ["uf", "municipio", "faixa_renda", "modalidade", "quantidade_contratos", "valor_financiado_total", "quantidade_com_atraso", "proporcao_com_atraso"], "row_limit": 5000}},
    {"key": "monitoramento", "title": "Carteira por competência", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_line", "params": {"x_axis": "dt_referencia", "metrics": ["sum__quantidade_contratos", "sum__quantidade_primeira_ocorrencia_serie"], "groupby": ["uf"], "time_grain_sqla": "P1M", "row_limit": 10000}},
    {"key": "recursos", "title": "Recursos contratados por competência", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_bar", "params": {"x_axis": "dt_referencia", "metrics": ["sum__valor_financiado_total"], "groupby": ["modalidade"], "time_grain_sqla": "P1M", "row_limit": 10000}},
    {"key": "cobertura", "title": "Cobertura das perguntas", "dataset": "ouro_reforma_casa_brasil_cobertura_perguntas_dash", "viz_type": "table", "params": {"query_mode": "raw", "all_columns": ["bloco", "pergunta", "situacao_dado", "resposta_disponivel", "fonte_necessaria", "limitacao"], "row_limit": 100}},
]


def datasets(api: Superset, database_id: int) -> dict[str, int]:
    existentes = {(x.get("schema"), x.get("table_name")): x["id"] for x in api.list("dataset")}
    ids: dict[str, int] = {}
    for nome in DATASETS:
        chave = (SCHEMA, nome)
        if chave not in existentes:
            existentes[chave] = api.create("dataset", {"database": database_id, "schema": SCHEMA, "table_name": nome})["id"]
        ids[nome] = existentes[chave]
    for nome, metricas in METRICAS.items():
        atual = api.session.get(f"{api.base_url}/api/v1/dataset/{ids[nome]}", timeout=30).json()["result"].get("metrics", [])
        esperadas = {m[0] for m in metricas}
        if {m["metric_name"] for m in atual} == esperadas:
            continue
        if not api.dry_run:
            for metrica in atual:
                api.session.delete(f"{api.base_url}/api/v1/dataset/{ids[nome]}/metric/{metrica['id']}", timeout=30).raise_for_status()
        api.update("dataset", ids[nome], {"metrics": [{"metric_name": nome_metrica, "verbose_name": nome_metrica.replace("sum__", "Soma de ").replace("_", " "), "expression": expressao, "metric_type": "sum"} for nome_metrica, expressao in metricas]})
    return ids


def charts(api: Superset, ids: dict[str, int]) -> dict[str, int]:
    existentes = {x.get("slice_name"): x for x in api.list("chart")}
    resultado = {}
    for definicao in CHARTS:
        titulo = f"Reforma Casa Brasil | {definicao['title']}"
        dataset_id = ids[definicao["dataset"]]
        payload = {"slice_name": titulo, "viz_type": definicao["viz_type"], "datasource_id": dataset_id, "datasource_type": "table", "params": json.dumps({"datasource": f"{dataset_id}__table", "viz_type": definicao["viz_type"], **definicao["params"]})}
        atual = existentes.get(titulo)
        if atual is None:
            resultado[definicao["key"]] = api.create("chart", payload)["id"]
        else:
            resultado[definicao["key"]] = atual["id"]
            api.update("chart", atual["id"], payload)
    return resultado


def layout(ids: dict[str, int]) -> str:
    paginas = [("Implementação", [["contratos_uf", "financiamento_faixa"], ["implementacao"]]), ("Monitoramento", [["monitoramento"], ["recursos"]]), ("Cobertura e lacunas", [["cobertura"]])]
    tabs = "TABS-REFORMA"
    estrutura = {"ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": [tabs], "parents": [], "meta": {}}, tabs: {"id": tabs, "type": "TABS", "children": [], "parents": ["ROOT_ID"], "meta": {}}}
    for pagina, (titulo, linhas) in enumerate(paginas):
        tab = f"TAB-REF-{pagina}"
        estrutura[tabs]["children"].append(tab)
        estrutura[tab] = {"id": tab, "type": "TAB", "children": [], "parents": ["ROOT_ID", tabs], "meta": {"text": titulo, "defaultText": titulo, "placeholder": titulo}}
        for linha, chaves in enumerate(linhas):
            row = f"ROW-REF-{pagina}-{linha}"
            estrutura[tab]["children"].append(row)
            estrutura[row] = {"id": row, "type": "ROW", "children": [], "parents": ["ROOT_ID", tabs, tab], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            for posicao, chave in enumerate(chaves):
                node = f"CHART-{ids[chave]}"
                estrutura[row]["children"].append(node)
                estrutura[node] = {"id": node, "type": "CHART", "children": [], "parents": ["ROOT_ID", tabs, tab, row], "meta": {"chartId": ids[chave], "width": 12 // len(chaves), "height": 48, "index": f"{pagina}{linha}{posicao}"}}
    return json.dumps(estrutura)


def dashboard(api: Superset, ids: dict[str, int]) -> None:
    payload = {"dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG, "published": True, "position_json": layout(ids), "json_metadata": json.dumps({"refresh_frequency": 0})}
    dashboard_id = api.dashboard_id(DASHBOARD_SLUG)
    if dashboard_id:
        api.update("dashboard", dashboard_id, payload)
    else:
        dashboard_id = api.create("dashboard", payload)["id"]
    if api.dry_run:
        return
    for chart_id in ids.values():
        atual = api.session.get(f"{api.base_url}/api/v1/chart/{chart_id}", timeout=30).json()["result"]
        destinos = {x["id"] for x in atual.get("dashboards", [])}
        if dashboard_id not in destinos:
            api.update("chart", chart_id, {"dashboards": sorted(destinos | {dashboard_id})})


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    load_dotenv("local.env", override=False)
    load_dotenv(".env", override=False)
    api = Superset(env("SUPERSET_URL", "SUPERSET_HOST_PORT"), env("SUPERSET_USERNAME"), env("SUPERSET_PASSWORD"), args.dry_run)
    ids = datasets(api, get_or_create_database(api))
    dashboard(api, charts(api, ids))


if __name__ == "__main__":
    main()
