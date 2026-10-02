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
