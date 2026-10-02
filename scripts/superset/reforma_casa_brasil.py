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
