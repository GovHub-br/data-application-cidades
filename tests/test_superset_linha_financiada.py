from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]


def modulo():
    caminho = RAIZ / "scripts" / "superset" / "linha_financiada.py"
    sys.path.insert(0, str(caminho.parent))
    spec = importlib.util.spec_from_file_location("superset_linha_financiada", caminho)
    assert spec and spec.loader
    carregado = importlib.util.module_from_spec(spec)
    sys.modules["superset_linha_financiada"] = carregado
    spec.loader.exec_module(carregado)
    return carregado


def test_todas_as_gold_dos_charts_estao_declaradas() -> None:
    m = modulo()
    assert {c["dataset"] for c in m.CHARTS} <= set(m.DATASETS)


def test_dashboard_tem_mapa_e_cobertura_das_perguntas() -> None:
    m = modulo()
    por_chave = {c["key"]: c for c in m.CHARTS}
    # O mapa de pontos dependia de latitude/longitude incompletas e foi
    # substituído pelo mapa nacional por UF, que não inventa coordenadas.
    completos = {c["key"]: c for c in m.CHARTS_COMPLETO}
    assert completos["mapa_brasil"]["viz_type"] == "country_map"
    assert por_chave["cobertura"]["dataset"].endswith("cobertura_perguntas")


def test_campos_dos_charts_conferem_com_as_golds() -> None:
    m = modulo()
    por_chave = {c["key"]: c for c in m.CHARTS}
    assert por_chave["semanal"]["params"]["order_by_cols"] == [
        '["semana_referencia", false]'
    ]
    assert "ic_valor_informado" in por_chave["contrapartidas"]["params"]["groupby"]


def test_slug_do_dashboard_e_estavel() -> None:
    assert modulo().DASHBOARD_SLUG == "linha-financiada"
