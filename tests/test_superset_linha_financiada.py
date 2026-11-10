from __future__ import annotations

import linha_financiada


def test_todas_as_gold_dos_charts_estao_declaradas() -> None:
    m = linha_financiada
    assert {c["dataset"] for c in m.CHARTS} <= set(m.DATASETS)


def test_dashboard_tem_mapa_e_cobertura_das_perguntas() -> None:
    m = linha_financiada
    por_chave = {c["key"]: c for c in m.CHARTS}
    # O mapa de pontos dependia de latitude/longitude incompletas e foi
    # substituído pelo mapa nacional por UF, que não inventa coordenadas.
    completos = {c["key"]: c for c in m.CHARTS_COMPLETO}
    assert completos["mapa_brasil"]["viz_type"] == "country_map"
    assert por_chave["cobertura"]["dataset"].endswith("cobertura_perguntas")


def test_campos_dos_charts_conferem_com_as_golds() -> None:
    m = linha_financiada
    por_chave = {c["key"]: c for c in m.CHARTS}
    assert por_chave["semanal"]["params"]["order_by_cols"] == ['["semana_referencia", false]']
    assert "ic_valor_informado" in por_chave["contrapartidas"]["params"]["groupby"]


def test_slug_do_dashboard_e_estavel() -> None:
    assert linha_financiada.DASHBOARD_SLUG == "linha-financiada"
