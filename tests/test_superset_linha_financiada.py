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


class _SupersetFalso:
    """Registra o que dashboard_perguntas cria e atualiza."""

    base_url = "http://superset"
    dry_run = False

    def __init__(self, existente=None, ligados=()):
        self.existente = existente
        self.ligados = ligados
        self.chamadas = []
        self.session = self

    def list(self, rota):
        tabelas = {t for c in linha_financiada.CHARTS for t in [c["dataset"]]}
        tabelas |= {
            "ouro_linha_financiada_execucao_orcamentaria",
            "ouro_linha_financiada_contrapartidas",
            "ouro_linha_financiada_resumo_mensal",
            "ouro_linha_financiada_relatorio_semanal",
            "ouro_linha_financiada_features_preditivas",
            "ouro_linha_financiada_mapa_execucao",
            "ouro_linha_financiada_ranking_territorial",
            "ouro_linha_financiada_empreendimentos",
        }
        return [
            {"table_name": t, "id": i, "schema": linha_financiada.SCHEMA}
            for i, t in enumerate(sorted(tabelas), start=1)
        ]

    def dashboard_id(self, slug):
        return self.existente

    def create(self, rota, payload):
        self.chamadas.append(("create", rota, payload))
        return {"id": 99}

    def update(self, rota, ident, payload):
        self.chamadas.append(("update", rota, ident, payload))

    def get(self, url, timeout):
        chart_id = int(url.rsplit("/", 1)[-1])
        dashboards = [{"id": 99}] if chart_id in self.ligados else []
        return type("R", (), {"json": lambda _s: {"result": {"dashboards": dashboards}}})()


def _ids_das_perguntas():
    return {c["key"]: i for i, c in enumerate(linha_financiada.CHARTS_PERGUNTAS, start=1)}


def test_dashboard_perguntas_cria_e_liga_os_graficos() -> None:
    import json

    ids = _ids_das_perguntas()
    api = _SupersetFalso(ligados={1})
    linha_financiada.dashboard_perguntas(api, ids)
    criado = [c for c in api.chamadas if c[0] == "create"]
    assert len(criado) == 1 and criado[0][2]["slug"] == "perguntas-linha-financiada"
    posicao = json.loads(criado[0][2]["position_json"])
    graficos = {v["meta"]["chartId"] for v in posicao.values() if v["type"] == "CHART"}
    assert graficos <= set(ids.values())
    filtros = json.loads(criado[0][2]["json_metadata"])["native_filter_configuration"]
    assert len(filtros) == 14
    ligados = {c[2] for c in api.chamadas if c[:2] == ("update", "chart")}
    assert ligados == set(ids.values()) - {1}


def test_dashboard_perguntas_atualiza_o_que_existe() -> None:
    api = _SupersetFalso(existente=7)
    linha_financiada.dashboard_perguntas(api, _ids_das_perguntas())
    assert [c[:3] for c in api.chamadas if c[1] == "dashboard"] == [("update", "dashboard", 7)]
