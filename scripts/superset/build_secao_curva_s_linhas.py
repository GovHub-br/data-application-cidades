"""Seção "Curva S e quem foge dela" nas abas **FNHIS Sub-50** e **Pró-Moradia** do painel `mcid-perguntas`.

Adaptado ao dado de cada linha:
  - Pró-Moradia: Curva S por TEMPO DE CONTRATO (mediana e p25–p75 do % físico do Canal FGTS), como a referência do
    Rural; execução mensal só da carteira em execução; obras atrasadas e obras sem medição há 12+ meses;
  - FNHIS Sub-50: não há % físico nas bases e o empenho sai em parcelas fixas, então não há Curva S. A seção mostra a
    evolução do empenho, as faixas/parcelas de empenho e os termos parados na mesma parcela.

O que o script altera:
  - só ACRESCENTA a seção no fim de cada aba (as linhas do layout com id `CS-<SIGLA>-*`); o que já existe na aba
    fica como está. Rodar de novo troca só essas linhas;
  - charts criados ou atualizados pelo nome (prefixo "Sub-50 ▸ " / "Pró-Moradia ▸ ");
  - não mexe em filtros nativos nem apaga chart ou dataset.

Uso, na raiz do repo:
    poetry run python scripts/superset/build_secao_curva_s_linhas.py --dry-run
    poetry run python scripts/superset/build_secao_curva_s_linhas.py

Pré-requisito: ouro_pro_moradia_curva_s, _curva_s_referencia e _curva_s_desvio; ouro_fnhis_curva_s e
ouro_fnhis_empenho_parcelas materializadas.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from build_boletim import Superset  # noqa: E402
from build_aba_rural_mcid import (  # noqa: E402
    AMAR, N, SCHEMA, SLUG, TXT, VERM, Grafico, Texto, barra, col, cond, m, tabela_agregada, tabela_linhas,
)



def linha_curva(chave, titulo, dataset, metricas, eixo_y, descricao, largura=6, filtro_sql=None):
    p = {"x_axis": "mes", "time_grain_sqla": "P1M", "metrics": metricas, "groupby": [], "y_axis_format": ",.1f",
         "show_legend": True, "rich_tooltip": True, "markerEnabled": True, "x_axis_title": "Mês",
         "y_axis_title": eixo_y, "x_axis_title_margin": 30, "y_axis_title_margin": 50,
         "y_axis_title_position": "Left", "truncateYAxis": False, "y_axis_bounds": [0, 100], "row_limit": 1000}
    if filtro_sql:
        p["adhoc_filters"] = [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": filtro_sql}]
    return Grafico(chave, titulo, dataset, "echarts_timeseries_line", p, descricao, largura, 55)


def linha_referencia(chave, titulo, dataset, metricas, eixo_x, eixo_y, descricao, largura=7):
    p = {"x_axis": "idade_meses", "metrics": metricas, "groupby": [], "y_axis_format": ",.1f",
         "show_legend": True, "rich_tooltip": True, "markerEnabled": True, "x_axis_title": eixo_x,
         "y_axis_title": eixo_y, "x_axis_title_margin": 30, "y_axis_title_margin": 50,
         "y_axis_title_position": "Left", "y_axis_bounds": [0, 100], "row_limit": 1000,
         "x_axis_sort": "idade_meses", "x_axis_sort_asc": True,
         "adhoc_filters": [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": "ic_referencia_valida"}]}
    return Grafico(chave, titulo, dataset, "echarts_timeseries_line", p, descricao, largura, 55)


def numero(chave, titulo, dataset, metrica, sub, descricao, fmt=",d"):
    p = {"metric": metrica, "subheader": sub, "y_axis_format": fmt, "header_font_size": 0.4,
         "subheader_font_size": 0.15}
    return Grafico(chave, titulo, dataset, "big_number_total", p, descricao, 3, 55)


# --------------------------------------------------------------------------
# Pró-Moradia

def secao_pro_moradia() -> list[list]:
    CS, REF, DES = "ouro_pro_moradia_curva_s", "ouro_pro_moradia_curva_s_referencia", "ouro_pro_moradia_curva_s_desvio"
    real = m("SUM(fis_realizado_x_valor) / NULLIF(SUM(valor_com_medicao), 0)", "Físico realizado (%)")
    fin = m("100.0 * SUM(vr_desembolsado_acumulado) / NULLIF(SUM(valor_com_desembolso), 0)", "Financeiro desembolsado (%)")
    atrasada = "situacao_acompanhamento = 'Atrasada'"
    parada = "situacao_acompanhamento = 'Sem medição há 12+ meses'"
    em_aberto = "situacao_acompanhamento <> 'Concluída ou encerrada'"
    cols_contrato = [col("contrato", "Contrato"), col("empreendimento_nome", "Empreendimento"),
                     col("tomador_nome", "Tomador"), col("uf", "UF"), col("municipio", "Município"),
                     col("tipo_intervencao", "Intervenção")]
    cfg_contrato = {"Empreendimento": TXT(220), "Tomador": TXT(200), "% físico": N(",.1f", True),
                    "Mediana da idade": N(",.1f"), "Desvio da mediana (p.p.)": N(",.1f"),
                    "% desembolsado": N(",.1f"), "Contratado (R$)": N(",.0f")}
    situacao_uf = barra("pm_situacao_uf", "Obras em aberto por situação e UF", DES, col("uf", "UF"),
                        [m("COUNT(DISTINCT cod_contrato)", "Contratos")], "UF", "Contratos",
                        grupo=col("situacao_acompanhamento", "Situação"), largura=6, altura=55,
                        descricao="Contratos vigentes com físico < 100%, pela situação de acompanhamento.")
    situacao_uf.params["adhoc_filters"] = [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": em_aberto}]
    return [
        [Texto(
            "## Curva S e obras fora da curva\n"
            "O Pró-Moradia tem contratos de 1995 até hoje, então a Curva S é desenhada pelo **tempo de contrato** "
            "(meses desde a assinatura), como na referência do Rural: para cada idade, a **mediana** do % físico "
            "realizado de todos os contratos medidos naquela idade e a faixa central (p25 a p75). É o ritmo típico "
            "de uma obra do programa.\n\n"
            "Cada obra em aberto (contrato vigente, físico < 100%) é comparada com essa curva na sua idade:\n"
            "- **Atrasada**: medição nos últimos 12 meses e abaixo do quartil inferior (p25) da idade;\n"
            "- **Sem medição há 12+ meses**: não dá para dizer se está atrasada; provável paralisação ou contrato que "
            "parou de reportar no Canal FGTS;\n"
            "- **Em dia**: medição recente, na curva ou acima.\n\n"
            "O % previsto do cronograma não aparece: no Canal FGTS ele vem quase sempre 0 (474 de 653 avaliações "
            "desde 2024). Pesos por **valor contratado** (há contratos sem UH: urbanização, lotes, encostas).",
            altura=44)],
        [linha_referencia("pm_ref", "Curva S por tempo de contrato (% físico realizado)", REF,
                          [m("MAX(fis_p25)", "Quartil inferior (p25)"), m("MAX(fis_mediana)", "Mediana (curva padrão)"),
                           m("MAX(fis_p75)", "Quartil superior (p75)")],
                          "Meses desde a assinatura", "% físico realizado",
                          "Distribuição do % físico de todos os contratos medidos em cada idade. Só idades com pelo "
                          "menos 10 medições.", largura=6),
         numero("pm_k_atrasada", "Obras atrasadas", DES,
                m(f"COUNT(DISTINCT CASE WHEN {atrasada} THEN cod_contrato END)", "Obras atrasadas"),
                "medição recente, abaixo do p25 da idade", "Obras em aberto com medição nos últimos 12 meses e "
                "abaixo do quartil inferior da sua idade."),
         numero("pm_k_parada", "Obras sem medição há 12+ meses", DES,
                m(f"COUNT(DISTINCT CASE WHEN {parada} THEN cod_contrato END)", "Sem medição 12+ meses"),
                "vigentes, físico < 100%", "Obras em aberto cuja última medição tem 12 meses ou mais.")],
        [linha_curva("pm_cs", "Execução da carteira em obra (contratos em execução)", CS, [real, fin],
                     "Execução acumulada (%)",
                     "Só contratos em execução no mês (com medição nos 12 meses anteriores e físico < 100%). Média "
                     "ponderada por valor contratado. Não é uma Curva S: a carteira entra e sai.",
                     filtro_sql="situacao_carteira = 'Em execução' AND mes >= DATE '2010-01-01'"),
         situacao_uf],
        [tabela_linhas(
            "pm_t_atrasada", "Obras atrasadas (piores primeiro)", DES,
            cols_contrato + [
                col("idade_meses", "Meses de contrato"), col("pct_fisico_realizado", "% físico"),
                col("fis_mediana", "Mediana da idade"), col("desvio_mediana_pp", "Desvio da mediana (p.p.)"),
                col("pct_financeiro", "% desembolsado"), col("situacao_obra", "Situação da obra"),
                col("valor_contratado", "Contratado (R$)"), col("identificador_selecao", "Ato de seleção"),
                col("dt_ultima_medicao", "Medição de")],
            cfg_contrato,
            [cond("Desvio da mediana (p.p.)", "<", -20, VERM), cond("Desvio da mediana (p.p.)", "<", 0, AMAR)],
            ordem=[["desvio_mediana_pp", True]], filtro_sql=atrasada,
            descricao="Obras em aberto, com medição nos últimos 12 meses, abaixo do quartil inferior da idade.",
            altura=60)],
        [tabela_linhas(
            "pm_t_parada", "Obras sem medição há 12 meses ou mais", DES,
            cols_contrato + [
                col("dt_ultima_medicao", "Última medição"), col("meses_sem_medicao", "Meses sem medição"),
                col("idade_meses", "Meses de contrato na medição"), col("pct_fisico_realizado", "% físico"),
                col("pct_financeiro", "% desembolsado"), col("situacao_obra", "Situação da obra"),
                col("situacao_contrato", "Situação do contrato"), col("valor_contratado", "Contratado (R$)")],
            {**cfg_contrato, "Meses sem medição": N(",d", True)},
            [cond("Meses sem medição", ">=", 60, VERM), cond("Meses sem medição", ">=", 12, AMAR)],
            ordem=[["meses_sem_medicao", False]], filtro_sql=parada,
            descricao="Contratos vigentes com físico < 100% e sem medição no Canal FGTS há 12 meses ou mais.",
            altura=60)],
    ]


# --------------------------------------------------------------------------
# FNHIS Sub-50

def secao_fnhis() -> list[list]:
    CS, PAR = "ouro_fnhis_curva_s", "ouro_fnhis_empenho_parcelas"
    emp = m("100.0 * SUM(valor_empenhado_acumulado) / NULLIF(SUM(valor_repasse), 0)", "Empenhado (%)")
    parado = "situacao_parcelas IN ('Sem novo empenho há 12+ meses', 'Sem empenho')"
    faixa = barra("fn_faixa", "Termos por faixa de empenho", PAR, col("faixa_empenho", "Faixa de empenho"),
                  [m("COUNT(DISTINCT numero_proposta_transferegov)", "Termos")], "% empenhado do repasse", "Termos",
                  largura=6, altura=55, ordenar_por_soma=False,
                  descricao="Último retrato de cada termo ativo. As faixas seguem as parcelas de empenho.")
    faixa.params.update({"x_axis_sort": "faixa_empenho", "x_axis_sort_asc": True})
    return [
        [Texto(
            "## Empenho por parcelas\n"
            "**O Sub-50 ainda não tem Curva S.** As bases não trazem o % físico das obras (na SNH as operações "
            "estão com 0%), e todos os termos são da seleção da Portaria 1.310/2024, com 12 a 24 meses de "
            "assinatura: mesmo com dado físico, só existiria o começo da curva.\n\n"
            "O que dá para acompanhar é o **empenho**, que sai em **parcelas fixas** (≈10%, 20%, 30%, 50%…). Por "
            "isso a linha abaixo anda em degraus. Para cada termo ativo (sem anulados ou rescindidos) o painel "
            "mostra quantas parcelas ele já teve e **há quanto tempo não recebe uma nova**. Os retratos mensais "
            "do TransfereGov começam em mar/2025: o que foi empenhado antes conta como uma parcela. Empenho é "
            "recurso comprometido, não pago.", altura=36)],
        [linha_curva("fn_cs", "Evolução do empenho dos termos ativos", CS, [emp], "Empenhado acumulado (%)",
                     "Σ empenhado acumulado ÷ Σ repasse previsto, por retrato. Degraus = novas parcelas.",
                     filtro_sql="ic_retrato_completo"),
         numero("fn_k_parado", "Termos sem novo empenho há 12+ meses", PAR,
                m("COUNT(DISTINCT CASE WHEN situacao_parcelas = 'Sem novo empenho há 12+ meses' "
                  "THEN numero_proposta_transferegov END)", "Sem novo empenho 12+ meses"),
                "parados na mesma parcela", "Termos cuja última parcela de empenho tem 12 meses ou mais."),
         numero("fn_k_recente", "Termos com empenho nos últimos 6 meses", PAR,
                m("COUNT(DISTINCT CASE WHEN situacao_parcelas = 'Empenho nos últimos 6 meses' "
                  "THEN numero_proposta_transferegov END)", "Empenho recente"),
                "receberam nova parcela", "Termos que tiveram aumento de empenho nos últimos 6 retratos-mês.")],
        [faixa,
         barra("fn_situacao_uf", "Termos por situação das parcelas e UF", PAR, col("uf", "UF"),
               [m("COUNT(DISTINCT numero_proposta_transferegov)", "Termos")], "UF", "Termos",
               grupo=col("situacao_parcelas", "Situação"), largura=6, altura=55)],
        [tabela_linhas(
            "fn_t_parado", "Termos parados (sem empenho ou sem nova parcela há 12+ meses)", PAR,
            [col("numero_proposta_transferegov", "Proposta (TransfereGov)"), col("proponente_nome", "Proponente"),
             col("uf", "UF"), col("municipio", "Município"), col("situacao_parcelas", "Situação"),
             col("qt_parcelas_empenhadas", "Parcelas"), col("pct_empenhado", "% empenhado"),
             col("dt_ultimo_empenho", "Último empenho"), col("meses_sem_novo_empenho", "Meses sem nova parcela"),
             col("idade_meses", "Meses de termo"), col("valor_repasse", "Repasse (R$)"),
             col("valor_empenhado_acumulado", "Empenhado (R$)"), col("situacao_instrumento", "Situação do termo"),
             col("dt_assinatura", "Assinatura")],
            {"Proponente": TXT(220), "Situação": TXT(200), "% empenhado": N(",.1f", True),
             "Meses sem nova parcela": N(",d", True), "Repasse (R$)": N(",.0f"), "Empenhado (R$)": N(",.0f")},
            [cond("Meses sem nova parcela", ">=", 18, VERM), cond("Meses sem nova parcela", ">=", 12, AMAR)],
            ordem=[["meses_sem_novo_empenho", False]], filtro_sql=parado,
            descricao="Termos ativos sem empenho ou parados na mesma parcela há 12 meses ou mais. Quando o último "
                      "empenho já estava no primeiro retrato (mar/2025), os meses são um mínimo.", altura=70)],
    ]


ABAS = [
    # (título da aba no dashboard, sigla dos ids do layout, prefixo do nome dos charts, linhas)
    ("Pró-Moradia", "PM", "Pró-Moradia ▸ ", secao_pro_moradia),
    ("FNHIS Sub-50", "FN", "Sub-50 ▸ ", secao_fnhis),
]


# --------------------------------------------------------------------------
# construção

def garantir_dataset(api, cache, tabela):
    ds = cache["datasets"].get((SCHEMA, tabela))
    if ds is None:
        ds = api.criar("dataset", {"database": cache["db"], "schema": SCHEMA, "table_name": tabela})
        cache["datasets"][(SCHEMA, tabela)] = ds
    if ds != -1 and not api.dry_run:
        api.s.put(f"{api.base}/api/v1/dataset/{ds}/refresh", headers=api.h, timeout=120)
    return ds


def garantir_grafico(api, cache, g: Grafico, prefixo: str) -> int:
    ds = garantir_dataset(api, cache, g.dataset)
    params = {"datasource": f"{ds}__table", "viz_type": g.viz, "adhoc_filters": [], "time_range": "No filter",
              **g.params}
    nome = prefixo + g.titulo
    corpo = {"slice_name": nome, "viz_type": g.viz, "datasource_id": ds, "datasource_type": "table",
             "description": g.descricao, "params": json.dumps(params)}
    cid = cache["charts"].get(nome)
    if cid:
        api.atualizar("chart", cid, corpo)
    else:
        cid = api.criar("chart", corpo)
        cache["charts"][nome] = cid
    return cid


def acrescenta_secao(layout: dict, tab_id: str, sigla: str, blocos: list[list[dict]]) -> dict:
    """Remove as linhas desta seção (ids CS-<sigla>-*) e acrescenta as novas no fim da aba."""
    prefixo = f"CS-{sigla}-"
    for k in [k for k in layout if k.startswith(prefixo)]:
        layout.pop(k)
    layout[tab_id]["children"] = [c for c in layout[tab_id].get("children", []) if not c.startswith(prefixo)]
    pais = layout[tab_id].get("parents", []) + [tab_id]
    for i, linha in enumerate(blocos):
        row = f"{prefixo}ROW-{i:02d}"
        layout[tab_id]["children"].append(row)
        layout[row] = {"id": row, "type": "ROW", "children": [], "parents": pais,
                       "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for j, no in enumerate(linha):
            if no["tipo"] == "CHART":
                nid = f"{prefixo}CHART-{i:02d}-{j}"
                meta = {"chartId": no["chart"], "width": no["largura"], "height": no["altura"],
                        "sliceName": no["titulo"]}
            else:
                nid = f"{prefixo}MARKDOWN-{i:02d}-{j}"
                meta = {"width": no["largura"], "height": no["altura"], "code": no["md"]}
            layout[row]["children"].append(nid)
            layout[nid] = {"id": nid, "type": no["tipo"], "children": [], "parents": pais + [row], "meta": meta}
    return layout


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    api = Superset(dry_run=args.dry_run)
    cache = {
        "db": next(d["id"] for d in api.listar("database") if d["database_name"] == "Cidades"),
        "datasets": {(d.get("schema"), d["table_name"]): d["id"] for d in api.listar("dataset")},
        "charts": {c["slice_name"]: c["id"] for c in api.listar("chart")},
    }
    painel = api.get("dashboard", SLUG)
    if not painel:
        raise SystemExit(f"dashboard '{SLUG}' não encontrado.")
    layout = json.loads(painel.get("position_json") or "{}")
    charts_novos = []
    for titulo_aba, sigla, prefixo, secao in ABAS:
        tab_id = next((k for k, v in layout.items() if isinstance(v, dict) and v.get("type") == "TAB"
                       and (v.get("meta") or {}).get("text") == titulo_aba), None)
        if not tab_id:
            raise SystemExit(f"aba '{titulo_aba}' não encontrada no dashboard '{SLUG}'.")
        print(f"  aba {titulo_aba}")
        blocos = []
        for linha in secao():
            nos = []
            for item in linha:
                if isinstance(item, Texto):
                    nos.append({"tipo": "MARKDOWN", "md": item.md, "largura": item.largura, "altura": item.altura})
                else:
                    cid = garantir_grafico(api, cache, item, prefixo)
                    charts_novos.append(cid)
                    nos.append({"tipo": "CHART", "chart": cid, "largura": item.largura, "altura": item.altura,
                                "titulo": prefixo + item.titulo})
                    print(f"    {item.titulo} -> chart {cid}")
            blocos.append(nos)
        layout = acrescenta_secao(layout, tab_id, sigla, blocos)
    if api.dry_run:
        print("\n[dry-run] layout não gravado.")
        return
    api.atualizar("dashboard", painel["id"], {"position_json": json.dumps(layout)})
    for cid in charts_novos:
        info = api.get("chart", cid) or {}
        atuais = {d["id"] for d in (info.get("dashboards") or [])}
        if painel["id"] not in atuais:
            api.atualizar("chart", cid, {"dashboards": sorted(atuais | {painel["id"]})})
    print(f"\nseções gravadas no dashboard {painel['id']}: {len(charts_novos)} charts.")
    print(f"URL: {api.base}/superset/dashboard/{SLUG}/")


if __name__ == "__main__":
    main()
