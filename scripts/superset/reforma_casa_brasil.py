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
    "ouro_reforma_casa_brasil_ficha_reforma_dash",
    "ouro_reforma_casa_brasil_implementacao_dash",
    "ouro_reforma_casa_brasil_monitoramento_recursos_dash",
    "ouro_reforma_casa_brasil_mapa_uf",
    "ouro_reforma_casa_brasil_cobertura_perguntas_dash",
]

METRICAS = {
    "ouro_reforma_casa_brasil_ficha_reforma_dash": [
        ("sum__valor_financiado", "SUM(valor_financiado)"),
        ("sum__valor_investimento", "SUM(valor_investimento)"),
        ("sum__valor_recurso_proprio", "SUM(valor_recurso_proprio)"),
        ("sum__dias_atraso", "SUM(dias_atraso)"),
    ],
    "ouro_reforma_casa_brasil_implementacao_dash": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__valor_financiado_total", "SUM(valor_financiado_total)"),
        ("sum__valor_investimento_total", "SUM(valor_investimento_total)"),
        ("sum__valor_desconto_total", "SUM(valor_desconto_total)"),
        ("sum__valor_recurso_proprio_total", "SUM(valor_recurso_proprio_total)"),
        ("sum__valor_fgts_utilizado_total", "SUM(valor_fgts_utilizado_total)"),
        ("sum__quantidade_com_atraso", "SUM(quantidade_com_atraso)"),
        ("avg__prestacao_inicial_media", "SUM(prestacao_inicial_media * quantidade_contratos) / NULLIF(SUM(quantidade_contratos), 0)"),
        ("avg__taxa_juros_media", "SUM(taxa_juros_media * quantidade_contratos) / NULLIF(SUM(quantidade_contratos), 0)"),
        ("avg__prazo_medio_meses", "SUM(prazo_medio_meses * quantidade_contratos) / NULLIF(SUM(quantidade_contratos), 0)"),
    ],
    "ouro_reforma_casa_brasil_monitoramento_recursos_dash": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__valor_financiado_total", "SUM(valor_financiado_total)"),
        ("sum__quantidade_primeira_ocorrencia_serie", "SUM(quantidade_primeira_ocorrencia_serie)"),
        ("sum__valor_investimento_total", "SUM(valor_investimento_total)"),
        ("sum__valor_desconto_total", "SUM(valor_desconto_total)"),
        ("sum__valor_recurso_proprio_total", "SUM(valor_recurso_proprio_total)"),
        ("sum__valor_fgts_utilizado_total", "SUM(valor_fgts_utilizado_total)"),
        ("sum__quantidade_contratos_com_atraso", "SUM(quantidade_contratos_com_atraso)"),
    ],
    "ouro_reforma_casa_brasil_mapa_uf": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__quantidade_municipios", "SUM(quantidade_municipios)"),
        ("sum__valor_financiado_total", "SUM(valor_financiado_total)"),
        ("sum__valor_investimento_total", "SUM(valor_investimento_total)"),
        ("sum__quantidade_contratos_com_atraso", "SUM(quantidade_contratos_com_atraso)"),
    ],
}

CHARTS = [
    {"key": "ficha_financiado", "title": "Ficha — valor financiado", "dataset": "ouro_reforma_casa_brasil_ficha_reforma_dash", "viz_type": "big_number_total", "params": {"metric": "sum__valor_financiado", "subheader": "selecione uma reforma para o valor individual"}},
    {"key": "ficha_investimento", "title": "Ficha — valor de investimento", "dataset": "ouro_reforma_casa_brasil_ficha_reforma_dash", "viz_type": "big_number_total", "params": {"metric": "sum__valor_investimento", "subheader": "valor informado no contrato"}},
    {"key": "ficha_recurso_proprio", "title": "Ficha — recursos próprios", "dataset": "ouro_reforma_casa_brasil_ficha_reforma_dash", "viz_type": "big_number_total", "params": {"metric": "sum__valor_recurso_proprio", "subheader": "aporte informado"}},
    {"key": "ficha_atraso", "title": "Ficha — dias de atraso", "dataset": "ouro_reforma_casa_brasil_ficha_reforma_dash", "viz_type": "big_number_total", "params": {"metric": "sum__dias_atraso", "subheader": "posição administrativa"}},
    {"key": "ficha_reforma", "title": "Ficha da reforma selecionada", "dataset": "ouro_reforma_casa_brasil_ficha_reforma_dash", "viz_type": "table", "params": {"query_mode": "raw", "all_columns": ["identificador_reforma", "dt_contratacao", "dt_referencia", "uf", "municipio", "faixa_renda", "modalidade", "tipo_imovel", "classificacao_imovel", "tipo_desembolso", "tipo_garantia", "situacao_garantia", "sistema_amortizacao", "valor_financiado", "valor_desconto", "valor_recurso_proprio", "valor_fgts_utilizado", "valor_investimento", "valor_prestacao_inicial", "taxa_juros_nominal", "prazo_financiamento_meses", "dias_atraso", "situacao_administrativa", "ressalva_reforma"], "row_limit": 100}},
    {"key": "kpi_contratos", "title": "Contratos", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "big_number_total", "params": {"metric": "sum__quantidade_contratos", "subheader": "contratos na posição disponível"}},
    {"key": "kpi_financiado", "title": "Valor financiado", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "big_number_total", "params": {"metric": "sum__valor_financiado_total", "subheader": "financiamento contratado"}},
    {"key": "kpi_investimento", "title": "Valor de investimento", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "big_number_total", "params": {"metric": "sum__valor_investimento_total", "subheader": "investimento informado"}},
    {"key": "kpi_atraso", "title": "Contratos com atraso", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "big_number_total", "params": {"metric": "sum__quantidade_com_atraso", "subheader": "dias de atraso acima de zero"}},
    {"key": "kpi_desconto", "title": "Descontos", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "big_number_total", "params": {"metric": "sum__valor_desconto_total", "subheader": "descontos registrados"}},
    {"key": "kpi_recurso_proprio", "title": "Recursos próprios", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "big_number_total", "params": {"metric": "sum__valor_recurso_proprio_total", "subheader": "aporte da família"}},
    {"key": "kpi_fgts", "title": "FGTS utilizado", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "big_number_total", "params": {"metric": "sum__valor_fgts_utilizado_total", "subheader": "FGTS declarado"}},
    {"key": "kpi_prazo", "title": "Prazo médio", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "big_number_total", "params": {"metric": "avg__prazo_medio_meses", "subheader": "meses de financiamento"}},
    {"key": "kpi_municipios", "title": "Municípios alcançados", "dataset": "ouro_reforma_casa_brasil_mapa_uf", "viz_type": "big_number_total", "params": {"metric": "sum__quantidade_municipios", "subheader": "municípios na carteira"}},
    {"key": "mapa_brasil", "title": "Mapa do Brasil — contratos por UF", "dataset": "ouro_reforma_casa_brasil_mapa_uf", "viz_type": "country_map", "params": {"select_country": "brazil", "entity": "iso_3166_2", "metric": "sum__quantidade_contratos", "linear_color_scheme": "dark_blue"}},
    {"key": "ranking_uf", "title": "Ranking de contratação por UF", "dataset": "ouro_reforma_casa_brasil_mapa_uf", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["uf"], "metrics": ["sum__quantidade_contratos", "sum__quantidade_municipios", "sum__valor_financiado_total", "sum__valor_investimento_total"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 27}},
    {"key": "contratos_uf", "title": "Contratos por UF", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["uf"], "metrics": ["sum__quantidade_contratos", "sum__valor_financiado_total", "sum__valor_investimento_total"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 30}},
    {"key": "financiamento_faixa", "title": "Financiamento por faixa", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "pie", "params": {"groupby": ["faixa_renda"], "metric": "sum__valor_financiado_total", "donut": True, "row_limit": 30}},
    {"key": "contratos_modalidade", "title": "Contratos por modalidade", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "pie", "params": {"groupby": ["modalidade"], "metric": "sum__quantidade_contratos", "donut": True, "row_limit": 30}},
    {"key": "contratos_tipo_imovel", "title": "Contratos por tipo de imóvel", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "pie", "params": {"groupby": ["tipo_imovel"], "metric": "sum__quantidade_contratos", "donut": True, "row_limit": 30}},
    {"key": "implementacao", "title": "Carteira de contratos por município", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "table", "params": {"query_mode": "raw", "all_columns": ["uf", "municipio", "codigo_ibge", "faixa_renda", "modalidade", "tipo_imovel", "tipo_desembolso", "sistema_amortizacao", "quantidade_contratos", "valor_financiado_total", "valor_investimento_total", "valor_recurso_proprio_total", "valor_fgts_utilizado_total", "quantidade_com_atraso", "proporcao_com_atraso", "dt_referencia"], "row_limit": 5000}},
    {"key": "monitoramento", "title": "Carteira por competência", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_line", "params": {"x_axis": "dt_referencia", "metrics": ["sum__quantidade_contratos", "sum__quantidade_primeira_ocorrencia_serie"], "groupby": ["uf"], "time_grain_sqla": "P1M", "row_limit": 10000}},
    {"key": "recursos", "title": "Recursos contratados por competência", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_bar", "params": {"x_axis": "dt_referencia", "metrics": ["sum__valor_financiado_total"], "groupby": ["modalidade"], "time_grain_sqla": "P1M", "row_limit": 10000}},
    {"key": "cobertura", "title": "Cobertura das perguntas", "dataset": "ouro_reforma_casa_brasil_cobertura_perguntas_dash", "viz_type": "table", "params": {"query_mode": "raw", "all_columns": ["bloco", "pergunta", "situacao_dado", "fonte_necessaria"], "row_limit": 100}},
    {"key": "pergunta_perfil_territorio", "title": "Caracterização — contratos por UF", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["uf"], "metrics": ["sum__quantidade_contratos", "sum__valor_financiado_total"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 30}},
    {"key": "pergunta_perfil_faixa", "title": "Caracterização — financiamento por faixa de renda", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "pie", "params": {"groupby": ["faixa_renda"], "metric": "sum__valor_financiado_total", "donut": True, "row_limit": 30}},
    {"key": "pergunta_perfil_imovel", "title": "Caracterização — contratos por tipo de imóvel", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "pie", "params": {"groupby": ["tipo_imovel"], "metric": "sum__quantidade_contratos", "donut": True, "row_limit": 30}},
    {"key": "pergunta_implementacao", "title": "Implementação — carteira contratual por município", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "table", "params": {"query_mode": "raw", "all_columns": ["uf", "municipio", "faixa_renda", "modalidade", "tipo_imovel", "tipo_desembolso", "quantidade_contratos", "valor_financiado_total", "valor_investimento_total", "valor_recurso_proprio_total", "valor_fgts_utilizado_total", "quantidade_com_atraso", "proporcao_com_atraso", "dt_referencia"], "row_limit": 5000}},
    {"key": "pergunta_monitoramento", "title": "Monitoramento — carteira por competência", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_line", "params": {"x_axis": "dt_referencia", "metrics": ["sum__quantidade_contratos", "sum__quantidade_primeira_ocorrencia_serie"], "groupby": ["uf"], "time_grain_sqla": "P1M", "row_limit": 10000}},
    {"key": "pergunta_recursos", "title": "Monitoramento — recursos contratados por competência", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_bar", "params": {"x_axis": "dt_referencia", "metrics": ["sum__valor_financiado_total"], "groupby": ["modalidade"], "time_grain_sqla": "P1M", "row_limit": 10000}},
]

for chave, bloco in [
    ("cobertura_caracterizacao", "caracterizacao"),
    ("cobertura_acesso", "acesso"),
    ("cobertura_implementacao", "implementacao"),
    ("cobertura_resultado", "resultado"),
    ("cobertura_monitoramento", "monitoramento"),
    ("cobertura_avaliacao", "avaliacao"),
]:
    CHARTS.append({
        "key": chave,
        "title": f"Pergunta da oficina — {bloco.capitalize()}",
        "dataset": "ouro_reforma_casa_brasil_cobertura_perguntas_dash",
        "viz_type": "table",
        "params": {
            "query_mode": "raw",
            "all_columns": ["pergunta", "situacao_dado", "resposta_disponivel", "fonte_necessaria", "limitacao"],
            "adhoc_filters": [{"clause": "WHERE", "expressionType": "SIMPLE", "subject": "bloco", "operator": "==", "comparator": bloco}],
            "row_limit": 30,
        },
    })

GUIA_PERGUNTAS = """## Como ler: perguntas da oficina

Use **✓** para evidência disponível, **◐** para proxy/atributo administrativo e **—** para lacuna de fonte.

### Caracterização

- **Perfil de quem acessa / Faixa 1**: ✓ filtros de **UF**, **município**, **faixa de renda**, **modalidade** e **tipo de imóvel** nas abas *Visão geral*, *Território* e *Contratos e imóveis*.
- **Raça/cor, família → pessoa e vínculo CadÚnico**: — aguardam a materialização protegida do CadÚnico e o vínculo pseudonimizado.
- **O que está sendo reformado**: — a base contratual não traz serviço, item de obra, ventilação, placa solar ou projeto.

### Acesso

- **Conhece, entende, conseguiu acesso, desistiu e por quê; gargalos**: — exigem funil de propostas/recusas/desistências e pesquisa com usuários. A carteira contratada não mede quem ficou de fora.

### Implementação

- **Valor disponibilizado, prazo, juros, prestação, recursos próprios, FGTS e atraso**: ✓ aba *Implementação* e *Ficha da reforma*.
- **Adequação à necessidade**: ◐ há atributos financeiros; falta projeto/escopo da reforma e necessidade declarada.
- **Burocracia, documentação, facilidade, relação com banco e compreensão das condições**: — exigem pesquisa ou registros de atendimento.
- **Obra bem feita / no prazo**: — não há medição, vistoria ou cronograma físico nesta fonte.

### Resultados

- **Quanto foi contratado/investido**: ✓ cards e tabelas de *Visão geral* e *Implementação*; são valores contratuais, não gasto comprovado.
- **Conclusão, conforto, segurança, salubridade, redução da inadequação e impacto financeiro pós-obra**: — exigem acompanhamento/vistoria/pesquisa após a reforma. CadÚnico futuro poderá compor apenas linha de base.

### Monitoramento

- **Carteira e recursos por competência**: ✓ aba *Monitoramento*; mostra posições administrativas por data de referência.
- **Execução orçamentária, pagamento e medição física**: — não estão na base atual e não devem ser inferidos da contratação.

> **Pergunta estrela:** hoje o painel caracteriza a carteira e monitora contratos. Ainda não permite estimar quais características causam acesso ou melhoria habitacional, pois faltam funil de acesso, tipo de reforma e resultados pós-obra.
"""

COMO_LER_CARACTERIZACAO = """### 1. Caracterização — quem acessa e o que está sendo reformado?

**Como ler**

- Os gráficos ao lado mostram a carteira contratada por **UF**, **faixa de renda** e **tipo de imóvel**.
- Eles respondem ao perfil administrativo disponível: território, faixa, modalidade e imóvel.
- **Não** identificam o serviço físico da reforma: a base não registra ventilação, placa solar, item de obra, orçamento ou projeto.

⚠️ Raça/cor, composição familiar e vínculo com CadÚnico dependem da camada protegida ainda não materializada.
"""

COMO_LER_ACESSO = """### 2. Acesso — quem conhece, consegue entrar ou desiste?

**Como ler**

Esta tabela não usa zero como resposta. Ela mostra por que a carteira contratada **não permite** medir conhecimento do programa, tentativa de acesso, recusa, desistência, motivo ou gargalos.

Para responder, precisamos do funil de propostas e atendimento do agente financeiro, além de pesquisa com usuários. Quando o CadÚnico protegido estiver materializado, será possível medir o vínculo dos contratados — não as barreiras de quem ficou de fora.
"""

COMO_LER_IMPLEMENTACAO = """### 3. Implementação — o financiamento foi adequado e fácil de contratar?

**Como ler**

- Cada linha agrega contratos por município, faixa, modalidade, imóvel e desembolso.
- **Valor financiado, investimento, recursos próprios, FGTS, prazo, juros, prestação e atraso** são atributos administrativos observáveis.
- Eles permitem caracterizar o contrato; não demonstram burocracia, documentação, facilidade de contratação, relação com banco ou compreensão das condições.

⚠️ Adequação à necessidade exige também o projeto e o escopo físico da reforma; qualidade e prazo de obra exigem vistoria/medição.
"""

COMO_LER_RESULTADO = """### 4. Resultados — o que mudou depois da reforma?

**Como ler**

Os valores contratuais mostram o que foi financiado/informado, **não** o gasto efetivo nem a reforma executada.

Esta tabela explicita as lacunas para: tipo de reforma, conclusão, conforto, segurança, salubridade, redução da inadequação e impacto financeiro posterior. Esses resultados exigem medição/vistoria e pesquisa ou acompanhamento pós-obra.
"""

COMO_LER_MONITORAMENTO = """### 5. Monitoramento — como evolui a carteira e os recursos?

**Como ler**

- Cada ponto é uma **posição administrativa** na data de referência; não se deve somar todos os meses entre si.
- A série mostra contratos e primeiras ocorrências por UF; a aba *Monitoramento* mostra também valores contratados.
- Não representa liquidação, pagamento, desembolso físico nem medição da obra.

⚠️ Para execução orçamentária/física é necessária fonte específica do agente financeiro ou orçamento.
"""

COMO_LER_ESTRELA = """### Pergunta estrela — acesso e melhoria habitacional

**A pesquisa permite identificar quais características aumentam ou reduzem a probabilidade de acesso e de produção de melhoria habitacional?**

Hoje, o painel caracteriza a carteira e monitora contratos. Ainda não estima causalidade ou impacto: faltam o funil de acesso, o tipo/escopo da reforma e observações comparáveis após a obra.
"""


def datasets(api: Superset, database_id: int) -> dict[str, int]:
    existentes = {(x.get("schema"), x.get("table_name")): x["id"] for x in api.list("dataset")}
    ids: dict[str, int] = {}
    for nome in DATASETS:
        chave = (SCHEMA, nome)
        if chave not in existentes:
            existentes[chave] = api.create("dataset", {"database": database_id, "schema": SCHEMA, "table_name": nome})["id"]
        ids[nome] = existentes[chave]
        if not api.dry_run:
            resposta = api.session.put(f"{api.base_url}/api/v1/dataset/{ids[nome]}/refresh", timeout=30)
            if resposta.status_code == 405:
                resposta = api.session.post(f"{api.base_url}/api/v1/dataset/{ids[nome]}/refresh", timeout=30)
            resposta.raise_for_status()
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
    paginas = [
        ("Visão geral", [["kpi_contratos", "kpi_financiado", "kpi_investimento", "kpi_atraso"], ["contratos_uf", "financiamento_faixa"], ["contratos_modalidade"]]),
        ("Ficha da reforma", [["ficha_financiado", "ficha_investimento", "ficha_recurso_proprio", "ficha_atraso"], ["ficha_reforma"]]),
        ("Território", [["mapa_brasil"], ["kpi_municipios", "ranking_uf"], ["implementacao"]]),
        ("Contratos e imóveis", [["contratos_tipo_imovel", "contratos_modalidade"], ["implementacao"]]),
        ("Implementação", [["kpi_desconto", "kpi_recurso_proprio", "kpi_fgts", "kpi_prazo"], ["implementacao"]]),
        ("Monitoramento", [["monitoramento"], ["recursos"]]),
        ("Perguntas da oficina", [
            [{"markdown": "## 1. Qual é o perfil de quem acessa e quais características do imóvel constam na contratação?"}],
            [{"markdown": COMO_LER_CARACTERIZACAO}, "pergunta_perfil_territorio"],
            [{"markdown": "### Como ler — faixa de renda\n\nA pizza distribui o **valor financiado** entre as faixas informadas no contrato. É perfil da carteira contratada; não mede renda posterior à reforma."}, "pergunta_perfil_faixa"],
            [{"markdown": "### Como ler — categoria do imóvel na fonte\n\nA carga recebida do Reforma Casa Brasil registra apenas o **código 5** para todos os contratos e não trouxe o dicionário que o traduz. Por isso, ele é exibido como código técnico, **não como casa, apartamento ou tipo de reforma**. Também não informa o serviço executado — ventilação, telhado ou placa solar."}, "pergunta_perfil_imovel"],
            [{"markdown": "## 2. Quem conhece, entende, consegue acessar ou desiste do programa — e por quê?"}],
            [{"markdown": COMO_LER_ACESSO}, "cobertura_acesso"],
            [{"markdown": "## 3. O financiamento é adequado à necessidade e como está a implementação contratual?"}],
            [{"markdown": COMO_LER_IMPLEMENTACAO}, "pergunta_implementacao"],
            [{"markdown": "## 4. O que foi reformado, quanto foi gasto e quais resultados ocorreram após a obra?"}],
            [{"markdown": COMO_LER_RESULTADO}, "cobertura_resultado"],
            [{"markdown": "## 5. Como a carteira e os recursos evoluem ao longo do tempo?"}],
            [{"markdown": COMO_LER_MONITORAMENTO}, "pergunta_monitoramento"],
            [{"markdown": "## Pergunta estrela — quais características aumentam ou reduzem acesso e melhoria habitacional?"}],
            [{"markdown": COMO_LER_ESTRELA}, "cobertura_avaliacao"],
            [{"markdown": "## Matriz completa de cobertura\n\nTodas as perguntas, fontes necessárias e limitações usadas na leitura do painel."}],
            [{"markdown": "### Como ler\n\nA tabela da direita consolida o que é respondido, o que é proxy e o que precisa de nova fonte."}, "cobertura"],
        ]),
    ]
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
                if isinstance(chave, dict) and "markdown" in chave:
                    node = f"MARKDOWN-REF-{pagina}-{linha}-{posicao}"
                    tipo = "MARKDOWN"
                    # Cabeçalhos de pergunta ocupam a largura toda; os pares
                    # explicação + gráfico ficam rigorosamente 50% / 50%.
                    meta = {"width": 12 // len(chaves), "height": 10 if len(chaves) == 1 else 48, "code": chave["markdown"]}
                else:
                    node = f"CHART-{ids[chave]}"
                    tipo = "CHART"
                    meta = {"chartId": ids[chave], "width": 12 // len(chaves), "height": 48, "index": f"{pagina}{linha}{posicao}"}
                estrutura[row]["children"].append(node)
                estrutura[node] = {"id": node, "type": tipo, "children": [], "parents": ["ROOT_ID", tabs, tab, row], "meta": meta}
    return json.dumps(estrutura)


def dashboard(api: Superset, ids: dict[str, int], datasets_ids: dict[str, int]) -> None:
    def seletor(indice: int, nome: str, coluna: str, tabelas: list[str], aba: int) -> dict:
        return {
            "id": f"NATIVE_FILTER-RCB-{indice}", "name": nome,
            "filterType": "filter_select", "type": "NATIVE_FILTER",
            "targets": [{"datasetId": datasets_ids[t], "column": {"name": coluna}} for t in tabelas],
            # Estado vazio explícito: evita o aviso "Filter value is required"
            # nesta versão do Superset quando o painel abre sem seleção.
            "defaultDataMask": {"extraFormData": {}, "filterState": {"value": []}, "ownState": {}},
            "controlValues": {"multiSelect": True, "enableEmptyFilter": True, "defaultToFirstItem": False, "searchAllOptions": True, "inverseSelection": False},
            "required": False,
            "scope": {"rootPath": ["ROOT_ID", "TABS-REFORMA", f"TAB-REF-{aba}"], "excluded": []},
            "cascadeParentIds": [],
        }

    def periodo(indice: int, aba: int) -> dict:
        return {
            "id": f"NATIVE_FILTER-RCB-{indice}", "name": "Período",
            "filterType": "filter_time", "type": "NATIVE_FILTER",
            "targets": [{"datasetId": datasets_ids["ouro_reforma_casa_brasil_monitoramento_recursos_dash"], "column": {"name": "dt_referencia"}}],
            "defaultDataMask": {"extraFormData": {"time_range": "No filter"}, "filterState": {"value": "No filter"}},
            "controlValues": {"enableEmptyFilter": True, "defaultToFirstItem": False},
            "required": False,
            "scope": {"rootPath": ["ROOT_ID", "TABS-REFORMA", f"TAB-REF-{aba}"], "excluded": []},
            "cascadeParentIds": [],
        }

    implementacao = ["ouro_reforma_casa_brasil_implementacao_dash"]
    monitoramento = ["ouro_reforma_casa_brasil_monitoramento_recursos_dash"]
    filtros = [
        seletor(1, "UF", "uf", implementacao, 0),
        seletor(2, "Faixa de renda", "faixa_renda", implementacao, 0),
        seletor(3, "Modalidade", "modalidade", implementacao, 0),
        seletor(4, "UF", "uf", ["ouro_reforma_casa_brasil_ficha_reforma_dash"], 1),
        seletor(5, "Município", "municipio", ["ouro_reforma_casa_brasil_ficha_reforma_dash"], 1),
        seletor(6, "Reforma / contrato", "identificador_reforma", ["ouro_reforma_casa_brasil_ficha_reforma_dash"], 1),
        seletor(7, "UF", "uf", ["ouro_reforma_casa_brasil_mapa_uf", *implementacao], 2),
        seletor(8, "Município", "municipio", implementacao, 2),
        seletor(9, "UF", "uf", implementacao, 3),
        seletor(10, "Município", "municipio", implementacao, 3),
        seletor(11, "Tipo de imóvel", "tipo_imovel", implementacao, 3),
        seletor(12, "Faixa de renda", "faixa_renda", implementacao, 3),
        seletor(13, "UF", "uf", implementacao, 4),
        seletor(14, "Município", "municipio", implementacao, 4),
        seletor(15, "Tipo de desembolso", "tipo_desembolso", implementacao, 4),
        periodo(16, 5),
        seletor(17, "UF", "uf", monitoramento, 5),
        seletor(18, "Faixa de renda", "faixa_renda", monitoramento, 5),
    ]
    payload = {"dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG, "published": True, "position_json": layout(ids), "json_metadata": json.dumps({"refresh_frequency": 0, "native_filter_configuration": filtros, "show_native_filters": True})}
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


def dashboard_perguntas(api: Superset, ids: dict[str, int]) -> None:
    """Painel orientado pelas perguntas da oficina, no padrão mcid-perguntas."""
    slug = "perguntas-reforma-casa-brasil"
    titulo = "Perguntas: Reforma Casa Brasil"
    tabs = "TABS-PERGUNTAS-RCB"
    estrutura = {
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": [tabs], "parents": [], "meta": {}},
        tabs: {"id": tabs, "type": "TABS", "children": [], "parents": ["ROOT_ID"], "meta": {}},
    }

    paginas = [
        ("Comece aqui", [
            ("charts", ["kpi_contratos", "kpi_financiado", "kpi_investimento"]),
            ("heading", "## O que este painel responde"),
            ("pair", """### Como ler

Este painel parte das perguntas da oficina. Ele separa o que é evidência administrativa disponível do que ainda depende de CadÚnico protegido, funil de acesso, projeto de reforma, vistoria ou pesquisa pós-obra.

**Base atual:** contratos administrativos do Reforma Casa Brasil. Valores são contratuais/informados; não são, por si, gasto efetivo ou obra concluída.""", "cobertura"),
        ]),
        ("Caracterização", [
            ("heading", "## 1. Caracterização — qual é o perfil dos beneficiários e quais tipos de reforma foram realizados?"),
            ("pair", COMO_LER_CARACTERIZACAO, "pergunta_perfil_territorio"),
            ("pair", """### Como ler — faixa de renda

O gráfico distribui o **valor financiado** entre as faixas declaradas na carteira contratada. Não representa renda após a reforma.""", "pergunta_perfil_faixa"),
            ("pair", """### Como ler — categoria do imóvel na fonte

A carga recebida registra apenas o **código 5** para todos os contratos e não trouxe o dicionário que o traduz. Ele é um código técnico da fonte, não uma afirmação de que o imóvel seja casa, apartamento ou um tipo de reforma. Também não informa o serviço executado — ventilação, placa solar, telhado ou outro item físico.""", "pergunta_perfil_imovel"),
        ]),
        ("Acesso", [
            ("heading", "## 2. Acesso — as famílias conhecem, entendem e conseguem acessar o programa? Quem desiste, por quê e quais são os gargalos?"),
            ("pair", COMO_LER_ACESSO, "cobertura_acesso"),
        ]),
        ("Implementação", [
            ("charts", ["kpi_desconto", "kpi_recurso_proprio", "kpi_fgts", "kpi_prazo"]),
            ("heading", "## 3. Implementação — o financiamento é adequado à necessidade? Como estão burocracia, facilidade de contratação, documentação, relação com a instituição financeira, valor disponibilizado e compreensão das condições?"),
            ("pair", COMO_LER_IMPLEMENTACAO, "pergunta_implementacao"),
            ("pair", """### Consulta individual

Na aba **Ficha da reforma**, filtre UF, município e contrato para ver valores, garantia, modalidade, prazo, juros e atraso do registro selecionado.""", "ficha_reforma"),
        ]),
        ("Resultados", [
            ("heading", "## 4. Resultados — o que foi reformado, quanto foi gasto, a obra foi concluída e houve aumento de conforto, redução da inadequação, segurança ou salubridade?"),
            ("pair", COMO_LER_RESULTADO, "cobertura_resultado"),
        ]),
        ("Monitoramento", [
            ("heading", "## 5. Monitoramento — como a carteira contratada e os recursos evoluem ao longo do tempo?"),
            ("pair", COMO_LER_MONITORAMENTO, "pergunta_monitoramento"),
            ("pair", """### Como ler — recursos contratados

As barras mostram o valor contratado por competência e modalidade. Não devem ser lidas como liquidação, pagamento ou execução física da obra.""", "pergunta_recursos"),
        ]),
        ("Pergunta estrela", [
            ("heading", "## Pergunta estrela — a pesquisa permite identificar quais características aumentam ou reduzem a probabilidade de acesso e de produção de melhoria habitacional?"),
            ("pair", COMO_LER_ESTRELA, "cobertura_avaliacao"),
        ]),
    ]

    for pagina, (nome, blocos) in enumerate(paginas):
        tab = f"TAB-PERGUNTAS-RCB-{pagina}"
        estrutura[tabs]["children"].append(tab)
        estrutura[tab] = {"id": tab, "type": "TAB", "children": [], "parents": ["ROOT_ID", tabs], "meta": {"text": nome, "defaultText": nome, "placeholder": nome}}
        for linha, bloco in enumerate(blocos):
            row = f"ROW-PERGUNTAS-RCB-{pagina}-{linha}"
            estrutura[tab]["children"].append(row)
            estrutura[row] = {"id": row, "type": "ROW", "children": [], "parents": ["ROOT_ID", tabs, tab], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            tipo, conteudo, *resto = bloco
            if tipo == "heading":
                node = f"MARKDOWN-PERGUNTAS-RCB-{pagina}-{linha}"
                estrutura[row]["children"].append(node)
                estrutura[node] = {"id": node, "type": "MARKDOWN", "children": [], "parents": ["ROOT_ID", tabs, tab, row], "meta": {"width": 12, "height": 10, "code": conteudo}}
            elif tipo == "charts":
                for posicao, chave in enumerate(conteudo):
                    node = f"CHART-PERGUNTAS-RCB-{ids[chave]}-{pagina}-{linha}"
                    estrutura[row]["children"].append(node)
                    estrutura[node] = {"id": node, "type": "CHART", "children": [], "parents": ["ROOT_ID", tabs, tab, row], "meta": {"chartId": ids[chave], "width": 12 // len(conteudo), "height": 28, "index": f"{pagina}{linha}{posicao}"}}
            else:  # pair: explicação à esquerda, gráfico à direita
                markdown = f"MARKDOWN-PERGUNTAS-RCB-{pagina}-{linha}"
                chart = f"CHART-PERGUNTAS-RCB-{ids[resto[0]]}-{pagina}-{linha}"
                estrutura[row]["children"].extend([markdown, chart])
                estrutura[markdown] = {"id": markdown, "type": "MARKDOWN", "children": [], "parents": ["ROOT_ID", tabs, tab, row], "meta": {"width": 4, "height": 50, "code": conteudo}}
                estrutura[chart] = {"id": chart, "type": "CHART", "children": [], "parents": ["ROOT_ID", tabs, tab, row], "meta": {"chartId": ids[resto[0]], "width": 8, "height": 50, "index": f"{pagina}{linha}1"}}

    payload = {
        "dashboard_title": titulo,
        "slug": slug,
        "published": True,
        "position_json": json.dumps(estrutura),
        "json_metadata": json.dumps({"refresh_frequency": 0, "show_native_filters": False}),
    }
    dashboard_id = api.dashboard_id(slug)
    if dashboard_id:
        api.update("dashboard", dashboard_id, payload)
    else:
        dashboard_id = api.create("dashboard", payload)["id"]
    if api.dry_run:
        return
    for chart_id in ids.values():
        atual = api.session.get(f"{api.base_url}/api/v1/chart/{chart_id}", timeout=30).json()["result"]
        destinos = {item["id"] for item in atual.get("dashboards", [])}
        if dashboard_id not in destinos:
            api.update("chart", chart_id, {"dashboards": sorted(destinos | {dashboard_id})})


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--perguntas", action="store_true")
    args = parser.parse_args()
    load_dotenv("local.env", override=False)
    load_dotenv(".env", override=False)
    api = Superset(env("SUPERSET_URL", "SUPERSET_HOST_PORT"), env("SUPERSET_USERNAME"), env("SUPERSET_PASSWORD"), args.dry_run)
    ids = datasets(api, get_or_create_database(api))
    charts_ids = charts(api, ids)
    if args.perguntas:
        dashboard_perguntas(api, charts_ids)
    else:
        dashboard(api, charts_ids, ids)


if __name__ == "__main__":
    main()
