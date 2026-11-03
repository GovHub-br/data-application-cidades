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
    "ouro_reforma_casa_brasil_acesso_dash",
    "ouro_reforma_casa_brasil_resultado_dash",
    "ouro_reforma_casa_brasil_trabalho_dash",
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
        ("avg__valor_financiado", "SUM(valor_financiado_total) / NULLIF(SUM(quantidade_contratos), 0)"),
        ("taxa__atraso", "SUM(quantidade_com_atraso)::numeric / NULLIF(SUM(quantidade_contratos), 0)"),
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
        ("taxa__atraso", "SUM(quantidade_contratos_com_atraso)::numeric / NULLIF(SUM(quantidade_contratos), 0)"),
        ("avg__valor_financiado", "SUM(valor_financiado_total) / NULLIF(SUM(quantidade_contratos), 0)"),
    ],
    "ouro_reforma_casa_brasil_mapa_uf": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__quantidade_municipios", "SUM(quantidade_municipios)"),
        ("sum__valor_financiado_total", "SUM(valor_financiado_total)"),
        ("sum__valor_investimento_total", "SUM(valor_investimento_total)"),
        ("sum__quantidade_contratos_com_atraso", "SUM(quantidade_contratos_com_atraso)"),
    ],
    "ouro_reforma_casa_brasil_acesso_dash": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__quantidade_encontrada_cadunico", "SUM(quantidade_encontrada_cadunico)"),
        ("taxa__vinculo_cadunico", "SUM(quantidade_encontrada_cadunico)::numeric / NULLIF(SUM(quantidade_contratos), 0)"),
        ("sum__quantidade_familias_cadunico", "SUM(quantidade_familias_cadunico)"),
    ],
    "ouro_reforma_casa_brasil_resultado_dash": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__quantidade_com_inadequacao", "SUM(quantidade_com_inadequacao_observavel_na_linha_de_base)"),
        ("taxa__inadequacao_linha_base", "SUM(quantidade_com_inadequacao_observavel_na_linha_de_base)::numeric / NULLIF(SUM(quantidade_contratos), 0)"),
        ("sum__quantidade_sem_agua", "SUM(quantidade_sem_agua_canalizada)"),
        ("sum__quantidade_sem_banheiro", "SUM(quantidade_sem_banheiro)"),
        ("sum__quantidade_escoamento_precario", "SUM(quantidade_escoamento_precario)"),
    ],
    "ouro_reforma_casa_brasil_trabalho_dash": [
        ("sum__quantidade_contratos", "SUM(quantidade_contratos)"),
        ("sum__quantidade_vinculada_cadunico", "SUM(quantidade_vinculada_cadunico)"),
        ("sum__quantidade_renda_construcao", "SUM(quantidade_renda_construcao)"),
        ("sum__quantidade_renda_servicos_gerais", "SUM(quantidade_renda_servicos_gerais)"),
        ("sum__quantidade_renda_vendas", "SUM(quantidade_renda_vendas)"),
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
    {"key": "kpi_ticket", "title": "Ticket médio", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "big_number_total", "params": {"metric": "avg__valor_financiado", "subheader": "valor financiado por contrato"}},
    {"key": "kpi_vinculo_cadunico", "title": "Vínculo com CadÚnico", "dataset": "ouro_reforma_casa_brasil_acesso_dash", "viz_type": "big_number_total", "params": {"metric": "taxa__vinculo_cadunico", "subheader": "contratos encontrados por CPF/NIS HMAC"}},
    {"key": "kpi_familias_cadunico", "title": "Famílias CadÚnico", "dataset": "ouro_reforma_casa_brasil_acesso_dash", "viz_type": "big_number_total", "params": {"metric": "sum__quantidade_familias_cadunico", "subheader": "famílias distintas em células protegidas"}},
    {"key": "kpi_inadequacao", "title": "Linha de base: inadequação", "dataset": "ouro_reforma_casa_brasil_resultado_dash", "viz_type": "big_number_total", "params": {"metric": "taxa__inadequacao_linha_base", "subheader": "proxy anterior; não mede efeito pós-reforma"}},
    {"key": "mapa_brasil", "title": "Mapa do Brasil — contratos por UF", "dataset": "ouro_reforma_casa_brasil_mapa_uf", "viz_type": "country_map", "params": {"select_country": "brazil", "entity": "iso_3166_2", "metric": "sum__quantidade_contratos", "linear_color_scheme": "dark_blue"}},
    {"key": "ranking_uf", "title": "Ranking de contratação por UF", "dataset": "ouro_reforma_casa_brasil_mapa_uf", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["uf"], "metrics": ["sum__quantidade_contratos", "sum__quantidade_municipios", "sum__valor_financiado_total", "sum__valor_investimento_total"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 27}},
    {"key": "contratos_uf", "title": "Contratos por UF", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["uf"], "metrics": ["sum__quantidade_contratos", "sum__valor_financiado_total", "sum__valor_investimento_total"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 30}},
    {"key": "financiamento_faixa", "title": "Financiamento por faixa", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "pie", "params": {"groupby": ["faixa_renda"], "metric": "sum__valor_financiado_total", "donut": True, "row_limit": 30}},
    {"key": "contratos_modalidade", "title": "Contratos por modalidade", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "pie", "params": {"groupby": ["modalidade"], "metric": "sum__quantidade_contratos", "donut": True, "row_limit": 30}},
    {"key": "contratos_tipo_imovel", "title": "Contratos por tipo de imóvel", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "pie", "params": {"groupby": ["tipo_imovel"], "metric": "sum__quantidade_contratos", "donut": True, "row_limit": 30}},
    {"key": "implementacao", "title": "Carteira de contratos por município", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "table", "params": {"query_mode": "raw", "all_columns": ["uf", "municipio", "codigo_ibge", "faixa_renda", "modalidade", "tipo_imovel", "tipo_desembolso", "sistema_amortizacao", "quantidade_contratos", "valor_financiado_total", "valor_investimento_total", "valor_recurso_proprio_total", "valor_fgts_utilizado_total", "quantidade_com_atraso", "proporcao_com_atraso", "dt_referencia"], "row_limit": 5000}},
    {"key": "monitoramento", "title": "Carteira por competência", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_line", "params": {"x_axis": "dt_referencia", "metrics": ["sum__quantidade_contratos", "sum__quantidade_primeira_ocorrencia_serie"], "groupby": ["uf"], "time_grain_sqla": "P1M", "row_limit": 10000}},
    {"key": "recursos", "title": "Recursos contratados por competência", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_bar", "params": {"x_axis": "dt_referencia", "metrics": ["sum__valor_financiado_total"], "groupby": ["modalidade"], "time_grain_sqla": "P1M", "row_limit": 10000}},
    {"key": "evolucao_geral", "title": "Evolução dos financiamentos e contratos", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_line", "params": {"x_axis": "dt_referencia", "metrics": ["sum__quantidade_contratos", "sum__valor_financiado_total"], "time_grain_sqla": "P1M", "row_limit": 10000}},
    {"key": "contratos_mes_faixa", "title": "Contratos por mês e faixa de renda", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_bar", "params": {"x_axis": "dt_referencia", "metrics": ["sum__quantidade_contratos"], "groupby": ["faixa_renda"], "time_grain_sqla": "P1M", "row_limit": 10000, "stack": "Stack"}},
    {"key": "valor_mes_faixa", "title": "Valor financiado por mês e faixa de renda", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_bar", "params": {"x_axis": "dt_referencia", "metrics": ["sum__valor_financiado_total"], "groupby": ["faixa_renda"], "time_grain_sqla": "P1M", "row_limit": 10000, "stack": "Stack"}},
    {"key": "ticket_faixa", "title": "Ticket médio por faixa de renda", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "echarts_timeseries_bar", "params": {"x_axis": "faixa_renda", "metrics": ["avg__valor_financiado"], "row_limit": 20, "orientation": "horizontal", "show_legend": False}},
    {"key": "atraso_uf", "title": "Taxa de atraso por UF", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "echarts_timeseries_bar", "params": {"x_axis": "uf", "metrics": ["taxa__atraso"], "row_limit": 30, "orientation": "horizontal", "show_legend": False}},
    {"key": "prazo_uf", "title": "Prazo médio de financiamento por UF", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "echarts_timeseries_bar", "params": {"x_axis": "uf", "metrics": ["avg__prazo_medio_meses"], "row_limit": 30, "orientation": "horizontal", "show_legend": False}},
    {"key": "atraso_serie", "title": "Histórico de contratos com atraso", "dataset": "ouro_reforma_casa_brasil_monitoramento_recursos_dash", "viz_type": "echarts_timeseries_line", "params": {"x_axis": "dt_referencia", "metrics": ["sum__quantidade_contratos_com_atraso", "taxa__atraso"], "time_grain_sqla": "P1M", "row_limit": 10000}},
    {"key": "municipios_top", "title": "Top municípios — contratos e valor financiado", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["municipio", "uf"], "metrics": ["sum__quantidade_contratos", "sum__valor_financiado_total", "avg__valor_financiado", "taxa__atraso"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 100}},
    {"key": "municipios_faixa", "title": "Municípios por faixa de renda", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["municipio", "uf", "faixa_renda"], "metrics": ["sum__quantidade_contratos", "sum__valor_financiado_total"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 500}},
    {"key": "qualidade_fontes", "title": "Cobertura, lacunas e fontes necessárias", "dataset": "ouro_reforma_casa_brasil_cobertura_perguntas_dash", "viz_type": "table", "params": {"query_mode": "raw", "all_columns": ["bloco", "pergunta", "situacao_dado", "resposta_disponivel", "fonte_necessaria", "limitacao"], "row_limit": 100}},
    {"key": "cobertura", "title": "Cobertura das perguntas", "dataset": "ouro_reforma_casa_brasil_cobertura_perguntas_dash", "viz_type": "table", "params": {"query_mode": "raw", "all_columns": ["bloco", "pergunta", "situacao_dado", "fonte_necessaria"], "row_limit": 100}},
    {"key": "pergunta_perfil_territorio", "title": "Caracterização — contratos por UF", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["uf"], "metrics": ["sum__quantidade_contratos", "sum__valor_financiado_total"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 30}},
    {"key": "pergunta_perfil_faixa", "title": "Caracterização — financiamento por faixa de renda", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "pie", "params": {"groupby": ["faixa_renda"], "metric": "sum__valor_financiado_total", "donut": True, "row_limit": 30}},
    {"key": "pergunta_perfil_imovel", "title": "Caracterização — contratos por tipo de imóvel", "dataset": "ouro_reforma_casa_brasil_implementacao_dash", "viz_type": "pie", "params": {"groupby": ["tipo_imovel"], "metric": "sum__quantidade_contratos", "donut": True, "row_limit": 30}},
    {"key": "pergunta_cadunico", "title": "Acesso — vínculo pseudonimizado com CadÚnico", "dataset": "ouro_reforma_casa_brasil_acesso_dash", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["uf", "faixa_renda", "raca_cor", "sexo"], "metrics": ["sum__quantidade_contratos", "sum__quantidade_encontrada_cadunico", "taxa__vinculo_cadunico"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 500}},
    {"key": "pergunta_trabalho", "title": "Caracterização — atividade laboral declarada", "dataset": "ouro_reforma_casa_brasil_trabalho_dash", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["codigo_trabalhou_semana", "codigo_ocupacao_principal", "faixa_renda_trabalho"], "metrics": ["sum__quantidade_contratos", "sum__quantidade_vinculada_cadunico", "sum__quantidade_renda_construcao", "sum__quantidade_renda_servicos_gerais", "sum__quantidade_renda_vendas"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 500}},
    {"key": "pergunta_linha_base", "title": "Resultados — condições domiciliares na linha de base", "dataset": "ouro_reforma_casa_brasil_resultado_dash", "viz_type": "table", "params": {"query_mode": "aggregate", "groupby": ["uf", "faixa_renda", "raca_cor"], "metrics": ["sum__quantidade_contratos", "sum__quantidade_com_inadequacao", "taxa__inadequacao_linha_base", "sum__quantidade_sem_agua", "sum__quantidade_sem_banheiro", "sum__quantidade_escoamento_precario"], "order_by_cols": ['["sum__quantidade_contratos", false]'], "row_limit": 500}},
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

- **Perfil de quem acessa / Faixa 1**: ✓ filtros de **UF**, **município** e **faixa de renda** nas abas *Visão geral*, *Análises*, *Cruzamentos* e *Perguntas SNH*.
- **Raça/cor, família → pessoa e vínculo CadÚnico**: ✓ exibidos somente em agregados com supressão de células pequenas; o vínculo usa CPF/NIS HMAC.
- **O que está sendo reformado**: — a base contratual não traz serviço, item de obra, ventilação, placa solar ou projeto.

### Acesso

- **Conhece, entende, conseguiu acesso, desistiu e por quê; gargalos**: — exigem funil de propostas/recusas/desistências e pesquisa com usuários. A carteira contratada não mede quem ficou de fora.

### Implementação

- **Valor disponibilizado, prazo, juros, prestação, recursos próprios, FGTS e atraso**: ✓ abas *Análises*, *Mapa* (ficha individual) e *Perguntas SNH*.
- **Adequação à necessidade**: ◐ há atributos financeiros; falta projeto/escopo da reforma e necessidade declarada.
- **Burocracia, documentação, facilidade, relação com banco e compreensão das condições**: — exigem pesquisa ou registros de atendimento.
- **Obra bem feita / no prazo**: — não há medição, vistoria ou cronograma físico nesta fonte.

### Resultados

- **Quanto foi contratado/investido**: ✓ cards e tabelas de *Visão geral* e *Análises*; são valores contratuais, não gasto comprovado.
- **Conclusão, conforto, segurança, salubridade, redução da inadequação e impacto financeiro pós-obra**: — exigem acompanhamento/vistoria/pesquisa após a reforma. O CadÚnico compõe apenas linha de base.

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

⚠️ Raça/cor, composição familiar e vínculo com CadÚnico são exibidos apenas em agregados
com supressão de células pequenas; CPF, NIS, nome e endereço não entram no painel.
"""

COMO_LER_ACESSO = """### 2. Acesso — quem conhece, consegue entrar ou desiste?

**Como ler**

Esta tabela não usa zero como resposta. Ela mostra por que a carteira contratada **não permite** medir conhecimento do programa, tentativa de acesso, recusa, desistência, motivo ou gargalos.

Para responder, precisamos do funil de propostas e atendimento do agente financeiro, além de pesquisa com usuários. O cruzamento protegido mede o vínculo dos contratados — não as barreiras de quem ficou de fora.
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
        ("Visão geral", [
            [{"markdown": "## Reforma Casa Brasil — visão geral\n\nCarteira administrativa disponível no recorte selecionado. Valores representam contratação/informação da fonte, e não medição ou gasto comprovado de obra."}],
            ["kpi_contratos", "kpi_financiado", "kpi_ticket", "kpi_municipios"],
            ["evolucao_geral"],
            [{"markdown": "### Como ler\n\nA série acompanha a posição da carteira em cada data de referência. Não some os meses: cada ponto é um *snapshot* administrativo da mesma carteira."}, "municipios_top"],
            ["ranking_uf"],
        ]),
        ("Análises", [
            [{"markdown": "## Evolução mensal\n\nContratos e valor financiado por data de referência, com composição por faixa de renda."}],
            ["contratos_mes_faixa"],
            ["valor_mes_faixa"],
            [{"markdown": "## Perfil financeiro\n\nAs faixas são as informadas nos registros contratuais. Elas não substituem a renda observada depois da reforma."}],
            ["ticket_faixa", "financiamento_faixa"],
            [{"markdown": "## Prazo e atraso administrativo\n\nAtraso é o campo administrativo disponível na fonte. Ele não comprova prazo ou qualidade da obra."}],
            ["prazo_uf", "atraso_uf"],
            ["atraso_serie"],
        ]),
        ("Cruzamentos", [
            [{"markdown": "## Cruzamentos disponíveis\n\nO painel cruza os atributos contratuais por município, UF e faixa de renda. Cruzamentos territoriais com FCU, setor censitário, Censo, FJP e coordenadas dependem de fontes geográficas ainda não materializadas na camada Gold."}],
            ["municipios_faixa"],
            [{"markdown": "### Como ler\n\nA tabela mostra a distribuição contratual disponível. Não mede demanda territorial, vulnerabilidade do setor censitário ou localização intramunicipal."}, "ticket_faixa"],
            ["atraso_uf"],
        ]),
        ("Perguntas SNH", [
            [{"markdown": "## 1. Caracterização — qual é o perfil dos beneficiários e quais tipos de reforma foram realizados?"}],
            ["kpi_vinculo_cadunico", "kpi_familias_cadunico", "kpi_inadequacao"],
            [{"markdown": COMO_LER_CARACTERIZACAO}, "pergunta_perfil_territorio"],
            [{"markdown": "### Como ler — renda e categoria da fonte\n\nA distribuição ao lado é do valor financiado por faixa declarada. O tipo de imóvel é um código técnico recebido na remessa e não informa o serviço executado (telhado, ventilação, elétrica, placa solar etc.)."}, "pergunta_perfil_faixa"],
            [{"markdown": "### Como ler — trabalho, ocupação e serviços\n\nO CadÚnico permite caracterizar atividade laboral e fontes de renda **da pessoa vinculada**. Os códigos exigem o dicionário oficial do CadÚnico. Isso não informa os serviços executados na obra; estes dependem de orçamento, medição ou vistoria."}, "pergunta_trabalho"],
            [{"markdown": "## 2. Acesso — as famílias conhecem, entendem e conseguem acessar o programa? Quem desiste e por quê?"}],
            [{"markdown": COMO_LER_ACESSO}, "pergunta_cadunico"],
            [{"markdown": "## 3. Implementação — o financiamento é adequado à necessidade e como está a contratação?"}],
            [{"markdown": COMO_LER_IMPLEMENTACAO}, "pergunta_implementacao"],
            [{"markdown": "## 4. Resultados — o que foi reformado e houve melhoria habitacional?"}],
            [{"markdown": COMO_LER_RESULTADO}, "pergunta_linha_base"],
            [{"markdown": "## 5. Monitoramento — como a carteira e os recursos evoluem?"}],
            [{"markdown": COMO_LER_MONITORAMENTO}, "pergunta_monitoramento"],
            [{"markdown": "## Pergunta estrela — quais características aumentam ou reduzem o acesso e a melhoria habitacional?"}],
            [{"markdown": COMO_LER_ESTRELA}, "cobertura_avaliacao"],
        ]),
        ("Mapa", [
            [{"markdown": "## Distribuição territorial\n\nO mapa representa contratos por UF. A base atual possui município, mas não contém coordenada validada, setor censitário ou ponto de rua; por isso essas camadas não são simuladas."}],
            ["mapa_brasil"],
            ["kpi_municipios", "ranking_uf"],
            ["municipios_top"],
            [{"markdown": "## Ficha da reforma\n\nSelecione UF, município e contrato nos filtros desta aba para consultar os atributos administrativos do registro, sem dados pessoais."}],
            ["ficha_financiado", "ficha_investimento", "ficha_recurso_proprio", "ficha_atraso"],
            ["ficha_reforma"],
        ]),
        ("Qualidade dos Dados", [
            [{"markdown": "## Qualidade, cobertura e limites de interpretação\n\nEsta aba torna visível a fronteira entre dado observado, proxy administrativo e lacuna de fonte. Ausência de dado não é tratada como zero."}],
            ["qualidade_fontes"],
            [{"markdown": GUIA_PERGUNTAS}, "cobertura"],
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
    mapa = ["ouro_reforma_casa_brasil_mapa_uf"]
    ficha = ["ouro_reforma_casa_brasil_ficha_reforma_dash"]
    acesso = ["ouro_reforma_casa_brasil_acesso_dash"]
    resultado = ["ouro_reforma_casa_brasil_resultado_dash"]
    trabalho = ["ouro_reforma_casa_brasil_trabalho_dash"]
    filtros = [
        # Filtros ficam no topo de cada aba, no mesmo padrão do painel de
        # referência. Nenhum é obrigatório: abertura sem seleção é Brasil.
        seletor(1, "UF", "uf", [*implementacao, *monitoramento, *mapa], 0),
        seletor(2, "Município", "municipio", [*implementacao, *monitoramento], 0),
        seletor(3, "Extrato de renda", "faixa_renda", [*implementacao, *monitoramento], 0),
        periodo(4, 0),
        seletor(5, "UF", "uf", [*implementacao, *monitoramento], 1),
        seletor(6, "Município", "municipio", [*implementacao, *monitoramento], 1),
        seletor(7, "Extrato de renda", "faixa_renda", [*implementacao, *monitoramento], 1),
        periodo(8, 1),
        seletor(9, "UF", "uf", implementacao, 2),
        seletor(10, "Município", "municipio", implementacao, 2),
        seletor(11, "Extrato de renda", "faixa_renda", implementacao, 2),
        seletor(12, "UF", "uf", implementacao, 3),
        seletor(13, "Município", "municipio", implementacao, 3),
        seletor(14, "Extrato de renda", "faixa_renda", implementacao, 3),
        seletor(15, "UF", "uf", [*mapa, *implementacao, *ficha], 4),
        seletor(16, "Município", "municipio", [*implementacao, *ficha], 4),
        seletor(17, "Reforma / contrato", "identificador_reforma", ficha, 4),
        seletor(18, "UF", "uf", [*implementacao, *acesso, *resultado, *trabalho], 3),
        seletor(19, "Município", "municipio", [*implementacao, *acesso, *resultado, *trabalho], 3),
        seletor(20, "Faixa de renda", "faixa_renda", [*implementacao, *acesso, *resultado, *trabalho], 3),
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
