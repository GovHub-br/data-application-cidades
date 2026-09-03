{{ config(materialized="table", alias="gold_evolucao_financeira_chart") }}

-- Gold: Evolução Financeira (chart) — série APF × mês pronta para o gráfico
-- Enriquece a silver com nome do empreendimento, estado por extenso e labels de mês.
with
    evolucao as (select * from {{ ref("far_silver_evolucao_financeira") }}),

    -- distinct: a api_ibge_uf tem cada UF duplicada (54 linhas p/ 27 siglas) e o join
    -- dobraria as linhas da série
    ibge_uf as (
        select distinct sigla, upper(nome) as estado
        from {{ source("raw", "api_ibge_uf") }}
    ),

    ficha as (
        select apf, nome_empreendimento, apf_municipio_empreendimento
        from {{ ref("far_gold_ficha_empreendimento") }}
    )

select
    -- Identificação
    e.apf,
    f.nome_empreendimento,
    f.apf_municipio_empreendimento,

    -- Localização
    e.municipio,
    e.uf,
    i.estado,

    -- Série temporal (eixo X do gráfico)
    e.mes,
    to_char(e.mes, 'YYYY-MM') as mes_label,
    to_char(e.mes, 'MM/YYYY') as mes_label_br,

    -- Valores mensais
    e.qt_liberacoes,
    round(e.vr_liberado_mes, 2) as vr_liberado_mes,
    round(e.vr_pago_obra_mes, 2) as vr_pago_obra_mes,
    round(e.vr_pago_terreno_mes, 2) as vr_pago_terreno_mes,
    round(e.vr_pago_pts_mes, 2) as vr_pago_pts_mes,
    round(e.vr_pago_incc_mes, 2) as vr_pago_incc_mes,

    -- Acumulados e percentual
    round(e.vr_acumulado, 2) as vr_acumulado,
    round(e.valor_contratado, 2) as valor_contratado,
    e.pct_executado_financeiro

from evolucao e
left join ibge_uf i on e.uf = i.sigla
left join ficha f on e.apf = f.apf
