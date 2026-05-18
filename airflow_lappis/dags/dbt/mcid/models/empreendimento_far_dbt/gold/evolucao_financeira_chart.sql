{{ config(materialized="table") }}

-- Gold: Evolução Financeira (Chart) — Visualização para o Superset
-- Consome a silver evolucao_financeira e prepara os dados no formato ideal
-- para o gráfico de "Evolução Físico-Financeira" do dashboard.
-- Adiciona: label do mês, nome do estado, campos de busca/filtro.

with
    evolucao as (
        select * from {{ ref("evolucao_financeira") }}
    ),

    -- Referência IBGE para nome completo do estado
    ibge_uf as (
        select sigla, upper(nome) as estado
        from {{ source("raw", "api_ibge_uf") }}
    ),

    -- Trazer nome do empreendimento da ficha
    ficha as (
        select apf, nome_empreendimento, apf_municipio_empreendimento
        from {{ ref("ficha_empreendimento") }}
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

    -- Série temporal (para eixo X do gráfico)
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
