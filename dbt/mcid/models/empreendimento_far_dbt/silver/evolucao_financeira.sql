{{ config(materialized="table") }}

-- Silver: Evolução Financeira — Série temporal de desembolsos por empreendimento
-- Agrega as liberações da bronze financeiro_mensal por APF e mês.
-- JOIN com empreendimento usando raiz de 6 dígitos (LEFT/RIGHT) para calcular
-- o percentual de execução financeira sobre o valor contratado.
-- Grão: 1 linha por APF × mês (relação 1:N com empreendimento)

with
    financeiro as (
        select * from {{ ref("financeiro_mensal") }}
    ),

    empreendimento as (
        select * from {{ ref("empreendimento") }}
    ),

    -- Agregação mensal: soma todas as liberações do mês por APF
    mensal as (
        select
            -- Chave raiz de 6 dígitos para JOIN posterior
            right(f.apf, 6)                     as apf_raiz,

            -- Mês da liberação (truncado ao primeiro dia)
            date_trunc('month', f.dt_liberacao)  as mes,

            -- Totais do mês
            count(*)                             as qt_liberacoes,
            sum(f.vr_liberado)                   as vr_liberado_mes,

            -- Decomposição por componente
            sum(f.vr_pago_obra)                  as vr_pago_obra_mes,
            sum(f.vr_pago_terreno)               as vr_pago_terreno_mes,
            sum(f.vr_pago_trabalho_social)        as vr_pago_pts_mes,
            sum(f.vr_pago_incc)                  as vr_pago_incc_mes,
            sum(f.vr_pago_aporte)                as vr_pago_aporte_mes,
            sum(f.vr_pago_equipamentos)          as vr_pago_equipamentos_mes,
            sum(f.vr_pago_manutencao)            as vr_pago_manutencao_mes,
            sum(f.vr_pago_legalizacao)           as vr_pago_legalizacao_mes

        from financeiro f
        where f.dt_liberacao is not null
        group by right(f.apf, 6), date_trunc('month', f.dt_liberacao)
    ),

    -- JOIN com empreendimento via raiz 6 dígitos
    -- e cálculo do acumulado com window function
    evolucao as (
        select
            e.apf,
            m.mes,
            m.qt_liberacoes,

            -- Valores mensais
            coalesce(m.vr_liberado_mes, 0.0)          as vr_liberado_mes,
            coalesce(m.vr_pago_obra_mes, 0.0)         as vr_pago_obra_mes,
            coalesce(m.vr_pago_terreno_mes, 0.0)      as vr_pago_terreno_mes,
            coalesce(m.vr_pago_pts_mes, 0.0)           as vr_pago_pts_mes,
            coalesce(m.vr_pago_incc_mes, 0.0)         as vr_pago_incc_mes,
            coalesce(m.vr_pago_aporte_mes, 0.0)       as vr_pago_aporte_mes,
            coalesce(m.vr_pago_equipamentos_mes, 0.0) as vr_pago_equipamentos_mes,
            coalesce(m.vr_pago_manutencao_mes, 0.0)   as vr_pago_manutencao_mes,
            coalesce(m.vr_pago_legalizacao_mes, 0.0)  as vr_pago_legalizacao_mes,

            -- Acumulado progressivo por APF
            sum(m.vr_liberado_mes) over (
                partition by e.apf order by m.mes
            ) as vr_acumulado,

            -- Contexto do empreendimento (para cálculo de %)
            e.valor_contratado,
            e.municipio,
            e.uf

        from mensal m
        inner join empreendimento e
            on m.apf_raiz = left(e.apf, 6)
    )

select
    apf,
    mes,
    qt_liberacoes,

    -- Valores mensais
    vr_liberado_mes,
    vr_pago_obra_mes,
    vr_pago_terreno_mes,
    vr_pago_pts_mes,
    vr_pago_incc_mes,
    vr_pago_aporte_mes,
    vr_pago_equipamentos_mes,
    vr_pago_manutencao_mes,
    vr_pago_legalizacao_mes,

    -- Acumulado e percentual
    vr_acumulado,
    case
        when coalesce(valor_contratado, 0.0) > 0
        then round((vr_acumulado / valor_contratado) * 100, 2)
        else 0.0
    end as pct_executado_financeiro,

    -- Contexto
    valor_contratado,
    municipio,
    uf

from evolucao
