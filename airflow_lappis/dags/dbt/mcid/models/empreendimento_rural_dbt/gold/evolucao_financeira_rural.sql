{{ config(materialized="table") }}

-- Silver: Evolução Financeira Rural — Série temporal de desembolsos consolidados por APF × mês.
-- Une o financeiro do Novo MCMV Rural com as liberações históricas do PNHR.

with
    financeiro_novo as (
        select
            apf,
            date_trunc('month', dt_liberacao_recurso) as mes,
            vr_movimento as vr_liberado,
            vr_desembolso_obra as vr_pago_obra,
            vr_desembolso_trabalho_social as vr_pago_ts,
            vr_desembolso_atec as vr_pago_atec,
            vr_desembolso_cisternas_efluentes as vr_pago_cisternas_efluentes,
            vr_desembolso_custos_indiretos as vr_pago_custos_indiretos
        from {{ ref("financeiro_mensal_rural") }}
        where dt_liberacao_recurso is not null
    ),

    financeiro_pnhr as (
        select
            {{ target.schema }}.normalize_apf(nu_apf) as apf,
            date_trunc('month', (
                case
                    when data_liberacao is null or trim(data_liberacao) = '' then null
                    when data_liberacao ~ '^\d{4}-\d{2}-\d{2}' then data_liberacao::date
                    else {{ target.schema }}.parse_date_br(data_liberacao)
                end
            )) as mes,
            {{ parse_financial_value('vr_valor') }} as vr_liberado,
            0.00 as vr_pago_obra,
            0.00 as vr_pago_ts,
            0.00 as vr_pago_atec,
            0.00 as vr_pago_cisternas_efluentes,
            0.00 as vr_pago_custos_indiretos
        from {{ source("raw", "int_financeiro_int055_liberacoes_caixa_bb") }}
        where nu_apf is not null
          and trim(data_liberacao) != ''
          and trim(upper(no_programa)) like '%PNHR%'
    ),

    union_financeiro as (
        select * from financeiro_novo
        union all
        select * from financeiro_pnhr
    ),

    mensal_agrupado as (
        select
            apf,
            mes,
            count(*) as qt_liberacoes,
            sum(vr_liberado) as vr_liberado_mes,
            sum(vr_pago_obra) as vr_pago_obra_mes,
            sum(vr_pago_ts) as vr_pago_ts_mes,
            sum(vr_pago_atec) as vr_pago_atec_mes,
            sum(vr_pago_cisternas_efluentes) as vr_pago_cisternas_efluentes_mes,
            sum(vr_pago_custos_indiretos) as vr_pago_custos_indiretos_mes
        from union_financeiro
        group by apf, mes
    ),

    empreendimento as (
        select * from {{ ref("empreendimento_rural") }}
    ),

    evolucao as (
        select
            e.apf,
            m.mes,
            m.qt_liberacoes,

            -- Valores mensais
            coalesce(m.vr_liberado_mes, 0.00) as vr_liberado_mes,
            coalesce(m.vr_pago_obra_mes, 0.00) as vr_pago_obra_mes,
            coalesce(m.vr_pago_ts_mes, 0.00) as vr_pago_ts_mes,
            coalesce(m.vr_pago_atec_mes, 0.00) as vr_pago_atec_mes,
            coalesce(m.vr_pago_cisternas_efluentes_mes, 0.00) as vr_pago_cisternas_efluentes_mes,
            coalesce(m.vr_pago_custos_indiretos_mes, 0.00) as vr_pago_custos_indiretos_mes,

            -- Acumulado progressivo por APF
            sum(m.vr_liberado_mes) over (
                partition by e.apf order by m.mes
            ) as vr_acumulado,

            -- Metadados do contrato
            e.valor_contratado,
            e.municipio,
            e.uf
            
        from mensal_agrupado m
        inner join empreendimento e on m.apf = e.apf
    )

select
    apf,
    mes,
    qt_liberacoes,
    
    -- Valores do mês
    vr_liberado_mes,
    vr_pago_obra_mes,
    vr_pago_ts_mes,
    vr_pago_atec_mes,
    vr_pago_cisternas_efluentes_mes,
    vr_pago_custos_indiretos_mes,
    
    -- Acumulado e percentual
    vr_acumulado,
    case
        when coalesce(valor_contratado, 0.00) > 0
        then round((vr_acumulado / valor_contratado) * 100, 2)
        else 0.00
    end as pct_executado_financeiro,

    valor_contratado,
    municipio,
    uf

from evolucao
