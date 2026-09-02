{{ config(materialized="table") }}

-- Gold: Evolução Financeira Rural — Série temporal de desembolsos consolidados por APF × mês.
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
        from {{ ref("silver_financeiro_mensal") }}
        where dt_liberacao_recurso is not null
    ),

    -- Liberações históricas do PNHR: já tipadas e filtradas na silver liberacoes_pnhr.
    -- O INT055 não decompõe o valor por componente, então só alimenta o total liberado.
    financeiro_pnhr as (
        select
            apf,
            date_trunc('month', dt_liberacao) as mes,
            vr_liberado,
            -- NULL, e não 0.00: o INT055 não decompõe o valor por componente. Zero
            -- afirmaria "não houve desembolso de obra"; o arquivo simplesmente não informa.
            null::numeric as vr_pago_obra,
            null::numeric as vr_pago_ts,
            null::numeric as vr_pago_atec,
            null::numeric as vr_pago_cisternas_efluentes,
            null::numeric as vr_pago_custos_indiretos
        from {{ ref("silver_pnhr_liberacoes") }}
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
        select * from {{ ref("silver_empreendimento") }}
    ),

    evolucao as (
        select
            e.apf,
            m.mes,
            m.qt_liberacoes,

            -- Valores mensais
            -- Sem coalesce para 0: componente nulo significa "a fonte não decompõe"
            -- (caso do INT055), e somar zeros aí produziria um total de obra falso.
            m.vr_liberado_mes,
            m.vr_pago_obra_mes,
            m.vr_pago_ts_mes,
            m.vr_pago_atec_mes,
            m.vr_pago_cisternas_efluentes_mes,
            m.vr_pago_custos_indiretos_mes,

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
    -- Sem `else 0.00`: contrato sem valor conhecido não permite calcular percentual, e
    -- publicar 0% seria afirmar que nada foi executado.
    case
        when coalesce(valor_contratado, 0.00) > 0
        then round((vr_acumulado / valor_contratado) * 100, 2)
    end as pct_executado_financeiro,

    valor_contratado,
    municipio,
    uf

from evolucao
