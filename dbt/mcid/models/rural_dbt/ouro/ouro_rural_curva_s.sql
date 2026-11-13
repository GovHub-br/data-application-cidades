{{ config(materialized="table") }}

-- Gold: Curva S físico-financeira do Novo MCMV Rural, por mês, já agregada por recorte de portaria.
-- Grão: mês × tipo_portaria × portaria × UF × agente financeiro × programa. Lê só prata.
--
-- Carteira: contratos do Novo MCMV Rural (prata_rural_vinculo_operacao, programa = NOVO MCMV RURAL) já
-- assinados até o mês. O PNHR (histórico) fica fora: não tem medição física mensal.
--
-- Séries (uma linha por APF × mês, depois somadas):
--   físico realizado e previsto — prata_rural_obra_mensal_serie (MONIT_MOV_OBRA da CAIXA), retratos mensais
--     de dez/2025 em diante. Mês sem retrato repete o último retrato; antes do primeiro retrato fica nulo
--     (não entra na média do mês);
--   financeiro liberado acumulado — prata_rural_evolucao_financeira (MONIT_MOV_FINANC, liberações a partir
--     de jul/2024, com a APF já corrigida para 8 dígitos). Contrato assinado e sem liberação conta como 0.
--
-- Agregação (nunca média de percentuais):
--   % físico realizado = Σ(% realizado × UH) ÷ Σ UH com medição;
--   % físico previsto  = Σ(% previsto × UH) ÷ Σ UH com previsão;
--   % financeiro       = Σ liberado acumulado ÷ Σ valor contratado.
-- No Superset, recalcule a partir das colunas de soma (fis_realizado_x_uh, uh_com_medicao, …); as colunas
-- pct_* desta tabela já valem para o recorte da linha, mas não podem ser somadas nem tiradas a média.
--
-- Cada APF aparece uma vez por tipo de portaria: filtre UM tipo_portaria antes de somar.

with
    op as (
        select *
        from {{ ref("prata_rural_vinculo_operacao") }}
        where programa = 'NOVO MCMV RURAL'
          and dt_contratacao is not null
    ),

    fis as (
        select apf, date_trunc('month', dt_referencia)::date as mes,
               max(percentual_obra_realizada) as pct_realizado,
               max(percentual_obra_prevista) as pct_previsto
        from {{ ref("prata_rural_obra_mensal_serie") }}
        group by apf, date_trunc('month', dt_referencia)::date
    ),

    fin as (
        select apf, date_trunc('month', mes)::date as mes, max(vr_acumulado) as vr_acumulado
        from {{ ref("prata_rural_evolucao_financeira") }}
        group by apf, date_trunc('month', mes)::date
    ),

    limites as (
        select
            (select date_trunc('month', min(dt_contratacao))::date from op) as mes_inicial,
            greatest((select max(mes) from fis), (select max(mes) from fin)) as mes_final
    ),

    meses as (
        select generate_series(l.mes_inicial, l.mes_final, interval '1 month')::date as mes
        from limites l
    ),

    apf_mes as (
        select o.apf, m.mes, fi.pct_realizado, fi.pct_previsto, fn.vr_acumulado
        from op o
        join meses m on m.mes >= date_trunc('month', o.dt_contratacao)::date
        left join fis fi on fi.apf = o.apf and fi.mes = m.mes
        left join fin fn on fn.apf = o.apf and fn.mes = m.mes
    ),

    grupos as (
        select
            a.*,
            count(a.pct_realizado) over w as g_realizado,
            count(a.pct_previsto) over w as g_previsto,
            count(a.vr_acumulado) over w as g_financeiro
        from apf_mes a
        window w as (partition by a.apf order by a.mes)
    ),

    carregado as (
        select
            g.apf,
            g.mes,
            case when g.g_realizado > 0
                then max(g.pct_realizado) over (partition by g.apf, g.g_realizado) end as pct_realizado,
            case when g.g_previsto > 0
                then max(g.pct_previsto) over (partition by g.apf, g.g_previsto) end as pct_previsto,
            coalesce(
                case when g.g_financeiro > 0
                    then max(g.vr_acumulado) over (partition by g.apf, g.g_financeiro) end,
                0
            ) as vr_liberado_acumulado,
            (g.pct_realizado is not null) as ic_retrato_fisico_no_mes
        from grupos g
    ),

    linhas as (
        select
            c.mes,
            tp.tipo_portaria,
            tp.portaria,
            o.uf,
            o.agente_financeiro,
            o.programa,
            o.apf,
            o.quantidade_uh,
            o.valor_contratado,
            c.pct_realizado,
            c.pct_previsto,
            c.vr_liberado_acumulado,
            c.ic_retrato_fisico_no_mes
        from carregado c
        join op o on o.apf = c.apf
        {{ rural_portaria_por_tipo("o") }}
    )

select
    md5(concat_ws('|', mes::text, tipo_portaria, portaria, uf, agente_financeiro, programa)) as id_curva_s,
    mes,
    (mes = max(mes) over ()) as ic_ultimo_mes,
    tipo_portaria,
    portaria,
    uf,
    agente_financeiro,
    programa,
    count(*) as qt_contratos,
    sum(quantidade_uh) as uh_contratadas,
    sum(valor_contratado) as valor_contratado,
    sum(vr_liberado_acumulado) as vr_liberado_acumulado,
    count(*) filter (where pct_realizado is not null) as qt_contratos_com_medicao,
    count(*) filter (where ic_retrato_fisico_no_mes) as qt_contratos_com_retrato_no_mes,
    sum(quantidade_uh) filter (where pct_realizado is not null) as uh_com_medicao,
    sum(pct_realizado * quantidade_uh) filter (where pct_realizado is not null) as fis_realizado_x_uh,
    sum(quantidade_uh) filter (where pct_previsto is not null) as uh_com_previsao,
    sum(pct_previsto * quantidade_uh) filter (where pct_previsto is not null) as fis_previsto_x_uh,
    round(
        sum(pct_realizado * quantidade_uh) filter (where pct_realizado is not null)
        / nullif(sum(quantidade_uh) filter (where pct_realizado is not null), 0), 2
    ) as pct_fisico_realizado,
    round(
        sum(pct_previsto * quantidade_uh) filter (where pct_previsto is not null)
        / nullif(sum(quantidade_uh) filter (where pct_previsto is not null), 0), 2
    ) as pct_fisico_previsto,
    round(100.0 * sum(vr_liberado_acumulado) / nullif(sum(valor_contratado), 0), 2) as pct_financeiro
from linhas
group by mes, tipo_portaria, portaria, uf, agente_financeiro, programa
