{#-
  Curva S do Pró-Moradia e do FNHIS Sub-50 — mesmo desenho da do Rural (macros/rural_curva_s_referencia.sql):
    *_curva_s_obra_mes()   uma linha por contrato/termo × mês com tempo de contrato (idade_meses) e percentuais;
    curva_s_referencia_de(sql_base)  distribuição por idade (p10, p25, mediana, p75, p90) — a curva "padrão".
  Todas leem só prata (são usadas pelas ouros *_curva_s, *_curva_s_referencia e *_curva_s_desvio).
-#}

{% macro pro_moradia_curva_s_obra_mes() %}
    -- Pró-Moradia: medições mensais de obra do Canal FGTS (sem as avaliações futuras) + desembolsos acumulados.
    -- Peso = valor contratado (201 dos 686 contratos não financiam UH: urbanização, lotes, encostas…).
    -- % financeiro só para contratos que têm algum desembolso no canal (316 de 686); nos demais fica nulo, porque
    -- ausência de linha no canal não prova que nada foi pago.
    with
        c as (
            select cod_contrato, dt_assinatura, valor_contratado
            from {{ ref("prata_pro_moradia_contrato") }}
            where dt_assinatura is not null
        ),
        d as (
            select cod_contrato, date_trunc('month', dt_competencia)::date as mes,
                   sum(coalesce(vr_liberado, 0) - coalesce(vr_estornado, 0)) as vr_mes
            from {{ ref("prata_pro_moradia_desembolso_mensal") }}
            group by 1, 2
        ),
        dac as (
            select cod_contrato, mes, sum(vr_mes) over (partition by cod_contrato order by mes) as vr_acumulado
            from d
        )
    select
        o.cod_contrato as chave,
        date_trunc('month', o.dt_avaliacao)::date as mes,
        (
            (extract(year from o.dt_avaliacao)::int - extract(year from c.dt_assinatura)::int) * 12
            + extract(month from o.dt_avaliacao)::int - extract(month from c.dt_assinatura)::int
        ) as idade_meses,
        max(o.percentual_obra_realizado) as pct_fisico_realizado,
        max(o.percentual_obra_previsto) as pct_fisico_previsto,
        max(o.situacao_obra) as situacao,
        case
            when max(c.valor_contratado) > 0 and exists (select 1 from d where d.cod_contrato = o.cod_contrato)
                then round(100.0 * coalesce((array_agg(x.vr_acumulado order by x.mes desc)
                                             filter (where x.cod_contrato is not null))[1], 0)
                           / max(c.valor_contratado), 2)
        end as pct_financeiro,
        max(c.valor_contratado) as peso
    from {{ ref("prata_pro_moradia_execucao_obra") }} o
    join c on c.cod_contrato = o.cod_contrato
    left join dac x on x.cod_contrato = o.cod_contrato and x.mes <= date_trunc('month', o.dt_avaliacao)::date
    where not coalesce(o.ic_avaliacao_futura, false)
      and o.percentual_obra_realizado is not null
    group by o.cod_contrato, date_trunc('month', o.dt_avaliacao)::date, o.dt_avaliacao, c.dt_assinatura
{% endmacro %}

{% macro fnhis_curva_s_termo_mes() %}
    -- FNHIS Sub-50: retratos mensais do TransfereGov. Não há medição física (na SNH as operações FNHIS estão com
    -- 0%), então a "curva" é a do valor EMPENHADO acumulado ÷ repasse previsto. Empenho é recurso comprometido,
    -- não pago. Termos anulados ou rescindidos ficam fora.
    select
        m.numero_proposta_transferegov as chave,
        date_trunc('month', m.dt_retrato)::date as mes,
        (
            (extract(year from m.dt_retrato)::int - extract(year from t.dt_assinatura)::int) * 12
            + extract(month from m.dt_retrato)::int - extract(month from t.dt_assinatura)::int
        ) as idade_meses,
        null::numeric as pct_fisico_realizado,
        null::numeric as pct_fisico_previsto,
        m.situacao_instrumento as situacao,
        case when m.valor_repasse > 0
            then round(100.0 * coalesce(m.valor_empenhado_acumulado, 0) / m.valor_repasse, 2) end as pct_financeiro,
        m.valor_repasse as peso
    from {{ ref("prata_fnhis_termo_compromisso_mensal") }} m
    join {{ ref("prata_fnhis_termo_compromisso") }} t
        on t.numero_proposta_transferegov = m.numero_proposta_transferegov
    where t.dt_assinatura is not null
      and coalesce(m.situacao_instrumento, '') !~* 'anulad|rescindid'
{% endmacro %}

{% macro curva_s_referencia_de(sql_base, min_obs=10) %}
    select
        idade_meses,
        count(*) as qt_retratos,
        count(distinct chave) as qt_contratos,
        round(percentile_cont(0.10) within group (order by pct_fisico_realizado)::numeric, 1) as fis_p10,
        round(percentile_cont(0.25) within group (order by pct_fisico_realizado)::numeric, 1) as fis_p25,
        round(percentile_cont(0.50) within group (order by pct_fisico_realizado)::numeric, 1) as fis_mediana,
        round(percentile_cont(0.75) within group (order by pct_fisico_realizado)::numeric, 1) as fis_p75,
        round(percentile_cont(0.90) within group (order by pct_fisico_realizado)::numeric, 1) as fis_p90,
        round(percentile_cont(0.50) within group (order by pct_fisico_previsto)::numeric, 1) as fis_previsto_mediana,
        round(percentile_cont(0.10) within group (order by pct_financeiro)::numeric, 1) as fin_p10,
        round(percentile_cont(0.25) within group (order by pct_financeiro)::numeric, 1) as fin_p25,
        round(percentile_cont(0.50) within group (order by pct_financeiro)::numeric, 1) as fin_mediana,
        round(percentile_cont(0.75) within group (order by pct_financeiro)::numeric, 1) as fin_p75,
        round(percentile_cont(0.90) within group (order by pct_financeiro)::numeric, 1) as fin_p90,
        (count(*) >= {{ min_obs }}) as ic_referencia_valida
    from ({{ sql_base }}) b
    where idade_meses >= 0
    group by idade_meses
{% endmacro %}
