{#-
  rural_curva_s_apf_mes(): uma linha por APF × retrato mensal do Novo MCMV Rural, com o tempo de contrato
  (meses entre o mês da contratação e o mês do retrato), o % físico realizado e previsto e o % financeiro
  liberado acumulado até aquele mês. Lê só prata (prata_rural_obra_mensal_serie, prata_rural_evolucao_financeira,
  prata_rural_vinculo_operacao). Base da curva de referência e do desvio de cada empreendimento.

  rural_curva_s_referencia(): a curva "padrão" — por tempo de contrato, o p10, p25, a mediana, o p75 e o p90
  do % físico realizado e do % financeiro de todos os empreendimentos que estavam naquela idade em algum
  retrato. Idade com menos de `min_obs` retratos não tem referência (amostra pequena demais).
-#}
{% macro rural_curva_s_apf_mes() %}
    select
        o.apf,
        date_trunc('month', o.dt_referencia)::date as mes,
        (
            (extract(year from o.dt_referencia)::int - extract(year from v.dt_contratacao)::int) * 12
            + extract(month from o.dt_referencia)::int - extract(month from v.dt_contratacao)::int
        ) as idade_meses,
        max(o.percentual_obra_realizada) as pct_fisico_realizado,
        max(o.percentual_obra_prevista) as pct_fisico_previsto,
        max(o.situacao_operacao) as situacao_operacao,
        max(o.estagio_obra) as estagio_obra,
        -- liberado acumulado até o mês do retrato (0 se ainda não houve liberação) ÷ contratado
        case
            when max(v.valor_contratado) > 0
                then round(100.0 * coalesce((array_agg(f.vr_acumulado order by f.mes desc) filter (where f.apf is not null))[1], 0) / max(v.valor_contratado), 2)
        end as pct_financeiro
    from {{ ref("prata_rural_obra_mensal_serie") }} o
    join {{ ref("prata_rural_vinculo_operacao") }} v on v.apf = o.apf
    left join {{ ref("prata_rural_evolucao_financeira") }} f
        on f.apf = o.apf
        and date_trunc('month', f.mes)::date <= date_trunc('month', o.dt_referencia)::date
    where v.programa = 'NOVO MCMV RURAL'
      and v.dt_contratacao is not null
      and o.percentual_obra_realizada is not null
    group by o.apf, date_trunc('month', o.dt_referencia)::date, v.dt_contratacao, o.dt_referencia
{% endmacro %}

{% macro rural_curva_s_referencia(min_obs=10) %}
    select
        idade_meses,
        count(*) as qt_retratos,
        count(distinct apf) as qt_empreendimentos,
        round(percentile_cont(0.10) within group (order by pct_fisico_realizado)::numeric, 1) as fis_p10,
        round(percentile_cont(0.25) within group (order by pct_fisico_realizado)::numeric, 1) as fis_p25,
        round(percentile_cont(0.50) within group (order by pct_fisico_realizado)::numeric, 1) as fis_mediana,
        round(percentile_cont(0.75) within group (order by pct_fisico_realizado)::numeric, 1) as fis_p75,
        round(percentile_cont(0.90) within group (order by pct_fisico_realizado)::numeric, 1) as fis_p90,
        round(percentile_cont(0.50) within group (order by pct_fisico_previsto)::numeric, 1) as fis_previsto_mediana,
        round(percentile_cont(0.25) within group (order by pct_financeiro)::numeric, 1) as fin_p25,
        round(percentile_cont(0.50) within group (order by pct_financeiro)::numeric, 1) as fin_mediana,
        round(percentile_cont(0.75) within group (order by pct_financeiro)::numeric, 1) as fin_p75,
        (count(*) >= {{ min_obs }}) as ic_referencia_valida
    from ({{ rural_curva_s_apf_mes() }}) b
    where idade_meses >= 0
    group by idade_meses
{% endmacro %}
