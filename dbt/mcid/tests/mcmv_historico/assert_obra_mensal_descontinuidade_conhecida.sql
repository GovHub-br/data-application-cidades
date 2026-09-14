{{ config(severity="warn") }}

-- DESCONTINUIDADE CONHECIDA (change consolidar-schemas-historico-reloginho,
-- D2/C2): nos meses 2026-04..07 só o braço obra_mensal produz linha, então a
-- contagem de APF distintos por mês DESPENCA (de ~16.240 em 2026-03 para
-- ~2.700), e estoque/financeiro ficam NULL nessas linhas.
--
-- Este teste torna a descontinuidade VISÍVEL como `warn` — NÃO é regressão: é
-- a série honesta (o SFTP congelou em 2024-11 e o SNH vai até 2026-03). Quem
-- agrega estoque por mês deve filtrar `fonte_serie <> 'obra_mensal'` ou
-- `dt_referencia <= '2026-03-01'`. O teste falha (warn) enquanto a queda
-- existir — o que é o esperado até as fontes de estoque avançarem.
--
-- Retorna 1 linha por mês em que a contagem de APF caiu mais de 50% frente ao
-- mês anterior.

with
    consolidado as (
        select frente_mcmv, apf, dt_referencia
        from {{ ref('prata_far_historico_empreendimento') }}
        union all
        select frente_mcmv, apf, dt_referencia
        from {{ ref('prata_fds_historico_empreendimento') }}
        union all
        select frente_mcmv, apf, dt_referencia
        from {{ ref('prata_rural_historico_empreendimento') }}
    ),

    por_mes as (
        select
            date_trunc('month', dt_referencia)::date as mes,
            count(distinct apf) as n_apf
        from consolidado
        group by 1
    ),

    variacao as (
        select
            mes,
            n_apf,
            lag(n_apf) over (order by mes) as n_apf_anterior
        from por_mes
    )

select
    mes,
    n_apf,
    n_apf_anterior,
    round(100.0 * n_apf / nullif(n_apf_anterior, 0), 1) as pct_do_mes_anterior
from variacao
where
    n_apf_anterior is not null
    and n_apf < 0.5 * n_apf_anterior
