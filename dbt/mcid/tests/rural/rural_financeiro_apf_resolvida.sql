{#-
    Schema drift do MONIT_MOV_FINANC_RURAL: a APF chega sem o dígito verificador (6 dígitos) e é resolvida na
    prata pelos 6 primeiros dígitos. Este teste avisa se a resolução cair abaixo de 95% das APFs distintas
    (em 03/11/2026: 1.557 de 1.582, 98,4%) — sinal de que o layout mudou de novo.
-#}
{{ config(severity="warn") }}

with t as (
    select
        count(distinct nu_apf_origem) as apfs,
        count(distinct nu_apf_origem) filter (
            where metodo_chave_apf in ('APF com DV na origem', 'APF sem DV resolvida pelos 6 primeiros dígitos')
        ) as resolvidas
    from {{ ref("prata_rural_financeiro_mensal") }}
)

select apfs, resolvidas, round(100.0 * resolvidas / nullif(apfs, 0), 1) as pct_resolvidas
from t
where resolvidas < 0.95 * apfs
