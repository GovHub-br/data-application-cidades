-- Teste singular: reconciliação do reloginho com a referência #66 (Fase 4 da #130).
-- Para dt_referencia = 2026-03 e agente CAIXA, o gold deve produzir
-- uh_contratadas ≈ 1.697.630 e uh_entregues ≈ 1.391.909, dentro de ±0,5%.
-- O teste falha se a linha de referência não existir ou se algum valor
-- divergir além da tolerância (retorna linhas apenas nessas condições).

with

atual as (
    select
        uh_contratadas,
        uh_entregues
    from {{ ref("indicadores_reloginho") }}
    where agente_financeiro = 'CAIXA'
      and dt_referencia = date '2026-03-01'
),

referencia as (
    select
        1697630::bigint as uh_contratadas_ref,
        1391909::bigint as uh_entregues_ref
),

comparacao as (
    select
        (select count(*) from atual) as n_linhas,
        coalesce((select uh_contratadas from atual), 0) as uh_contratadas,
        coalesce((select uh_entregues from atual), 0) as uh_entregues,
        r.uh_contratadas_ref,
        r.uh_entregues_ref,
        r.uh_contratadas_ref * 0.005 as tol_contratadas,
        r.uh_entregues_ref * 0.005 as tol_entregues
    from referencia r
)

select
    n_linhas,
    uh_contratadas,
    uh_entregues,
    uh_contratadas_ref,
    uh_entregues_ref,
    abs(uh_contratadas - uh_contratadas_ref) as diff_contratadas,
    abs(uh_entregues - uh_entregues_ref) as diff_entregues
from comparacao
where n_linhas = 0
   or abs(uh_contratadas - uh_contratadas_ref) > tol_contratadas
   or abs(uh_entregues - uh_entregues_ref) > tol_entregues
