{{ config(materialized="table") }}

-- Prata: Contratos do Pró-Moradia com obra paralisada
-- Fonte: bronze_shpt_fgts_canal_paralisadas_setor_publico, restrita aos contratos de
-- prata_pro_moradia_contrato. É a lista que a CAIXA acompanha com o tomador — 13 contratos do
-- Pró-Moradia em 2026-09 —, com o motivo e o plano de retomada. Uma linha por contrato.

with
    contrato as (
        select cod_contrato, cod_contrato_dv from {{ ref("prata_pro_moradia_contrato") }}
    )

select
    trim(p.cod_contrato::text) as cod_contrato,
    trim(p.cod_contrato_dv::text) as cod_contrato_dv,
    {{ parse_int("p.dias_sem_evolucao::text") }} as dias_sem_evolucao,
    nullif(trim({{ target.schema }}.corrigir_mojibake(p.faixa_paralisacao::text)), '') as faixa_paralisacao,
    nullif(trim({{ target.schema }}.corrigir_mojibake(p.ultimo_motivo_paralisacao::text)), '') as motivo_paralisacao,
    nullif(trim({{ target.schema }}.corrigir_mojibake(p.ultimo_entrave_preenchido::text)), '') as entrave,
    nullif(trim({{ target.schema }}.corrigir_mojibake(p.situacao_atual::text)), '') as situacao_atual,
    upper(trim({{ target.schema }}.corrigir_mojibake(p.plano_acao_aprovado_af::text))) in ('SIM', 'S') as ic_plano_acao_aprovado,
    case
        when trim(p.dt_ultimo_bm::text) ~ '^\d{4}-\d{2}-\d{2}' then left(trim(p.dt_ultimo_bm::text), 10)::date
    end as dt_ultimo_boletim_medicao,
    {{ target.schema }}.parse_date_br(nullif(trim(p.dt_previsao_conclusao_objeto::text), '')) as dt_previsao_conclusao
from {{ ref("bronze_shpt_fgts_canal_paralisadas_setor_publico") }} p
join contrato c on c.cod_contrato = trim(p.cod_contrato::text)
