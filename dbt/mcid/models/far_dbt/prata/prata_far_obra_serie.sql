{{ config(materialized="table") }}

-- Prata: Série de execução física — uma linha por APF × competência.
-- Fonte: bronze.bronze_shpt_monit_mov_obra_far_serie
-- A competência vem do nome do arquivo (`_MENSAL_<aaaamm>_`), não de `dt_movimento`,
-- que cai no mês seguinte. Reenvio da mesma competência: vale o arquivo mais recente.
with
    obra as (
        select
            {{ var('schema_udfs') }}.normalize_apf(nu_apf) as apf,
            to_date(
                substring(filename from 'MENSAL_([0-9]{6})'), 'YYYYMM'
            ) as competencia,

            -- Situação (código; o rótulo está no seed far_situacao_obra)
            {{ parse_int("co_situacao_obra") }} as co_situacao_obra,
            {{ var('schema_udfs') }}.parse_date_br(
                dt_alteracao_situacao
            ) as dt_alteracao_situacao,

            -- Percentuais de execução
            {{ parse_numeric("pc_obra_prevista", "numeric(6, 2)") }} as pct_obra_prevista,
            {{ parse_numeric("pc_obra_realizada", "numeric(6, 2)") }}
            as pct_obra_realizada,

            -- Marcos
            {{ var('schema_udfs') }}.parse_date_br(dt_conclusao_obra) as dt_conclusao_obra,
            {{ var('schema_udfs') }}.parse_date_br(
                dt_previsao_entrega_do_empreendimento
            ) as dt_previsao_entrega,
            {{ var('schema_udfs') }}.parse_date_br(dt_entrega_do_empreendimento) as dt_entrega,
            {{ var('schema_udfs') }}.parse_date_br(dt_movimento) as dt_movimento,

            -- Metadados
            substring(filename from '[^/]+$') as arquivo_de_origem,
            nullif(trim(_ingested_at), '')::timestamp as criado_em

        from {{ ref("bronze_shpt_monit_mov_obra_far_serie") }}
        where nullif(trim(nu_apf), '') is not null
    ),

    deduplicado as (
        select
            *,
            row_number() over (
                partition by apf, competencia order by arquivo_de_origem desc
            ) as rn
        from obra
    )

select
    apf,
    competencia,
    co_situacao_obra,
    dt_alteracao_situacao,
    pct_obra_prevista,
    pct_obra_realizada,
    dt_conclusao_obra,
    dt_previsao_entrega,
    dt_entrega,
    dt_movimento,
    arquivo_de_origem,
    criado_em
from deduplicado
where rn = 1
