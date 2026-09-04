{{ config(materialized="table") }}

-- SILVER do reloginho (grupo A) — entregas por evento agregadas por mes.
--
-- Le bronze_reloginho_snh_entregas_evento, deduplica eventos repetidos (o mesmo
-- evento reaparece em snapshots mensais seguintes) por hash_linha, e soma a
-- entrega por (agente, apf, mes do EVENTO). Grao: (agente, apf, mes_evento).
--
-- mes_evento = mes de dt_entrega/dt_ass_doc (quando a UH foi entregue), NAO o
-- dt_referencia do arquivo. Assim a serie e um fluxo real de entregas.
--
-- Target obrigatorio: staging_duckdb (gating em dbt_project.yml).
with

    bronze as (select * from {{ ref("bronze_reloginho_snh_entregas_evento") }}),

    tipado as (
        select
            coalesce(
                upper(nullif(trim(cast(agente_financeiro as varchar)), '')),
                agente_arquivo
            ) as agente_financeiro,
            nullif(trim(cast(apf as varchar)), '') as apf,
            dt_evento,
            date_trunc('month', dt_evento)::date as mes_evento,
            coalesce(qt_uh_entregues_evento, 0) as qt_uh_entregues_evento,
            dt_referencia as dt_snapshot,
            hash_linha
        from bronze
        where nullif(trim(cast(apf as varchar)), '') is not null
    ),

    dedup as (
        select *, row_number() over (partition by hash_linha order by dt_snapshot) as rn
        from tipado
    )

select
    agente_financeiro,
    apf,
    mes_evento,
    sum(qt_uh_entregues_evento) as uh_entregues_evento_mes,
    count(*) as n_eventos,
    min(dt_snapshot) as dt_primeiro_snapshot
from dedup
where rn = 1 and mes_evento is not null and agente_financeiro is not null
group by agente_financeiro, apf, mes_evento
