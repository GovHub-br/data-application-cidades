{{ config(materialized="table") }}

-- SILVER do reloginho (grupo A) — entregas por evento agregadas por mes.
--
-- Le as BRONZES DE ENTREGA POR AGENTE (bronze_reloginho_snh_entregas_evento_bb
-- e _caixa) e as une aqui, com projecao explicita e identica por braco — desde
-- a change pipeline-bronze-historica-destino-trocavel (D5) nao existe mais
-- bronze unificada de entregas. Deduplica eventos repetidos (o mesmo
-- evento reaparece em snapshots mensais seguintes) por hash de conteudo de
-- negocio (agente, apf, dt_evento, qtd), e soma a entrega por (agente, apf,
-- mes do EVENTO). Grao: (agente, apf, mes_evento).
--
-- mes_evento = mes de dt_entrega/dt_ass_doc (quando a UH foi entregue), NAO o
-- dt_referencia do arquivo. Assim a serie e um fluxo real de entregas.
--
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`, Postgres
-- atachado em `prod_duckdb`.
{% set entregas_familias = familias_snh_entregas() %}

with

    tipado as (
        {% for f in entregas_familias %}
        select
            coalesce(
                upper(nullif(trim(cast(agente_financeiro as varchar)), '')),
                agente_arquivo
            ) as agente_financeiro,
            nullif(trim(cast(apf as varchar)), '') as apf,
            dt_evento,
            date_trunc('month', dt_evento)::date as mes_evento,
            coalesce(qt_uh_entregues_evento, 0) as qt_uh_entregues_evento,
            dt_referencia as dt_snapshot
        from {{ ref(f.modelo) }}
        where nullif(trim(cast(apf as varchar)), '') is not null
        {{ "union all" if not loop.last }}
        {% endfor %}
    ),

    hashed as (
        select
            *,
            -- Fallback de dedup por conteudo de negocio. Substitui hash_linha
            -- (unico por construcao na bronze — inclui row_number() do
            -- source_file — e por isso nunca detectava o mesmo evento
            -- reaparecendo em snapshots mensais seguintes).
            md5(
                concat_ws(
                    '|',
                    coalesce(agente_financeiro, '␀NULL␀'),
                    coalesce(apf, '␀NULL␀'),
                    coalesce(cast(dt_evento as varchar), '␀NULL␀'),
                    coalesce(cast(qt_uh_entregues_evento as varchar), '␀NULL␀')
                )
            ) as conteudo_hash
        from tipado
    ),

    dedup as (
        select *, row_number() over (partition by conteudo_hash order by dt_snapshot) as rn
        from hashed
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
