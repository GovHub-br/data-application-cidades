{{ config(materialized="table") }}

-- BRONZE do reloginho (grupo A) — entregas por EVENTO (fluxo), CAIXA + BB.
--
-- Complementa bronze_reloginho_snh_serie_mensal (que traz o ACUMULADO). Enquanto
-- aquele responde "quantas UH entregues ate o mes X", este responde "quantas UH
-- foram entregues NO mes X" (fluxo), necessario para o ritmo_recente e para o
-- caminho alternativo do total de entregas (decisao #5 da #130: 1.518.598 por
-- evento vs 1.543.432 pelo acumulado).
--
-- Fontes (staging/dados_historicos/, 2024-06 -> 2026-03):
--   o_recente_YYYYMM_..._af_caixa_entregas        -> apf, dt_entrega, qt_uh_entregues
--   YYYYMM_..._da_entrega_da_unidade_af_bb (+ truncados 024_10_...) ->
--       apf, dt_ass_doc, numero_de_unidades_entregues
--
-- Responsabilidade desta camada: 1 linha por linha de origem, sem regra de
-- negocio; union_by_name entre as duas familias; dt_referencia do nome do
-- arquivo; helpers harmonizados e hash_linha para a silver.
--
-- Target obrigatorio: staging_duckdb (gating em dbt_project.yml).

with

fonte as (
    select
        *,
        filename as source_file,
        {{ hist_dt_referencia('report_date', 'filename') }} as dt_referencia,
        case
            when lower(filename) like '%af_caixa%' then 'CAIXA'
            when lower(filename) like '%af_b%'     then 'BB'
        end as agente_arquivo,
        current_timestamp as dt_ingest
    from {{ read_minio_staging_parquet_series(
        'dados_historicos/*snh_pmcmv_dados_prioritarios*entrega*.parquet'
    ) }}
)

select
    *,
    -- helpers harmonizados (tipagem/normalizacao fina fica na silver)
    coalesce(
        try_cast(dt_entrega as date),
        try_cast(dt_ass_doc as date)
    ) as dt_evento,
    {{ parse_hist_bigint('coalesce(qt_uh_entregues, numero_de_unidades_entregues)') }} as qt_uh_entregues_evento,
    md5(concat_ws(
        '|',
        coalesce(cast(agente_financeiro as varchar), agente_arquivo, ''),
        coalesce(cast(apf as varchar), ''),
        coalesce(cast(dt_entrega as varchar), cast(dt_ass_doc as varchar), ''),
        coalesce(cast(qt_uh_entregues as varchar), cast(numero_de_unidades_entregues as varchar), ''),
        coalesce(cast(dt_referencia as varchar), '')
    )) as hash_linha
from fonte
