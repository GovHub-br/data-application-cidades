{{ config(materialized="table") }}

-- Bronze: Entregas por empreendimento — CAIXA + BB empilhados (union all by name)
-- Fontes: mcmv_staging.dados_prioritarios_recebidos_caixa_entregas (multi-programa)
-- mcmv_staging.dados_prioritarios_recebidos_bb_entregas
-- Contém TODAS as frentes; o filtro por programa é feito na silver via JOIN
-- com o cadastro FDS. Campos-chave: qt_uh_entregues, dt_entrega.
with
    caixa as (
        select
            'CAIXA' as agente,
            {{ normalize_apf('apf') }} as apf,
            nullif(trim(agente_financeiro), '') as agente_financeiro,
            {{ parse_int('qt_uh_entregues') }} as qt_uh_entregues,
            {{ parse_date_br('dt_entrega') }} as dt_entrega,
            {{ parse_date_br('data_de_movimento') }} as dt_movimento,
            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia
        from {{ source("mcmv_staging", "dados_prioritarios_recebidos_caixa_entregas") }}
    ),

    bb as (
        select
            'BB' as agente,
            {{ normalize_apf('apf') }} as apf,
            nullif(trim(agente_financeiro), '') as agente_financeiro,
            {{ parse_int('numero_de_unidades_entregues') }} as qt_uh_entregues,
            {{ parse_date_br('dt_ass_doc') }} as dt_entrega,
            {{ parse_date_br('data_de_movimento') }} as dt_movimento,
            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia
        from {{ source("mcmv_staging", "dados_prioritarios_recebidos_bb_entregas") }}
    )

select *
from caixa
union all by name
select *
from bb
