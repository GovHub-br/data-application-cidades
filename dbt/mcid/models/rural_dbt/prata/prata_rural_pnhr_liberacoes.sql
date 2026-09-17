{{ config(materialized="table") }}

-- Silver: Liberações históricas do PNHR (INT055 CAIXA/BB)
-- Fonte: bronze.bronze_sftp_int055_liberacoes_caixa_bb (parquet da staging/ carregado
-- pelo staging_para_bronze.py)
-- Saída: uma linha por liberação de recurso do PNHR, tipada, para a série financeira.
-- O filtro de programa (%PNHR%) fica aqui, junto do resto da regra de leitura da fonte.
with
    liberacoes_raw as (
        select
            {{ var('schema_udfs') }}.normalize_apf(nu_apf) as apf,
            nullif(trim({{ var('schema_udfs') }}.corrigir_mojibake(no_programa)), '') as programa,

            -- Valores
            {{ parse_financial_value("vr_valor") }} as vr_liberado,

            -- Datas: o INT055 chega com ISO em algumas remessas e dd/mm/aaaa em outras
            case
                when data_liberacao is null or trim(data_liberacao) = ''
                then null
                when data_liberacao ~ '^\d{4}-\d{2}-\d{2}'
                then data_liberacao::date
                else {{ var('schema_udfs') }}.parse_date_br(data_liberacao)
            end as dt_liberacao,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ var('schema_udfs') }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ ref("bronze_sftp_int055_liberacoes_caixa_bb") }}
        where
            nu_apf is not null
            and nullif(trim({{ var('schema_udfs') }}.corrigir_mojibake(data_liberacao)), '') is not null
            and trim(upper(no_programa)) like '%PNHR%'
    )

select *
from liberacoes_raw
where dt_liberacao is not null
