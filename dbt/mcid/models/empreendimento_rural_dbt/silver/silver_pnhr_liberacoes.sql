{{ config(materialized="table") }}

-- Silver: Liberações históricas do PNHR (INT055 CAIXA/BB)
-- Fonte: bronze.bronze_pnhr_liberacoes (parquet da staging/ carregado
-- pelo staging_para_bronze.py)
-- Saída: uma linha por liberação de recurso do PNHR, tipada, para a série financeira.
--
-- Existe para tirar a tipagem do INT055 de dentro da gold: antes o
-- gold/gold_evolucao_financeira.sql lia o text cru do dump e fazia parse ali dentro.
-- O filtro de programa (%PNHR%) fica aqui, junto do resto da regra de leitura da fonte.
with
    liberacoes_raw as (
        select
            {{ target.schema }}.normalize_apf(nu_apf) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_programa)), '') as programa,

            -- Valores
            {{ parse_financial_value("vr_valor") }} as vr_liberado,

            -- Datas: o INT055 chega com ISO em algumas remessas e dd/mm/aaaa em outras
            case
                when data_liberacao is null or trim(data_liberacao) = ''
                then null
                when data_liberacao ~ '^\d{4}-\d{2}-\d{2}'
                then data_liberacao::date
                else {{ target.schema }}.parse_date_br(data_liberacao)
            end as dt_liberacao,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ source("bronze_rural", "bronze_pnhr_liberacoes") }}
        where
            nu_apf is not null
            and nullif(trim({{ target.schema }}.corrigir_mojibake(data_liberacao)), '') is not null
            and trim(upper(no_programa)) like '%PNHR%'
    )

select *
from liberacoes_raw
where dt_liberacao is not null
