{{ config(
    materialized='table',
    alias='bronze_sharepoint_fgts_empreendimentos_posicoes'
) }}

select * from {{ fonte_lake('posicoes_empreendimentos_fgts', 'linha_financiada_lake') }}
