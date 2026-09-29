{{ config(materialized='table') }}

select * from {{ fonte_lake('empreendimentos_fgts', 'linha_financiada_lake') }}
