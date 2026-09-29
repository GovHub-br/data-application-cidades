{{ config(materialized='table') }}

select * from {{ fonte_lake('contratos_fgts', 'linha_financiada_lake') }}
