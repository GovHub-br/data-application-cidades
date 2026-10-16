{{ config(materialized='table') }}

select * from {{ fonte_lake('dominio_linha_fgts', 'linha_financiada_lake') }}
