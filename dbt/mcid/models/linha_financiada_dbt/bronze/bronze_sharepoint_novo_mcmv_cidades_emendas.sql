{{ config(materialized='table') }}

select * from {{ fonte_lake('mcmv_cidades_emendas', 'linha_financiada_lake') }}
