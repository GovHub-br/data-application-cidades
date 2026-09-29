{{ config(materialized='table') }}

select * from {{ fonte_lake('execucao_fundo_social', 'linha_financiada_lake') }}
