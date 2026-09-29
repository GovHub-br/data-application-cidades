{{ config(materialized='table') }}

select * from {{ fonte_lake('posicoes_empreendimentos_fgts', 'linha_financiada_lake') }}
