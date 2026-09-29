{{ config(materialized='table') }}

select * from {{ fonte_lake('orcamento_fgts_2021', 'linha_financiada_lake') }}
union all by name
select * from {{ fonte_lake('orcamento_fgts_2022', 'linha_financiada_lake') }}
union all by name
select * from {{ fonte_lake('orcamento_fgts_2023', 'linha_financiada_lake') }}
union all by name
select * from {{ fonte_lake('orcamento_fgts_2024', 'linha_financiada_lake') }}
