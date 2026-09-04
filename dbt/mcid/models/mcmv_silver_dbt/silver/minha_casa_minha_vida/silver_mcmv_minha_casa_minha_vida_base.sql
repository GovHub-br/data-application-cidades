{{ config(materialized="table") }}

select *
from {{ ref("silver_mcmv_frentes_base") }}
where frente_mcmv <> 'Conjuntura'
