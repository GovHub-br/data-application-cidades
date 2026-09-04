{{ config(materialized="table") }}

select * from {{ ref("silver_mcmv_far_base") }}
union all
select * from {{ ref("silver_mcmv_entidades_base") }}
union all
select * from {{ ref("silver_mcmv_rural_base") }}
union all
select * from {{ ref("silver_mcmv_classe_media_base") }}
union all
select * from {{ ref("silver_mcmv_cidades_base") }}
union all
select * from {{ ref("silver_mcmv_reforma_base") }}
union all
select * from {{ ref("silver_mcmv_pro_moradia_base") }}
union all
select * from {{ ref("silver_mcmv_sub50_base") }}
union all
select * from {{ ref("silver_mcmv_conjuntura_base") }}
