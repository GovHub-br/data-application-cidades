{{ config(materialized="table") }}

with
    emendas_convenio as (
        select *
        from {{ ref("emendas_convenio") }}
    ),
    proposta as (
        select *
        from {{ ref("proposta") }}
    )

select
    ec.*,
    p.uf_proponente,
    p.munic_proponente,
    p.modalidade as modalidade_proposta,
    p.nm_proponente,
    p.natureza_juridica
from emendas_convenio ec
left join proposta p 
    on ec.id_proposta = p.id_proposta