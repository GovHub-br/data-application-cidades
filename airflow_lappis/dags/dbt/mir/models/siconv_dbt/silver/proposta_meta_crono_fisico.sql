{{ config(materialized="table") }}

with
    proposta as (
        select *
        from {{ ref("proposta") }}
    ),
    meta_agg as (
        select
            id_proposta,
            count(id_meta) as qtd_metas,
            sum(vl_meta) as valor_total_metas,
            string_agg(distinct desc_meta, ' | ') as descricoes_metas,
            string_agg(distinct uf_meta, ', ') as ufs_atendidas,
            string_agg(distinct municipio_meta, ', ') as municipios_atendidos,
            string_agg(distinct endereco_meta, ' | ') as enderecos,
            min(data_inicio_meta) as inicio_metas,
            max(data_fim_meta) as fim_metas
        from {{ ref("meta_crono_fisico") }}
        group by id_proposta
    )

select
    p.*,
    m.qtd_metas,
    m.valor_total_metas,
    m.descricoes_metas,
    m.ufs_atendidas,
    m.municipios_atendidos,
    m.enderecos,
    m.inicio_metas,
    m.fim_metas
from proposta p
left join meta_agg m on p.id_proposta = m.id_proposta