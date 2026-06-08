{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    meta_agg as (
        select
            nr_convenio,
            count(id_meta) as qtd_metas,
            sum(vl_meta) as valor_total_metas,
            string_agg(distinct desc_meta, ' | ') as descricoes_metas,
            string_agg(distinct uf_meta, ', ') as ufs_atendidas,
            string_agg(distinct municipio_meta, ', ') as municipios_atendidos,
            min(data_inicio_meta) as inicio_metas,
            max(data_fim_meta) as fim_metas,
            bool_or(data_fim_meta < current_date) as prazo_expirado
        from {{ ref("meta_crono_fisico") }}
        group by nr_convenio
    )

select
    c.*,
    m.qtd_metas,
    m.valor_total_metas,
    m.descricoes_metas,
    m.ufs_atendidas,
    m.municipios_atendidos,
    m.inicio_metas,
    m.fim_metas,
    m.prazo_expirado
from convenio c
left join meta_agg m on c.nr_convenio = m.nr_convenio