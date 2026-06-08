{{ config(materialized="table") }}

with
    meta_agg as (
        select
            id_proposta,
            count(id_meta) as qtd_metas,
            min(data_inicio_meta) as inicio_metas,
            max(data_fim_meta) as fim_metas
        from {{ ref("meta_crono_fisico") }}
        group by id_proposta
    ),
    cronograma_agg as (
        select
            id_proposta,
            sum(valor_parcela_crono_desembolso) as total_previsto,
            min(mes_crono_desembolso::text || '-' || ano_crono_desembolso::text) as inicio_cronograma,
            max(mes_crono_desembolso::text || '-' || ano_crono_desembolso::text) as fim_cronograma
        from {{ ref("cronograma_desembolso") }}
        group by id_proposta
    )

select
    m.id_proposta,
    m.qtd_metas,
    m.inicio_metas,
    m.fim_metas,
    c.total_previsto,
    c.inicio_cronograma,
    c.fim_cronograma,
    case
        when m.inicio_metas::text < c.inicio_cronograma then 'METAS ANTES DO CRONOGRAMA'
        when m.inicio_metas::text > c.inicio_cronograma then 'CRONOGRAMA ANTES DAS METAS'
        else 'ALINHADO'
    end as situacao_alinhamento
from meta_agg m
left join cronograma_agg c on m.id_proposta = c.id_proposta