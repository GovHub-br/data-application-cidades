{{ config(materialized="table") }}

with
    proposta as (
        select *
        from {{ ref("proposta") }}
    ),
    cronograma_agg as (
        select
            id_proposta,
            sum(valor_parcela_crono_desembolso) as total_previsto,
            sum(case when tipo_resp_crono_desembolso = 'Concedente' 
                then valor_parcela_crono_desembolso else 0 end) as total_previsto_concedente,
            sum(case when tipo_resp_crono_desembolso = 'Convenente' 
                then valor_parcela_crono_desembolso else 0 end) as total_previsto_convenente,
            count(nr_parcela_crono_desembolso) as qtd_parcelas
        from {{ ref("cronograma_desembolso") }}
        group by id_proposta
    )

select
    p.*,
    c.total_previsto,
    c.total_previsto_concedente,
    c.total_previsto_convenente,
    c.qtd_parcelas
from proposta p
left join cronograma_agg c on p.id_proposta = c.id_proposta