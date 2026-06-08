{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    termo_aditivo_agg as (
        select
            nr_convenio,
            count(numero_ta) as qtd_termos_aditivos,
            sum(vl_global_ta) as total_valor_aditivado,
            min(dt_assinatura_ta) as primeiro_aditivo,
            max(dt_assinatura_ta) as ultimo_aditivo,
            max(dt_fim_ta) as novo_prazo_vigencia,
            string_agg(distinct tipo_ta, ', ') as tipos_aditivo,
            string_agg(distinct justificativa_ta, ' | ') as justificativas
        from {{ ref("termo_aditivo") }}
        group by nr_convenio
    )

select
    c.*,
    t.qtd_termos_aditivos,
    t.total_valor_aditivado,
    t.primeiro_aditivo,
    t.ultimo_aditivo,
    t.novo_prazo_vigencia,
    t.tipos_aditivo,
    t.justificativas
from convenio c
left join termo_aditivo_agg t on c.nr_convenio = t.nr_convenio