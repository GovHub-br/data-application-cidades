{{ config(materialized='table') }}

-- Substitui os rankings manuais por UF e município do relatório semanal.
-- O grão preserva as dimensões que permitem filtrar o painel sem dupla contagem.

with territorial as (
    select
        coalesce(uf, 'Não informado') as uf,
        coalesce(codigo_municipio, municipio, 'Não informado') as id_municipio,
        coalesce(municipio, 'Não informado') as municipio,
        fonte_recurso,
        segmento_linha_financiada,
        count(*) as quantidade_contratos,
        count(distinct codigo_empreendimento) as quantidade_empreendimentos,
        sum(coalesce(valor_financiamento, 0)) as valor_financiamento,
        sum(coalesce(valor_desconto_fgts, 0) + coalesce(valor_desconto_ogu, 0)) as valor_descontos,
        sum(coalesce(valor_contrapartida_informada, 0)) as valor_contrapartida_informada
    from {{ ref('ouro_linha_financiada_base_agregada') }}
    group by 1, 2, 3, 4, 5
),

ranqueado as (
    select
        *,
        dense_rank() over (
            partition by fonte_recurso, segmento_linha_financiada
            order by quantidade_contratos desc, valor_financiamento desc, uf, municipio
        ) as ranking_municipio_contratos,
        dense_rank() over (
            partition by fonte_recurso, segmento_linha_financiada
            order by valor_financiamento desc, quantidade_contratos desc, uf, municipio
        ) as ranking_municipio_financiamento,
        sum(quantidade_contratos) over (
            partition by fonte_recurso, segmento_linha_financiada, uf
        ) as quantidade_contratos_uf,
        sum(valor_financiamento) over (
            partition by fonte_recurso, segmento_linha_financiada, uf
        ) as valor_financiamento_uf,
        sum(valor_descontos) over (
            partition by fonte_recurso, segmento_linha_financiada, uf
        ) as valor_descontos_uf
    from territorial
)

select
    *,
    dense_rank() over (
        partition by fonte_recurso, segmento_linha_financiada
        order by quantidade_contratos_uf desc, valor_financiamento_uf desc, uf
    ) as ranking_uf_contratos,
    dense_rank() over (
        partition by fonte_recurso, segmento_linha_financiada
        order by valor_financiamento_uf desc, quantidade_contratos_uf desc, uf
    ) as ranking_uf_financiamento,
    current_timestamp as dt_gold
from ranqueado
