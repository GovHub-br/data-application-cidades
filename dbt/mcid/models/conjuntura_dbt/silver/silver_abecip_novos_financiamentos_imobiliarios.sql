{{ config(materialized="table") }}

with
    base as (
        select
            ano,
            mes,
            id_agente,
            agente,
            posicao_mes_rs_milhoes,
            posicao_mes_unidades,
            posicao_ano_rs_milhoes,
            posicao_ano_unidades
        from
            {{
                source(
                    "conjuntura_bronze",
                    "bronze_abecip_novos_financiamentos_imobiliarios",
                )
            }}
    ),

    agrupado as (
        select
            ano,
            mes,
            case
                when agente = 'CAIXA'
                then 'CEF (CAIXA)'
                when agente = 'ITAÚ UNIBANCO'
                then 'ITAU'
                when agente = 'BRADESCO'
                then 'BRADESCO'
                when agente = 'SANTANDER'
                then 'SANTANDER'
                when agente = 'BRB'
                then 'BRB'
                when agente = 'BANCO DO BRASIL'
                then 'BB (BANCO DO BRASIL)'
                else 'DEMAIS'
            end as banco,
            sum(posicao_ano_rs_milhoes) as valor_ano_milhoes,
            sum(posicao_ano_unidades) as uh_ano
        from base
        group by ano, mes, banco
    ),

    total as (
        select
            ano,
            mes,
            'TOTAL' as banco,
            sum(posicao_ano_rs_milhoes) as valor_ano_milhoes,
            sum(posicao_ano_unidades) as uh_ano
        from base
        group by ano, mes
    ),

    unificado as (
        select ano, mes, banco, valor_ano_milhoes, uh_ano
        from agrupado
        union all
        select ano, mes, banco, valor_ano_milhoes, uh_ano
        from total
    )

select
    ano,
    mes,
    banco,
    (valor_ano_milhoes / 1000.0)::numeric as valor_bi,
    uh_ano as uh,
    {{ add_metadata_timestamps("silver", has_ingest_date=false) }}
from unificado
