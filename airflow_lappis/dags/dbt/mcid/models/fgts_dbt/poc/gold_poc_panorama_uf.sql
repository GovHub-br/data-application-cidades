{#
    ===========================================================================
    Gold -- panorama por UF
    ===========================================================================
    Postgres puro. Nao ha uma unica referencia a MinIO, parquet ou pg_duckdb --
    esta camada nao sabe de onde o dado veio, e essa e a propriedade que o
    desenho preserva.

    PADROES EXERCITADOS AQUI
      - agregacao cruzando QUATRO silvers, com granularidades diferentes:
        empreendimento, posicao mensal, contrato e execucao de obra
      - TABELA-PONTE: obras sao por contrato e empreendimentos por
        empreendimento; silver_poc_contratos carrega as duas chaves e faz a
        ligacao. Ligar obra direto a empreendimento estaria errado
      - filtro pela posicao mais recente (is_posicao_atual), evitando somar o
        historico inteiro
      - count(distinct) e filter (where ...), sintaxe Postgres normal
      - divisao protegida com nullif, para nao estourar em UF sem obra
      - A SILVER PRESERVA NULL em medida e data; a GOLD decide o que exibir.
        min, max e avg ignoram nulo por definicao e ficam corretos sem
        tratamento nenhum -- e essa e a vantagem de nao ter zerado na silver.
        Onde o consumidor precisa de numero, medida_ou_zero() torna a decisao
        explicita e visivel na revisao
      - formatacao brasileira e fuso local apenas na saida, em colunas de
        exibicao que convivem com as numericas
#}

{{
    config(
        enabled=var('fgts_poc_enabled', false),
        materialized='table',
        schema='fgts_poc',
        tags=['fgts_poc']
    )
}}

with empreendimentos as (
    select * from {{ ref('silver_poc_empreendimentos') }}
),

posicao_atual as (
    select *
    from {{ ref('silver_poc_posicoes') }}
    where is_posicao_atual
),

contratos as (
    select cod_contrato, cod_empreendimento
    from {{ ref('silver_poc_contratos') }}
),

obras as (
    select
        c.cod_empreendimento,
        o.cod_contrato,
        o.is_paralisada
    from {{ ref('silver_poc_execucao_obras') }} o
    join contratos c on o.cod_contrato = c.cod_contrato
)

select
    e.municipio_uf,

    count(distinct e.cod_empreendimento)                as qtd_empreendimentos,
    count(distinct e.municipio_nome)                    as qtd_municipios,
    {{ medida_ou_zero('sum(e.quantidade_uh)') }}        as total_unidades,

    -- media correta de graca: o nulo da silver e ignorado pela agregacao
    avg(p.percentual_executado)::numeric(15, 2)         as percentual_medio_executado,

    min(p.data_inicio)                                  as inicio_obra_mais_antigo,
    max(p.data_termino)                                 as termino_obra_mais_recente,
    count(*) filter (where {{ tem_data('p.data_inauguracao') }})
                                                        as qtd_com_inauguracao,
    count(distinct p.cod_empreendimento)
        filter (where p.percentual_executado >= 100)    as qtd_concluidos,

    count(distinct o.cod_contrato)
        filter (where o.is_paralisada)                  as qtd_contratos_paralisados,

    round(
        100.0 * count(distinct o.cod_contrato) filter (where o.is_paralisada)
        / nullif(count(distinct o.cod_contrato), 0),
        2
    )                                                   as pct_contratos_paralisados,

    -- colunas de exibicao, ao lado das numericas -- nunca no lugar delas
    {{ formata_valor_br('sum(e.quantidade_uh)', casas=0) }}
                                                        as total_unidades_exibicao,
    {{ formata_data_br('max(p.data_termino)') }}        as ultimo_termino_exibicao

from empreendimentos e
left join posicao_atual p on e.cod_empreendimento = p.cod_empreendimento
left join obras o         on e.cod_empreendimento = o.cod_empreendimento

group by e.municipio_uf
order by total_unidades desc nulls last
