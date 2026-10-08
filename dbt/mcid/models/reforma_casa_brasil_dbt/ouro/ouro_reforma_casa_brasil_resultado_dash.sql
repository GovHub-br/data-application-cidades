{{ config(materialized="table") }}

with rotulada as (
    select
        coalesce(uf, 'Não informado') as uf,
        coalesce(municipio, 'Não informado') as municipio,
        coalesce(codigo_ibge, codigo_ibge_cadastro, 'Não informado') as codigo_ibge,
        coalesce(faixa_renda, 'Não informada') as faixa_renda,
        case codigo_raca_cor
            when '1' then 'Branca'
            when '2' then 'Preta'
            when '3' then 'Amarela'
            when '4' then 'Parda'
            when '5' then 'Indígena'
            else 'Não informada'
        end as raca_cor,
        * exclude (uf, municipio, codigo_ibge, faixa_renda)
    from {{ ref('prata_reforma_casa_brasil_acesso') }}
)

select
    uf,
    municipio,
    codigo_ibge,
    faixa_renda,
    raca_cor,
    count(*) as quantidade_contratos,
    count(*) filter (where indicador_inadequacao_observavel)
        as quantidade_com_inadequacao_observavel_na_linha_de_base,
    round(
        count(*) filter (where indicador_inadequacao_observavel)::double
        / nullif(count(*) filter (where indicador_encontrado_cadunico), 0),
        4
    ) as proporcao_inadequacao_observavel_na_linha_de_base,
    avg(pessoas_por_dormitorio) as media_pessoas_por_dormitorio,
    count(*) filter (where codigo_agua_canalizada = '2') as quantidade_sem_agua_canalizada,
    count(*) filter (where codigo_banheiro = '2') as quantidade_sem_banheiro,
    count(*) filter (where codigo_escoamento_sanitario in ('3', '4', '5', '6'))
        as quantidade_escoamento_precario,
    false as resultado_pos_obra_disponivel,
    'Linha de base CadÚnico; não mede causalmente o efeito posterior da reforma.'
        as ressalva_resultado,
    max(dt_referencia) as dt_referencia,
    current_timestamp as dt_ouro
from rotulada
group by 1, 2, 3, 4, 5
having count(*) >= 10
