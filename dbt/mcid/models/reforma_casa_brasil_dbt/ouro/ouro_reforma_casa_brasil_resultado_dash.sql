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
        indicador_encontrado_cadunico,
        indicador_inadequacao_observavel,
        pessoas_por_dormitorio,
        codigo_agua_canalizada,
        codigo_banheiro,
        codigo_escoamento_sanitario,
        renda_total_familiar,
        despesas_basicas_declaradas,
        valor_prestacao_inicial,
        valor_financiado,
        valor_recurso_proprio,
        valor_fgts_utilizado,
        dt_referencia
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
        count(*) filter (where indicador_inadequacao_observavel)::numeric
        / nullif(count(*) filter (where indicador_encontrado_cadunico), 0),
        4
    ) as proporcao_inadequacao_observavel_na_linha_de_base,
    avg(pessoas_por_dormitorio) as media_pessoas_por_dormitorio,
    count(*) filter (where codigo_agua_canalizada = '2') as quantidade_sem_agua_canalizada,
    count(*) filter (where codigo_banheiro = '2') as quantidade_sem_banheiro,
    count(*) filter (where codigo_escoamento_sanitario in ('3', '4', '5', '6'))
        as quantidade_escoamento_precario,
    count(*) filter (
        where renda_total_familiar is not null or despesas_basicas_declaradas is not null
    ) as quantidade_com_linha_de_base_financeira,
    avg(renda_total_familiar) as renda_total_familiar_media,
    avg(despesas_basicas_declaradas) as despesas_basicas_declaradas_media,
    avg(valor_prestacao_inicial) as prestacao_inicial_media,
    avg(
        case
            when renda_total_familiar > 0
            then valor_prestacao_inicial / renda_total_familiar
        end
    ) as comprometimento_renda_inicial_medio,
    avg(valor_financiado) as valor_financiado_medio,
    avg(valor_recurso_proprio) as recurso_proprio_medio,
    avg(valor_fgts_utilizado) as fgts_utilizado_medio,
    false as resultado_pos_obra_disponivel,
    false as impacto_financeiro_pos_obra_disponivel,
    'Linha de base CadÚnico; não mede causalmente o efeito posterior da reforma.'
        as ressalva_resultado,
    max(dt_referencia) as dt_referencia,
    current_timestamp as dt_ouro
from rotulada
group by 1, 2, 3, 4, 5
having count(*) >= 10
