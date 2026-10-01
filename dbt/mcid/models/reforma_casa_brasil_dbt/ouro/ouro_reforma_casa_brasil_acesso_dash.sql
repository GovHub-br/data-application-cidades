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
        case coalesce(codigo_sexo_cadunico, sexo_fonte)
            when '1' then 'Masculino'
            when '2' then 'Feminino'
            when 'M' then 'Masculino'
            when 'F' then 'Feminino'
            else 'Não informado'
        end as sexo,
        id_contrato,
        codigo_familiar,
        indicador_encontrado_cadunico,
        valor_financiado,
        renda_familiar_comprovada,
        renda_total_familiar,
        quantidade_membros,
        quantidade_pessoas_domicilio,
        quantidade_comodos,
        quantidade_dormitorios,
        pessoas_por_dormitorio,
        dt_referencia
    from {{ ref('prata_reforma_casa_brasil_acesso') }}
)

select
    uf,
    municipio,
    codigo_ibge,
    faixa_renda,
    raca_cor,
    sexo,
    count(*) as quantidade_contratos,
    count(distinct codigo_familiar) as quantidade_familias_cadunico,
    count(*) filter (where indicador_encontrado_cadunico) as quantidade_encontrada_cadunico,
    round(
        count(*) filter (where indicador_encontrado_cadunico)::numeric / count(*), 4
    ) as proporcao_encontrada_cadunico,
    sum(valor_financiado) as valor_financiado_total,
    avg(valor_financiado) as valor_financiado_medio,
    avg(renda_familiar_comprovada) as renda_familiar_media,
    avg(renda_total_familiar) as renda_total_cadunico_media,
    avg(quantidade_membros) as quantidade_membros_media,
    avg(quantidade_pessoas_domicilio) as quantidade_pessoas_domicilio_media,
    avg(quantidade_comodos) as quantidade_comodos_media,
    avg(quantidade_dormitorios) as quantidade_dormitorios_media,
    avg(pessoas_por_dormitorio) as pessoas_por_dormitorio_media,
    max(dt_referencia) as dt_referencia,
    current_timestamp as dt_ouro
from rotulada
group by 1, 2, 3, 4, 5, 6
-- Proteção contra reidentificação por células pequenas.
having count(*) >= 10
