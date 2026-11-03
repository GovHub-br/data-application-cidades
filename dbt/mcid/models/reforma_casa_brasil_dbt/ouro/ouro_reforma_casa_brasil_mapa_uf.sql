{{ config(materialized="table") }}

-- Agregado territorial para visualização nacional. Não contém identificadores
-- pessoais nem representa execução física/orçamentária.
with carteira as (
    select
        upper(coalesce(nullif(trim(uf), ''), 'NI')) as uf,
        municipio,
        codigo_ibge,
        valor_financiado,
        valor_investimento,
        valor_desconto,
        valor_recurso_proprio,
        valor_fgts_utilizado,
        dias_atraso,
        dt_referencia
    from {{ ref('prata_reforma_casa_brasil_contrato') }}
),
agregado as (
    select
        uf,
        count(*) as quantidade_contratos,
        count(distinct nullif(trim(municipio), '')) as quantidade_municipios,
        sum(valor_financiado) as valor_financiado_total,
        sum(valor_investimento) as valor_investimento_total,
        sum(valor_desconto) as valor_desconto_total,
        sum(valor_recurso_proprio) as valor_recurso_proprio_total,
        sum(valor_fgts_utilizado) as valor_fgts_utilizado_total,
        count(*) filter (where coalesce(dias_atraso, 0) > 0) as quantidade_contratos_com_atraso,
        max(dt_referencia) as dt_referencia
    from carteira
    where uf <> 'NI'
    group by uf
)

select
    case uf
        when 'AC' then 'BR-AC' when 'AL' then 'BR-AL' when 'AP' then 'BR-AP'
        when 'AM' then 'BR-AM' when 'BA' then 'BR-BA' when 'CE' then 'BR-CE'
        when 'DF' then 'BR-DF' when 'ES' then 'BR-ES' when 'GO' then 'BR-GO'
        when 'MA' then 'BR-MA' when 'MT' then 'BR-MT' when 'MS' then 'BR-MS'
        when 'MG' then 'BR-MG' when 'PA' then 'BR-PA' when 'PB' then 'BR-PB'
        when 'PR' then 'BR-PR' when 'PE' then 'BR-PE' when 'PI' then 'BR-PI'
        when 'RJ' then 'BR-RJ' when 'RN' then 'BR-RN' when 'RS' then 'BR-RS'
        when 'RO' then 'BR-RO' when 'RR' then 'BR-RR' when 'SC' then 'BR-SC'
        when 'SP' then 'BR-SP' when 'SE' then 'BR-SE' when 'TO' then 'BR-TO'
    end as iso_3166_2,
    *,
    current_timestamp as dt_ouro
from agregado
