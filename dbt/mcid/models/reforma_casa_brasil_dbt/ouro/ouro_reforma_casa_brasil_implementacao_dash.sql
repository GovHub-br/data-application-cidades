{{ config(materialized="table") }}

select
    coalesce(uf, 'Não informado') as uf,
    coalesce(municipio, 'Não informado') as municipio,
    coalesce(codigo_ibge, codigo_ibge_cadastro, 'Não informado') as codigo_ibge,
    coalesce(faixa_renda, 'Não informada') as faixa_renda,
    coalesce(modalidade, 'Não informada') as modalidade,
    coalesce(tipo_desembolso, 'Não informado') as tipo_desembolso,
    coalesce(sistema_amortizacao, 'Não informado') as sistema_amortizacao,
    count(*) as quantidade_contratos,
    sum(valor_financiado) as valor_financiado_total,
    avg(valor_financiado) as valor_financiado_medio,
    avg(valor_recurso_proprio) as recurso_proprio_medio,
    avg(valor_fgts_utilizado) as fgts_utilizado_medio,
    avg(valor_prestacao_inicial) as prestacao_inicial_media,
    avg(taxa_juros_nominal) as taxa_juros_media,
    avg(prazo_financiamento_meses) as prazo_medio_meses,
    count(*) filter (where coalesce(dias_atraso, 0) > 0) as quantidade_com_atraso,
    round(
        count(*) filter (where coalesce(dias_atraso, 0) > 0)::double / count(*), 4
    ) as proporcao_com_atraso,
    max(dt_referencia) as dt_referencia,
    current_timestamp as dt_ouro
from {{ ref('prata_reforma_casa_brasil_acesso') }}
group by 1, 2, 3, 4, 5, 6, 7
having count(*) >= 10
