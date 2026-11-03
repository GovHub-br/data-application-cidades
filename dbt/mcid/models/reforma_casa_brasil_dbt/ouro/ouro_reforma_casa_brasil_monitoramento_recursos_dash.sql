{{ config(materialized="table") }}

-- Posição da carteira por competência. Os valores são administrativos do
-- contrato e não substituem empenho, liquidação, pagamento ou medição física.
with base as (
    select
        id_contrato,
        dt_referencia,
        coalesce(uf, 'Não informado') as uf,
        coalesce(municipio, 'Não informado') as municipio,
        coalesce(codigo_ibge, 'Não informado') as codigo_ibge,
        coalesce(faixa_renda, 'Não informada') as faixa_renda,
        coalesce(modalidade, 'Não informada') as modalidade,
        valor_financiado,
        valor_investimento,
        valor_desconto,
        valor_recurso_proprio,
        valor_fgts_utilizado,
        valor_prestacao_inicial,
        taxa_juros_nominal,
        prazo_financiamento_meses,
        dias_atraso
    from {{ ref('prata_reforma_casa_brasil_contrato_historico') }}
),
primeira_ocorrencia as (
    select id_contrato, min(dt_referencia) as dt_primeira_ocorrencia_serie
    from base
    group by id_contrato
),
agregado as (
    select
        b.dt_referencia,
        b.uf,
        b.municipio,
        b.codigo_ibge,
        b.faixa_renda,
        b.modalidade,
        count(*) as quantidade_contratos,
        count(*) filter (
            where p.dt_primeira_ocorrencia_serie = b.dt_referencia
        ) as quantidade_primeira_ocorrencia_serie,
        sum(b.valor_financiado) as valor_financiado_total,
        sum(b.valor_investimento) as valor_investimento_total,
        sum(b.valor_desconto) as valor_desconto_total,
        sum(b.valor_recurso_proprio) as valor_recurso_proprio_total,
        sum(b.valor_fgts_utilizado) as valor_fgts_utilizado_total,
        avg(b.valor_prestacao_inicial) as prestacao_inicial_media,
        avg(b.taxa_juros_nominal) as taxa_juros_media,
        avg(b.prazo_financiamento_meses) as prazo_financiamento_medio_meses,
        count(*) filter (where coalesce(b.dias_atraso, 0) > 0)
            as quantidade_contratos_com_atraso,
        round(
            count(*) filter (where coalesce(b.dias_atraso, 0) > 0)::numeric
            / nullif(count(*), 0),
            4
        ) as proporcao_contratos_com_atraso
    from base b
    left join primeira_ocorrencia p on b.id_contrato = p.id_contrato
    group by
        b.dt_referencia,
        b.uf,
        b.municipio,
        b.codigo_ibge,
        b.faixa_renda,
        b.modalidade
)

select
    *,
    quantidade_contratos
        - lag(quantidade_contratos) over (
            partition by uf, municipio, codigo_ibge, faixa_renda, modalidade
            order by dt_referencia
        ) as variacao_contratos_vs_snapshot_anterior,
    false as execucao_orcamentaria_ou_fisica_disponivel,
    'Posição administrativa da carteira por snapshot; não equivale a execução orçamentária, pagamento ou medição física de obra.'
        as ressalva_monitoramento,
    current_timestamp as dt_ouro
from agregado
