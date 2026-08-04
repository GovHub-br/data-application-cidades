{{ config(materialized='table') }}

with contratos as (
    select * from {{ source('fgts', 'fgts_canal_tab_ao_1_contratos_fgts') }}
),
operacoes as (
    select * from {{ source('fgts', 'fgts_canal_tab_ao_1_operacoes_pj_pf') }}
),
areas as (
    select * from {{ source('fgts', 'fgts_canal_tdom_ao_1_area') }}
),
linhas as (
    select * from {{ source('fgts', 'fgts_canal_tdom_ao_1_linha') }}
),
modalidades as (
    select * from {{ source('fgts', 'fgts_canal_tdom_ao_1_modalidade') }}
),
entidades as (
    select * from {{ source('fgts', 'fgts_canal_tdom_ao_1_entidades') }}
)

select
    c.cod_contrato::varchar,
    c.cod_operacao_agente_financeiro::varchar,
    c.cod_empreendimento::varchar,
    
    -- Tipo da Operação (PJ ou PF)
    coalesce(op.tipo, 'Não Informado')::varchar as tipo_operacao,
    
    -- Tomador / Entidade
    c.cod_tomador::varchar,
    coalesce(e.entidade, 'Não Informado')::varchar as tomador_nome,
    
    -- Categorização do Contrato
    c.cod_area::varchar,
    coalesce(a.area, 'Não Informado')::varchar as area_descricao,
    c.cod_linha::varchar,
    coalesce(l.linha, 'Não Informado')::varchar as linha_descricao,
    c.cod_modalidade::varchar,
    coalesce(m.modalidade, 'Não Informado')::varchar as modalidade_descricao,
    
    -- Datas tratadas (evitando datas sentinelas e erros de datestyle MDY)
    to_timestamp(nullif(c.dte_assinatura, '1900-01-01 00:00:00'), 'MM/DD/YY HH24:MI:SS') as data_assinatura,
    c.dte_orcamento_ano::integer,
    
    -- Valores Financeiros (macro já retorna numeric(15,2))
    {{ parse_financial_value('c.vlr_contratado') }} as valor_contratado,
    {{ parse_financial_value('c.vlr_investimento') }} as valor_investimento,
    
    -- Status
    coalesce(c.cod_situacao_contrato, 'Não Informado')::varchar as status_contrato

from contratos c
left join operacoes op on c.cod_linha = op.cod_linha and c.cod_objetivo = op.cod_objetivo
left join areas a on c.cod_area = a.codigo
left join linhas l on c.cod_linha = l.codigo
left join modalidades m on c.cod_modalidade = m.codigo
left join entidades e on c.cod_tomador = e.codigo
