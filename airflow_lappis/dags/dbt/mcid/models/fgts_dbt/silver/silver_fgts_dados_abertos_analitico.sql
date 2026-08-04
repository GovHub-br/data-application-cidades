{{ config(materialized='table') }}

with analitico as (
    select * from {{ source('fgts', 'dados_abertos_mcmv_fgts_analitico') }}
)

select
    txt_uf::varchar as uf,
    txt_municipio::varchar as municipio,
    cod_ibge::varchar as codigo_ibge,
    txt_regiao::varchar as regiao,
    
    co_sexo::varchar as sexo,
    
    -- Tratar a data: ex '2001-04-01'
    to_date(nullif(dte_nascimento, ''), 'YYYY-MM-DD') as data_nascimento,
    
    -- Tratar a data: ex '2025-10-15 00:00:00.000'
    to_timestamp(nullif(data_assinatura_financiamento, ''), 'YYYY-MM-DD HH24:MI:SS.MS') as data_assinatura,
    
    -- Tratar a data: ex '05/12/2025'
    to_date(nullif(data_referencia, ''), 'DD/MM/YYYY') as data_referencia,
    
    txt_tipo_imovel::varchar as tipo_imovel,
    txt_programa_fgts::varchar as programa,
    txt_sistema_amortizacao::varchar as sistema_amortizacao,
    txt_compatibilidade_faixa_renda::varchar as faixa_renda,
    bln_cotista::varchar as is_cotista,
    
    -- Valores Financeiros (Tratamento específico para formato PT-BR dos Dados Abertos)
    -- Ex: '170.046' -> '170046' ou '2.845,27' -> '2845.27'
    replace(replace(coalesce(nullif(vlr_compra, ''), '0'), '.', ''), ',', '.')::numeric(15, 2) as valor_compra,
    replace(replace(coalesce(nullif(vlr_financiamento, ''), '0'), '.', ''), ',', '.')::numeric(15, 2) as valor_financiamento,
    replace(replace(coalesce(nullif(vlr_renda_familiar, ''), '0'), '.', ''), ',', '.')::numeric(15, 2) as valor_renda_familiar,
    replace(replace(coalesce(nullif(vlr_subsidio_desconto_ogu, ''), '0'), '.', ''), ',', '.')::numeric(15, 2) as valor_desconto_ogu,
    replace(replace(coalesce(nullif(vlr_subsidio_desconto_fgts, ''), '0'), '.', ''), ',', '.')::numeric(15, 2) as valor_desconto_fgts,
    replace(replace(coalesce(nullif(vlr_subsidio_equilibrio_ogu, ''), '0'), '.', ''), ',', '.')::numeric(15, 2) as valor_equilibrio_ogu,
    replace(replace(coalesce(nullif(vlr_subsidio_equilibrio_fgts, ''), '0'), '.', ''), ',', '.')::numeric(15, 2) as valor_equilibrio_fgts,
    
    replace(coalesce(nullif(qtd_uh_financiadas, ''), '0'), '.', '')::integer as quantidade_uh_financiadas,
    
    coalesce(txt_nome_empreendimento, 'Não Informado')::varchar as nome_empreendimento

from analitico
