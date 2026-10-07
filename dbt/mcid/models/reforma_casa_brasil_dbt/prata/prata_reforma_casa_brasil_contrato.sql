{{ config(materialized="table") }}

with tipada as (
    select
        md5('reforma|' || trim(nu_contrato::varchar)) as id_contrato,
        nullif(trim(nu_contrato::varchar), '') as nu_contrato,
        nullif(trim(nu_contrato_repasse::varchar), '') as nu_contrato_repasse,
        nullif(trim(nu_contrato_passivo::varchar), '') as nu_contrato_passivo,
        nullif(trim(nu_cpf_cnpj_mutuario::varchar), '') as cpf_hmac,
        nullif(trim(nu_pis::varchar), '') as nis_hmac,
        {{ reforma_casa_brasil_data('dt_referencia') }} as dt_referencia,
        {{ reforma_casa_brasil_data('dt_remessa') }} as dt_remessa,
        {{ reforma_casa_brasil_data('dt_evento') }} as dt_contratacao,
        upper(nullif(trim(sg_sexo::varchar), '')) as sexo_fonte,
        nullif(trim(sg_uf_imovel::varchar), '') as uf,
        nullif(trim(no_municipio_imovel::varchar), '') as municipio,
        regexp_replace(nullif(trim(co_municipio_ibge::varchar), ''), '\\.0$', '') as codigo_ibge,
        nullif(trim(linha_apf::varchar), '') as linha_apf,
        nullif(trim(modalidade::varchar), '') as modalidade,
        nullif(trim(faixa_renda::varchar), '') as faixa_renda,
        -- A remessa do RCB traz somente o código "5" e não acompanha o
        -- dicionário de domínio. Preservamos o valor sem atribuir uma
        -- tipologia imobiliária que a fonte não comprovou.
        case nullif(trim(tipo_imovel::varchar), '')
            when '5' then 'Código 5 — dicionário não recebido'
            else nullif(trim(tipo_imovel::varchar), '')
        end as tipo_imovel,
        nullif(trim(tipo_desembolso::varchar), '') as tipo_desembolso,
        nullif(trim(nu_tipo_garantia::varchar), '') as tipo_garantia,
        nullif(trim(situacao_garantia::varchar), '') as situacao_garantia,
        nullif(trim(no_sistema_amortizacao::varchar), '') as sistema_amortizacao,
        nullif(trim(ic_cotista::varchar), '') as indicador_cotista,
        nullif(trim(nu_legislacao::varchar), '') as legislacao,
        nullif(trim(co_classificacao_imovel::varchar), '') as classificacao_imovel,
        {{ reforma_casa_brasil_numero('vr_renda_familiar_comprovada') }} as renda_familiar_comprovada,
        {{ reforma_casa_brasil_double('pc_renda_informal') }} as percentual_renda_informal,
        {{ reforma_casa_brasil_numero('vr_avaliacao_terreno') }} as valor_avaliacao_terreno,
        {{ reforma_casa_brasil_numero('vr_evento') }} as valor_financiado,
        {{ reforma_casa_brasil_numero('vr_desconto_resolucao_460') }} as valor_desconto,
        {{ reforma_casa_brasil_numero('vr_recurso_proprio') }} as valor_recurso_proprio,
        {{ reforma_casa_brasil_numero('vr_fgts_utilizado') }} as valor_fgts_utilizado,
        {{ reforma_casa_brasil_double('pc_taxa_juros_nominal_inicial') }} as taxa_juros_nominal,
        {{ reforma_casa_brasil_numero('vr_garantia') }} as valor_garantia,
        {{ reforma_casa_brasil_bigint('pz_financiamento') }} as prazo_financiamento_meses,
        {{ reforma_casa_brasil_bigint('nu_dias_atraso') }} as dias_atraso,
        {{ reforma_casa_brasil_numero('vr_prestacao_inicial') }} as valor_prestacao_inicial,
        {{ reforma_casa_brasil_numero('vr_pagamento_amortizacao') }} as valor_amortizacao,
        {{ reforma_casa_brasil_numero('vr_pagamento_juros') }} as valor_juros_pago,
        {{ reforma_casa_brasil_numero('vr_investimento') }} as valor_investimento,
        {{ reforma_casa_brasil_double('nu_area_imovel') }} as area_imovel,
        _source_file as source_file,
        _source_hash as source_hash,
        current_timestamp as dt_prata,
        row_number() over (
            partition by nullif(trim(nu_contrato::varchar), '')
            order by
                coalesce({{ reforma_casa_brasil_data('dt_referencia') }}, date '1900-01-01') desc,
                coalesce({{ reforma_casa_brasil_data('dt_remessa') }}, date '1900-01-01') desc
        ) as rn
    from {{ ref('bronze_sftp_pmcmv_reformas_mcid') }}
    where nullif(trim(nu_contrato::varchar), '') is not null
)

select
    id_contrato,
    nu_contrato,
    nu_contrato_repasse,
    nu_contrato_passivo,
    cpf_hmac,
    nis_hmac,
    dt_referencia,
    dt_remessa,
    dt_contratacao,
    sexo_fonte,
    uf,
    municipio,
    codigo_ibge,
    linha_apf,
    modalidade,
    faixa_renda,
    tipo_imovel,
    tipo_desembolso,
    tipo_garantia,
    situacao_garantia,
    sistema_amortizacao,
    indicador_cotista,
    legislacao,
    classificacao_imovel,
    renda_familiar_comprovada,
    percentual_renda_informal,
    valor_avaliacao_terreno,
    valor_financiado,
    valor_desconto,
    valor_recurso_proprio,
    valor_fgts_utilizado,
    taxa_juros_nominal,
    valor_garantia,
    prazo_financiamento_meses,
    dias_atraso,
    valor_prestacao_inicial,
    valor_amortizacao,
    valor_juros_pago,
    valor_investimento,
    area_imovel,
    source_file,
    source_hash,
    dt_prata
from tipada
where rn = 1
