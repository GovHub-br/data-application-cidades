{{ config(materialized="table") }}

with tipada as (
    select
        md5('reforma|' || trim(nu_contrato::varchar)) as id_contrato,
        nullif(trim(nu_contrato::varchar), '') as nu_contrato,
        nullif(trim(nu_contrato_repasse::varchar), '') as nu_contrato_repasse,
        nullif(trim(nu_contrato_passivo::varchar), '') as nu_contrato_passivo,
        nullif(trim(nu_cpf_cnpj_mutuario::varchar), '') as cpf_hmac,
        nullif(trim(nu_pis::varchar), '') as nis_hmac,
        coalesce(
            {{ parse_hist_date('dt_referencia') }},
            try_strptime(nullif(trim(dt_referencia::varchar), ''), '%d/%m/%Y')::date
        ) as dt_referencia,
        coalesce(
            {{ parse_hist_date('dt_remessa') }},
            try_strptime(nullif(trim(dt_remessa::varchar), ''), '%d/%m/%Y')::date
        ) as dt_remessa,
        coalesce(
            {{ parse_hist_date('dt_evento') }},
            try_strptime(nullif(trim(dt_evento::varchar), ''), '%d/%m/%Y')::date
        ) as dt_contratacao,
        upper(nullif(trim(sg_sexo::varchar), '')) as sexo_fonte,
        nullif(trim(sg_uf_imovel::varchar), '') as uf,
        nullif(trim(no_municipio_imovel::varchar), '') as municipio,
        regexp_replace(nullif(trim(co_municipio_ibge::varchar), ''), '\\.0$', '') as codigo_ibge,
        nullif(trim(linha_apf::varchar), '') as linha_apf,
        nullif(trim(modalidade::varchar), '') as modalidade,
        nullif(trim(faixa_renda::varchar), '') as faixa_renda,
        nullif(trim(tipo_imovel::varchar), '') as tipo_imovel,
        nullif(trim(tipo_desembolso::varchar), '') as tipo_desembolso,
        nullif(trim(nu_tipo_garantia::varchar), '') as tipo_garantia,
        nullif(trim(situacao_garantia::varchar), '') as situacao_garantia,
        nullif(trim(no_sistema_amortizacao::varchar), '') as sistema_amortizacao,
        nullif(trim(ic_cotista::varchar), '') as indicador_cotista,
        nullif(trim(nu_legislacao::varchar), '') as legislacao,
        nullif(trim(co_classificacao_imovel::varchar), '') as classificacao_imovel,
        {{ parse_hist_numeric('vr_renda_familiar_comprovada') }} as renda_familiar_comprovada,
        {{ parse_hist_double('pc_renda_informal') }} as percentual_renda_informal,
        {{ parse_hist_numeric('vr_avaliacao_terreno') }} as valor_avaliacao_terreno,
        {{ parse_hist_numeric('vr_evento') }} as valor_financiado,
        {{ parse_hist_numeric('vr_desconto_resolucao_460') }} as valor_desconto,
        {{ parse_hist_numeric('vr_recurso_proprio') }} as valor_recurso_proprio,
        {{ parse_hist_numeric('vr_fgts_utilizado') }} as valor_fgts_utilizado,
        {{ parse_hist_double('pc_taxa_juros_nominal_inicial') }} as taxa_juros_nominal,
        {{ parse_hist_numeric('vr_garantia') }} as valor_garantia,
        {{ parse_hist_bigint('pz_financiamento') }} as prazo_financiamento_meses,
        {{ parse_hist_bigint('nu_dias_atraso') }} as dias_atraso,
        {{ parse_hist_numeric('vr_prestacao_inicial') }} as valor_prestacao_inicial,
        {{ parse_hist_numeric('vr_pagamento_amortizacao') }} as valor_amortizacao,
        {{ parse_hist_numeric('vr_pagamento_juros') }} as valor_juros_pago,
        {{ parse_hist_numeric('vr_investimento') }} as valor_investimento,
        {{ parse_hist_double('nu_area_imovel') }} as area_imovel,
        _source_file as source_file,
        _source_hash as source_hash,
        current_timestamp as dt_prata,
        row_number() over (
            partition by nullif(trim(nu_contrato::varchar), '')
            order by
                coalesce({{ parse_hist_date('dt_referencia') }}, date '1900-01-01') desc,
                coalesce({{ parse_hist_date('dt_remessa') }}, date '1900-01-01') desc
        ) as rn
    from {{ ref('bronze_sftp_pmcmv_reformas_mcid') }}
    where nullif(trim(nu_contrato::varchar), '') is not null
)

select * exclude (rn)
from tipada
where rn = 1
