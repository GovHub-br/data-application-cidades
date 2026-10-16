{{ config(materialized="table") }}

-- Série de fotografias administrativas do GEFUS. O grão é contrato por data
-- de referência e arquivo de snapshot; não é uma tabela de pagamentos nem de
-- medição física de obra.
with tipada as (
    select
        md5('reforma|' || trim(nu_contrato::varchar)) as id_contrato,
        coalesce(
            {{ reforma_casa_brasil_data('dt_referencia') }},
            {{ reforma_casa_brasil_data_arquivo('filename') }}
        ) as dt_referencia,
        {{ reforma_casa_brasil_data('dt_remessa') }} as dt_remessa,
        {{ reforma_casa_brasil_data('dt_evento') }} as dt_contratacao,
        nullif(trim(sg_uf_imovel::varchar), '') as uf,
        nullif(trim(no_municipio_imovel::varchar), '') as municipio,
        regexp_replace(nullif(trim(co_municipio_ibge::varchar), ''), '\\.0$', '') as codigo_ibge,
        nullif(trim(linha_apf::varchar), '') as linha_apf,
        nullif(trim(modalidade::varchar), '') as modalidade,
        nullif(trim(faixa_renda::varchar), '') as faixa_renda,
        nullif(trim(tipo_imovel::varchar), '') as tipo_imovel,
        nullif(trim(tipo_desembolso::varchar), '') as tipo_desembolso,
        nullif(trim(situacao_garantia::varchar), '') as situacao_garantia,
        nullif(trim(no_sistema_amortizacao::varchar), '') as sistema_amortizacao,
        {{ reforma_casa_brasil_numero('vr_renda_familiar_comprovada') }} as renda_familiar_comprovada,
        {{ reforma_casa_brasil_numero('vr_evento') }} as valor_financiado,
        {{ reforma_casa_brasil_numero('vr_desconto_resolucao_460') }} as valor_desconto,
        {{ reforma_casa_brasil_numero('vr_recurso_proprio') }} as valor_recurso_proprio,
        {{ reforma_casa_brasil_numero('vr_fgts_utilizado') }} as valor_fgts_utilizado,
        {{ reforma_casa_brasil_numero('vr_investimento') }} as valor_investimento,
        {{ reforma_casa_brasil_numero('vr_prestacao_inicial') }} as valor_prestacao_inicial,
        {{ reforma_casa_brasil_double('pc_taxa_juros_nominal_inicial') }} as taxa_juros_nominal,
        {{ reforma_casa_brasil_bigint('pz_financiamento') }} as prazo_financiamento_meses,
        {{ reforma_casa_brasil_bigint('nu_dias_atraso') }} as dias_atraso,
        cast(filename as varchar) as arquivo_snapshot,
        _source_file as source_file,
        _source_hash as source_hash,
        current_timestamp as dt_prata,
        row_number() over (
            partition by
                nullif(trim(nu_contrato::varchar), ''),
                coalesce(
                    {{ reforma_casa_brasil_data('dt_referencia') }},
                    {{ reforma_casa_brasil_data_arquivo('filename') }}
                ),
                cast(filename as varchar)
            order by {{ reforma_casa_brasil_data('dt_remessa') }} desc nulls last
        ) as rn
    from {{ ref('bronze_sftp_pmcmv_reformas_mcid_historico') }}
    where nullif(trim(nu_contrato::varchar), '') is not null
)

select
    id_contrato,
    dt_referencia,
    dt_remessa,
    dt_contratacao,
    uf,
    municipio,
    codigo_ibge,
    linha_apf,
    modalidade,
    faixa_renda,
    tipo_imovel,
    tipo_desembolso,
    situacao_garantia,
    sistema_amortizacao,
    renda_familiar_comprovada,
    valor_financiado,
    valor_desconto,
    valor_recurso_proprio,
    valor_fgts_utilizado,
    valor_investimento,
    valor_prestacao_inicial,
    taxa_juros_nominal,
    prazo_financiamento_meses,
    dias_atraso,
    arquivo_snapshot,
    source_file,
    source_hash,
    dt_prata
from tipada
where rn = 1 and dt_referencia is not null
