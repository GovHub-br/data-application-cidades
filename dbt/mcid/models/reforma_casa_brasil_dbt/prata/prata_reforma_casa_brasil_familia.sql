{{ config(materialized="table") }}

with base as (
    select
        nullif(trim(co_familiar_fam::varchar), '') as codigo_familiar,
        regexp_replace(nullif(trim(cd_ibge_cadastro::varchar), ''), '\\.0$', '') as codigo_ibge_cadastro,
        coalesce(
            {{ parse_hist_date('dt_cadastro_fam') }},
            try_strptime(nullif(trim(dt_cadastro_fam::varchar), ''), '%d/%m/%Y')::date
        ) as dt_cadastro,
        coalesce(
            {{ parse_hist_date('dt_atualizacao_fam') }},
            try_strptime(nullif(trim(dt_atualizacao_fam::varchar), ''), '%d/%m/%Y')::date
        ) as dt_atualizacao,
        nullif(trim(co_est_cadastral_fam::varchar), '') as situacao_cadastral_familia,
        nullif(trim(marc_pbf::varchar), '') as marcador_pbf,
        {{ parse_hist_numeric('vl_renda_media_fam') }} as renda_media_familiar,
        {{ parse_hist_numeric('vl_renda_total_fam') }} as renda_total_familiar,
        {{ parse_hist_bigint('qt_membro_familia') }} as quantidade_membros,
        {{ parse_hist_bigint('qt_pessoas_domic_fam') }} as quantidade_pessoas_domicilio,
        {{ parse_hist_bigint('qt_comodos_domic_fam') }} as quantidade_comodos,
        {{ parse_hist_bigint('qt_comodos_dormitorio_fam') }} as quantidade_dormitorios,
        nullif(trim(co_local_domic_fam::varchar), '') as codigo_local_domicilio,
        nullif(trim(co_especie_domic_fam::varchar), '') as codigo_especie_domicilio,
        nullif(trim(co_material_piso_fam::varchar), '') as codigo_material_piso,
        nullif(trim(co_material_domic_fam::varchar), '') as codigo_material_domicilio,
        nullif(trim(co_agua_canalizada_fam::varchar), '') as codigo_agua_canalizada,
        nullif(trim(co_abaste_agua_domic_fam::varchar), '') as codigo_abastecimento_agua,
        nullif(trim(co_banheiro_domic_fam::varchar), '') as codigo_banheiro,
        nullif(trim(co_escoa_sanitario_domic_fam::varchar), '') as codigo_escoamento_sanitario,
        nullif(trim(co_destino_lixo_domic_fam::varchar), '') as codigo_destino_lixo,
        nullif(trim(co_iluminacao_domic_fam::varchar), '') as codigo_iluminacao,
        nullif(trim(co_calcamento_domic_fam::varchar), '') as codigo_calcamento,
        nullif(trim(in_familia_indigena_fam::varchar), '') as indicador_familia_indigena,
        nullif(trim(in_familia_quilombola_fam::varchar), '') as indicador_familia_quilombola,
        _source_file as source_file,
        _source_hash as source_hash
    from {{ ref('bronze_sftp_arq_familia_pbf') }}
    where nullif(trim(co_familiar_fam::varchar), '') is not null
)

select
    *,
    case
        when quantidade_dormitorios > 0
        then quantidade_pessoas_domicilio::double / quantidade_dormitorios
    end as pessoas_por_dormitorio,
    -- Proxy operacional; não substitui o conceito oficial de inadequação habitacional.
    (
        codigo_agua_canalizada = '2'
        or codigo_banheiro = '2'
        or codigo_material_domicilio in ('5', '6', '7', '8')
        or codigo_escoamento_sanitario in ('3', '4', '5', '6')
        or (
            quantidade_dormitorios > 0
            and quantidade_pessoas_domicilio::double / quantidade_dormitorios > 3
        )
    ) as indicador_inadequacao_observavel,
    current_timestamp as dt_prata
from base
