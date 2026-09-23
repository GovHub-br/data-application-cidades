{{ config(materialized="table") }}

select
    md5(
        'cadunico-pessoa|'
        || coalesce(nullif(trim(nu_cpf_pessoa::varchar), ''), '')
        || '|'
        || coalesce(nullif(trim(nu_nis_pessoa::varchar), ''), '')
        || '|'
        || coalesce(nullif(trim(co_familiar_fam::varchar), ''), '')
    ) as id_pessoa,
    nullif(trim(nu_cpf_pessoa::varchar), '') as cpf_hmac,
    nullif(trim(nu_nis_pessoa::varchar), '') as nis_hmac,
    nullif(trim(co_familiar_fam::varchar), '') as codigo_familiar,
    regexp_replace(nullif(trim(cd_ibge_cadastro::varchar), ''), '\\.0$', '') as codigo_ibge_cadastro,
    nullif(trim(co_est_cadastral_memb::varchar), '') as situacao_cadastral_pessoa,
    nullif(trim(co_sexo_pessoa::varchar), '') as codigo_sexo,
    nullif(trim(co_parentesco_rf_pessoa::varchar), '') as codigo_parentesco_responsavel,
    nullif(trim(co_raca_cor_pessoa::varchar), '') as codigo_raca_cor,
    nullif(trim(co_deficiencia_memb::varchar), '') as codigo_deficiencia,
    nullif(trim(co_sabe_ler_escrever_memb::varchar), '') as codigo_alfabetizacao,
    nullif(trim(in_frequenta_escola_memb::varchar), '') as indicador_frequenta_escola,
    nullif(trim(co_trabalhou_semana_memb::varchar), '') as codigo_trabalhou_semana,
    nullif(trim(fx_renda_individual_805::varchar), '') as faixa_renda_trabalho,
    nullif(trim(ds_marc_bpc::varchar), '') as marcador_bpc,
    _source_file as source_file,
    _source_hash as source_hash,
    current_timestamp as dt_prata
from {{ ref('bronze_sftp_arq_pessoa_pbf') }}
where
    nullif(trim(co_familiar_fam::varchar), '') is not null
    and (
        nullif(trim(nu_cpf_pessoa::varchar), '') is not null
        or nullif(trim(nu_nis_pessoa::varchar), '') is not null
    )
