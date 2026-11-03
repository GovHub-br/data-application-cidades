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
    -- Trabalho/ocupação. Os códigos são preservados para posterior tradução
    -- pelo dicionário oficial do CadÚnico; não representam serviços da obra.
    nullif(trim(co_trabalhou_semana_memb::varchar), '') as codigo_trabalhou_semana,
    nullif(trim(co_afastado_trab_memb::varchar), '') as codigo_afastado_trabalho,
    nullif(trim(co_agricultura_trab_memb::varchar), '') as codigo_trabalho_agricultura,
    nullif(trim(co_principal_trab_memb::varchar), '') as codigo_ocupacao_principal,
    nullif(trim(co_trabalho_12_meses_memb::varchar), '') as codigo_trabalho_doze_meses,
    nullif(trim(in_motivo_trabalho_memb::varchar), '') as codigo_motivo_trabalho,
    nullif(trim(co_cart_assinada_memb::varchar), '') as codigo_carteira_assinada,
    nullif(trim(fx_renda_individual_805::varchar), '') as faixa_renda_trabalho,
    nullif(trim(in_dinh_const_memb::varchar), '') as indicador_renda_construcao,
    nullif(trim(in_dinh_flanelhinha_memb::varchar), '') as indicador_renda_flanelinha,
    nullif(trim(in_dinh_carregador_memb::varchar), '') as indicador_renda_carregador,
    nullif(trim(in_dinh_catador_memb::varchar), '') as indicador_renda_catador,
    nullif(trim(in_dinh_servs_gerais_memb::varchar), '') as indicador_renda_servicos_gerais,
    nullif(trim(in_dinh_pede_memb::varchar), '') as indicador_renda_pede,
    nullif(trim(in_dinh_vendas_memb::varchar), '') as indicador_renda_vendas,
    nullif(trim(in_dinh_outro_memb::varchar), '') as indicador_renda_outro,
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
