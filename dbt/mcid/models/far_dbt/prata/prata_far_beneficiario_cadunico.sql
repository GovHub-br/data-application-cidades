{{ config(materialized="table") }}

-- Prata: atributos do CadÚnico de cada contrato do FAR cujo titular está no cadastro.
-- O CadÚnico é a foto de 12/2025 e 01/2026, não a da assinatura. Raça/cor é dado
-- sensível: esta tabela é restrita e o painel só lê a ouro agregada.
-- Códigos do dicionário de variáveis do CadÚnico, versão 7.
with
    base as (
        select
            geracao,
            agente_financeiro,
            nu_contrato,
            nullif(trim(co_parentesco_rf_pessoa), '') as co_parentesco,
            nullif(trim(co_raca_cor_pessoa), '') as co_raca_cor,
            nullif(trim(co_deficiencia_memb), '') as co_deficiencia,
            nullif(trim(co_tempo_rua_memb), '') as co_tempo_rua,
            ic_familia_encontrada,
            nullif(trim(cd_ibge_cadastro), '') as cod_ibge_cadastro,
            case
                when trim(dt_atualizacao_fam) ~ '^\d{4}-\d{2}-\d{2}' then left(trim(dt_atualizacao_fam), 10)::date
                when trim(dt_atualizacao_fam) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(trim(dt_atualizacao_fam), 'DD/MM/YYYY')
            end as dt_atualizacao_cadastro,
            nullif(trim(marc_pbf), '') as marc_pbf,
            {{ parse_int("qt_membro_familia") }} as qt_membros_cadunico,
            {{ parse_numeric("vl_renda_media_fam") }} as renda_per_capita_cadunico,
            {{ parse_numeric("vl_desp_energia_fam") }} as despesa_energia,
            {{ parse_numeric("vl_desp_agua_esgoto_fam") }} as despesa_agua_esgoto
        from {{ ref("bronze_sftp_cadunico_beneficiario_far") }}
    )

select
    geracao || '|' || agente_financeiro || '|' || nu_contrato as chave_contrato,
    geracao,
    agente_financeiro,
    nu_contrato,
    co_parentesco = '1' as ic_responsavel_familiar,
    case co_raca_cor
        when '1' then 'Branca'
        when '2' then 'Preta'
        when '3' then 'Amarela'
        when '4' then 'Parda'
        when '5' then 'Indígena'
        else 'Não informada'
    end as raca_cor,
    case co_deficiencia when '1' then true when '2' then false end as ic_pcd,
    -- o bloco de situação de rua só é preenchido para quem está nessa situação
    co_tempo_rua is not null as ic_situacao_rua,
    case co_tempo_rua
        when '1' then 'Até 6 meses'
        when '2' then '6 meses a 1 ano'
        when '3' then '1 a 2 anos'
        when '4' then '2 a 5 anos'
        when '5' then '5 a 10 anos'
        when '6' then 'Mais de 10 anos'
    end as tempo_situacao_rua,
    ic_familia_encontrada,
    cod_ibge_cadastro,
    dt_atualizacao_cadastro,
    case marc_pbf when '1' then true when '0' then false end as ic_bolsa_familia,
    qt_membros_cadunico,
    renda_per_capita_cadunico,
    -- acima de R$ 5.000 por mês é erro de digitação
    case when despesa_energia <= 5000 then despesa_energia end as despesa_energia,
    case when despesa_agua_esgoto <= 5000 then despesa_agua_esgoto end as despesa_agua_esgoto
from base
