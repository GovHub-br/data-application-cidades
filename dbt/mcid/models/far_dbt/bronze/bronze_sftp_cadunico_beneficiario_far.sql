{{ config(materialized="table") }}

-- Bronze: CadÚnico dos titulares do FAR, casado pelo CPF em HMAC (mesmo segredo nas
-- duas fontes). Pessoa do corte 12/12/2025 e família do corte 09/01/2026, porque o
-- de 12/12/2025 só tem metade das famílias. Não guarda o CPF.
-- Lê o CadÚnico filtrado pelas chaves do FAR: varrê-lo inteiro estoura a memória do DW.
with
    far as (
        select 'Novo MCMV' as geracao, 'CAIXA' as agente_financeiro,
            trim(nu_contrato) as nu_contrato, trim(nu_cpf_beneficiario) as cpf
        from {{ ref("bronze_sftp_mont_cont_pf_far") }}
        union all
        select 'Legado (2009–2023)', 'CAIXA', trim(nu_contrato), trim(nu_cpf_cgc_mutuario)
        from {{ ref("bronze_sftp_int039_far_caixa_pf") }}
        union all
        select 'Legado (2009–2023)', 'Banco do Brasil', trim(nu_contrato), trim(nu_cpf_cgc_mutuario)
        from {{ ref("bronze_sftp_int021_far_bb_pf") }}
    ),

    pessoa as (
        select
            cast(r['nu_cpf_pessoa'] as varchar) as cpf,
            -- a chave da família vem sem os zeros à esquerda em parte das linhas
            ltrim(cast(r['co_familiar_fam'] as varchar), '0') as co_familiar_fam,
            cast(r['co_parentesco_rf_pessoa'] as varchar) as co_parentesco_rf_pessoa,
            cast(r['co_raca_cor_pessoa'] as varchar) as co_raca_cor_pessoa,
            cast(r['co_deficiencia_memb'] as varchar) as co_deficiencia_memb,
            cast(r['co_tempo_rua_memb'] as varchar) as co_tempo_rua_memb
        from {{ fonte_lake('cadunico_pessoa', 'lake_staging_reforma_casa_brasil') }} as r
        where cast(r['nu_cpf_pessoa'] as varchar) in (select cpf from far)
    ),

    familia as (
        select
            ltrim(cast(r['co_familiar_fam'] as varchar), '0') as co_familiar_fam,
            cast(r['cd_ibge_cadastro'] as varchar) as cd_ibge_cadastro,
            cast(r['dt_atualizacao_fam'] as varchar) as dt_atualizacao_fam,
            cast(r['marc_pbf'] as varchar) as marc_pbf,
            cast(r['qt_membro_familia'] as varchar) as qt_membro_familia,
            cast(r['vl_renda_media_fam'] as varchar) as vl_renda_media_fam,
            cast(r['vl_desp_energia_fam'] as varchar) as vl_desp_energia_fam,
            cast(r['vl_desp_agua_esgoto_fam'] as varchar) as vl_desp_agua_esgoto_fam
        from {{ fonte_lake('cadunico_familia_beneficiario', 'lake_staging_far') }} as r
        where ltrim(cast(r['co_familiar_fam'] as varchar), '0') in (select co_familiar_fam from pessoa)
    )

select
    far.geracao,
    far.agente_financeiro,
    far.nu_contrato,
    p.co_parentesco_rf_pessoa,
    p.co_raca_cor_pessoa,
    p.co_deficiencia_memb,
    p.co_tempo_rua_memb,
    f.co_familiar_fam is not null as ic_familia_encontrada,
    f.cd_ibge_cadastro,
    f.dt_atualizacao_fam,
    f.marc_pbf,
    f.qt_membro_familia,
    f.vl_renda_media_fam,
    f.vl_desp_energia_fam,
    f.vl_desp_agua_esgoto_fam
from far
join pessoa as p on p.cpf = far.cpf
left join familia as f on f.co_familiar_fam = p.co_familiar_fam
