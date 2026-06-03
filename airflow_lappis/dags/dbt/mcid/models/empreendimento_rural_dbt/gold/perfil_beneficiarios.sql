{{ config(materialized="table") }}

-- Gold: Perfil dos Beneficiários (Rural)
-- Consolida os dados socioeconômicos dos beneficiários de cada empreendimento a partir da base de pessoas físicas.

with
    cadastro_pf as (
        select * from {{ ref("cadastro_pf") }}
    ),

    agregado as (
        select
            apf,
            count(nu_registro) as total_beneficiarios_cadastrados,
            avg(coalesce(qt_pessoas_familia, 0)) as media_pessoas_familia,
            
            -- Gênero
            sum(case when upper(trim(beneficiario_sexo)) = 'F' then 1 else 0 end) as total_mulheres,
            sum(case when upper(trim(beneficiario_sexo)) = 'M' then 1 else 0 end) as total_homens,
            
            -- Benefícios Sociais
            sum(case when ic_benef_bolsa_familia then 1 else 0 end) as total_beneficiarios_bolsa_familia,
            sum(case when ic_benef_bpc then 1 else 0 end) as total_beneficiarios_bpc,
            
            -- Renda Familiar
            avg(coalesce(vr_renda_familiar, 0.00)) as renda_familiar_media

        from cadastro_pf
        group by apf
    ),

    fichas as (
        select
            apf,
            nome_empreendimento,
            municipio,
            uf,
            programa,
            quantidade_uh
        from {{ ref("ficha_empreendimento_rural") }}
    )

select
    f.apf,
    f.nome_empreendimento,
    f.municipio,
    f.uf,
    f.programa,
    f.quantidade_uh,
    
    -- Métricas de Cadastro e Demografia
    coalesce(a.total_beneficiarios_cadastrados, 0) as total_beneficiarios_cadastrados,
    round(coalesce(a.media_pessoas_familia, 0.00), 1) as media_pessoas_familia,
    
    -- Proporção de Gênero
    coalesce(a.total_mulheres, 0) as total_mulheres,
    case 
        when coalesce(a.total_beneficiarios_cadastrados, 0) > 0 
        then round((a.total_mulheres::numeric / a.total_beneficiarios_cadastrados) * 100, 2)
        else 0.00
    end as percentual_mulheres,
    
    -- Proporção de Benefícios Sociais
    coalesce(a.total_beneficiarios_bolsa_familia, 0) as total_beneficiarios_bolsa_familia,
    case 
        when coalesce(a.total_beneficiarios_cadastrados, 0) > 0 
        then round((a.total_beneficiarios_bolsa_familia::numeric / a.total_beneficiarios_cadastrados) * 100, 2)
        else 0.00
    end as percentual_bolsa_familia,
    
    coalesce(a.total_beneficiarios_bpc, 0) as total_beneficiarios_bpc,
    case 
        when coalesce(a.total_beneficiarios_cadastrados, 0) > 0 
        then round((a.total_beneficiarios_bpc::numeric / a.total_beneficiarios_cadastrados) * 100, 2)
        else 0.00
    end as percentual_bpc,

    -- Renda
    round(coalesce(a.renda_familiar_media, 0.00), 2) as renda_familiar_media

from fichas f
inner join agregado a on f.apf = a.apf
