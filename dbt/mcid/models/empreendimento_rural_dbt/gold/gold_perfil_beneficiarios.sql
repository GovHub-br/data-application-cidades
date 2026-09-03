{{ config(materialized="table") }}

-- Gold: Perfil dos Beneficiários (Rural)
-- Consolida os dados socioeconômicos dos beneficiários de cada empreendimento a partir da base de pessoas físicas.

with
    cadastro_pf as (
        select * from {{ ref("silver_cadastro_pf") }}
    ),

    agregado as (
        select
            apf,
            count(nu_registro) as total_beneficiarios_cadastrados,
            -- avg() já ignora nulo. Com o coalesce, beneficiário sem tamanho de família
            -- informado entrava na média como "família de 0 pessoas" e puxava tudo para baixo.
            avg(qt_pessoas_familia) as media_pessoas_familia,

            -- Gênero
            sum(case when upper(trim(beneficiario_sexo)) = 'F' then 1 else 0 end) as total_mulheres,
            sum(case when upper(trim(beneficiario_sexo)) = 'M' then 1 else 0 end) as total_homens,

            -- Benefícios Sociais
            sum(case when ic_benef_bolsa_familia then 1 else 0 end) as total_beneficiarios_bolsa_familia,
            sum(case when ic_benef_bpc then 1 else 0 end) as total_beneficiarios_bpc,

            -- Renda Familiar
            -- Mesmo caso, e mais grave: renda não informada entrava como R$ 0,00 e
            -- rebaixava a renda média do empreendimento.
            avg(vr_renda_familiar) as renda_familiar_media

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
        from {{ ref("gold_ficha_empreendimento") }}
    )

select
    f.apf,
    f.nome_empreendimento,
    f.municipio,
    f.uf,
    f.programa,
    f.quantidade_uh,

    -- Métricas de Cadastro e Demografia. O cadastro PF cobre ~1% da carteira; nos outros
    -- 99% a ausência fica NULL, não 0, senão qualquer média do Superset é destruída.
    a.total_beneficiarios_cadastrados,
    round(a.media_pessoas_familia, 1) as media_pessoas_familia,

    -- Proporção de Gênero
    a.total_mulheres,
    case
        when coalesce(a.total_beneficiarios_cadastrados, 0) > 0
        then round((a.total_mulheres::numeric / a.total_beneficiarios_cadastrados) * 100, 2)
    end as percentual_mulheres,

    -- Proporção de Benefícios Sociais
    a.total_beneficiarios_bolsa_familia,
    case
        when coalesce(a.total_beneficiarios_cadastrados, 0) > 0
        then round((a.total_beneficiarios_bolsa_familia::numeric / a.total_beneficiarios_cadastrados) * 100, 2)
    end as percentual_bolsa_familia,

    a.total_beneficiarios_bpc,
    case
        when coalesce(a.total_beneficiarios_cadastrados, 0) > 0
        then round((a.total_beneficiarios_bpc::numeric / a.total_beneficiarios_cadastrados) * 100, 2)
    end as percentual_bpc,

    -- Renda
    round(a.renda_familiar_media, 2) as renda_familiar_media,

    -- Diz explicitamente se este empreendimento tem cadastro PF, para o dashboard poder
    -- filtrar em vez de diluir a carteira inteira numa média de nulos.
    (a.apf is not null) as tem_cadastro_pf

from fichas f
inner join agregado a on f.apf = a.apf
