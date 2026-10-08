{{ config(materialized="table") }}

with pessoas_cpf as (
    select *, row_number() over (partition by cpf_hmac order by id_pessoa) as rn
    from {{ ref('prata_reforma_casa_brasil_pessoa') }}
    where cpf_hmac is not null
),
pessoas_nis as (
    select *, row_number() over (partition by nis_hmac order by id_pessoa) as rn
    from {{ ref('prata_reforma_casa_brasil_pessoa') }}
    where nis_hmac is not null
),
integrada as (
    select
        c.*,
        coalesce(pc.id_pessoa, pn.id_pessoa) as id_pessoa_cadunico,
        coalesce(pc.codigo_familiar, pn.codigo_familiar) as codigo_familiar,
        coalesce(pc.codigo_sexo, pn.codigo_sexo) as codigo_sexo_cadunico,
        coalesce(pc.codigo_raca_cor, pn.codigo_raca_cor) as codigo_raca_cor,
        coalesce(pc.codigo_deficiencia, pn.codigo_deficiencia) as codigo_deficiencia,
        coalesce(pc.codigo_parentesco_responsavel, pn.codigo_parentesco_responsavel)
            as codigo_parentesco_responsavel,
        case
            when pc.id_pessoa is not null then 'cpf_hmac'
            when pn.id_pessoa is not null then 'nis_hmac'
            else 'sem_correspondencia'
        end as metodo_vinculo_cadunico
    from {{ ref('prata_reforma_casa_brasil_contrato') }} c
    left join pessoas_cpf pc on c.cpf_hmac = pc.cpf_hmac and pc.rn = 1
    left join pessoas_nis pn
        on pc.id_pessoa is null and c.nis_hmac = pn.nis_hmac and pn.rn = 1
)

select
    i.*,
    (i.id_pessoa_cadunico is not null) as indicador_encontrado_cadunico,
    f.codigo_ibge_cadastro,
    f.situacao_cadastral_familia,
    f.marcador_pbf,
    f.renda_media_familiar,
    f.renda_total_familiar,
    f.quantidade_membros,
    f.quantidade_pessoas_domicilio,
    f.quantidade_comodos,
    f.quantidade_dormitorios,
    f.pessoas_por_dormitorio,
    f.codigo_local_domicilio,
    f.codigo_especie_domicilio,
    f.codigo_material_piso,
    f.codigo_material_domicilio,
    f.codigo_agua_canalizada,
    f.codigo_abastecimento_agua,
    f.codigo_banheiro,
    f.codigo_escoamento_sanitario,
    f.codigo_destino_lixo,
    f.codigo_iluminacao,
    f.codigo_calcamento,
    f.indicador_familia_indigena,
    f.indicador_familia_quilombola,
    f.indicador_inadequacao_observavel,
    current_timestamp as dt_integracao
from integrada i
left join {{ ref('prata_reforma_casa_brasil_familia') }} f
    on i.codigo_familiar = f.codigo_familiar
