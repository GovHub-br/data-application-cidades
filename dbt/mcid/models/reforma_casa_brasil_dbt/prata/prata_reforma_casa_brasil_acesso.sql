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
        coalesce(pc.codigo_trabalhou_semana, pn.codigo_trabalhou_semana)
            as codigo_trabalhou_semana,
        coalesce(pc.codigo_afastado_trabalho, pn.codigo_afastado_trabalho)
            as codigo_afastado_trabalho,
        coalesce(pc.codigo_trabalho_agricultura, pn.codigo_trabalho_agricultura)
            as codigo_trabalho_agricultura,
        coalesce(pc.codigo_ocupacao_principal, pn.codigo_ocupacao_principal)
            as codigo_ocupacao_principal,
        coalesce(pc.codigo_trabalho_doze_meses, pn.codigo_trabalho_doze_meses)
            as codigo_trabalho_doze_meses,
        coalesce(pc.codigo_carteira_assinada, pn.codigo_carteira_assinada)
            as codigo_carteira_assinada,
        coalesce(pc.faixa_renda_trabalho, pn.faixa_renda_trabalho)
            as faixa_renda_trabalho,
        coalesce(pc.indicador_renda_construcao, pn.indicador_renda_construcao)
            as indicador_renda_construcao,
        coalesce(pc.indicador_renda_flanelinha, pn.indicador_renda_flanelinha)
            as indicador_renda_flanelinha,
        coalesce(pc.indicador_renda_carregador, pn.indicador_renda_carregador)
            as indicador_renda_carregador,
        coalesce(pc.indicador_renda_catador, pn.indicador_renda_catador)
            as indicador_renda_catador,
        coalesce(pc.indicador_renda_servicos_gerais, pn.indicador_renda_servicos_gerais)
            as indicador_renda_servicos_gerais,
        coalesce(pc.indicador_renda_pede, pn.indicador_renda_pede)
            as indicador_renda_pede,
        coalesce(pc.indicador_renda_vendas, pn.indicador_renda_vendas)
            as indicador_renda_vendas,
        coalesce(pc.indicador_renda_outro, pn.indicador_renda_outro)
            as indicador_renda_outro,
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
    f.despesa_energia_familiar,
    f.despesa_agua_esgoto_familiar,
    f.despesa_gas_familiar,
    f.despesa_alimentacao_familiar,
    f.despesa_transporte_familiar,
    f.despesa_aluguel_familiar,
    f.despesa_medicamentos_familiar,
    f.despesas_basicas_declaradas,
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
