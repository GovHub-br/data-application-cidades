{{ config(materialized="table") }}

-- Ficha sem identificadores pessoais: uma reforma contratada por linha.
-- O número contratual é identificador administrativo para seleção interna;
-- CPF, NIS, nome e endereço não são publicados nesta Gold.
select
    id_contrato,
    nu_contrato as identificador_reforma,
    dt_contratacao,
    dt_referencia,
    uf,
    municipio,
    codigo_ibge,
    linha_apf,
    modalidade,
    faixa_renda,
    tipo_imovel,
    classificacao_imovel,
    tipo_desembolso,
    tipo_garantia,
    situacao_garantia,
    sistema_amortizacao,
    indicador_cotista,
    legislacao,
    renda_familiar_comprovada,
    percentual_renda_informal,
    valor_avaliacao_terreno,
    valor_financiado,
    valor_desconto,
    valor_recurso_proprio,
    valor_fgts_utilizado,
    valor_garantia,
    valor_investimento,
    valor_prestacao_inicial,
    taxa_juros_nominal,
    prazo_financiamento_meses,
    dias_atraso,
    case
        when coalesce(dias_atraso, 0) > 0 then 'Com atraso registrado'
        else 'Sem atraso registrado'
    end as situacao_administrativa,
    false as tipo_fisico_reforma_disponivel,
    false as acompanhamento_obra_disponivel,
    'A base contratual não informa serviço executado, medição, vistoria ou conclusão da reforma.'
        as ressalva_reforma,
    source_file,
    current_timestamp as dt_ouro
from {{ ref('prata_reforma_casa_brasil_contrato') }}
