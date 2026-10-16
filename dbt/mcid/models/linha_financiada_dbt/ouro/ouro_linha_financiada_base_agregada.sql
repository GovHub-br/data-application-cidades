{{ config(materialized='table') }}

-- Base simples e uniforme para substituir a consolidação manual em Excel.
-- Não contém CPF, nome, endereço ou outro identificador pessoal direto.

select
    c.*,
    extract(year from c.data_contratacao)::integer as ano_contratacao,
    extract(month from c.data_contratacao)::integer as mes_contratacao,
    date_trunc('month', c.data_contratacao)::date as competencia_contratacao,
    coalesce(c.valor_financiamento, 0)
      + coalesce(c.valor_desconto_fgts, 0)
      + coalesce(c.valor_desconto_ogu, 0)
      + coalesce(c.valor_contrapartida_informada, 0) as valor_total_recursos_identificados,
    c.valor_contrapartida_informada is not null
      and c.valor_contrapartida_informada <> 0 as ic_contrapartida_identificada,
    case
        when c.valor_contrapartida_informada is not null
         and c.valor_contrapartida_informada <> 0 then 'Informação parcial disponível'
        else 'Sem informação na cobertura atual'
    end as cobertura_contrapartida
from {{ ref('prata_linha_financiada_contrato') }} c
