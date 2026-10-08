{{ config(materialized='table') }}

-- A área de negócio confirmou que ainda não existe uma base institucional
-- completa de contrapartidas. Este modelo publica somente valores explícitos
-- nas fontes atuais e torna a cobertura/lacuna auditável.

select
    id_contrato_linha_financiada,
    contrato,
    segmento_linha_financiada,
    fonte_recurso,
    case
        when ic_mcmv_cidades then 'Aporte MCMV Cidades'
        else 'Contrapartida de parceria informada pelo agente financeiro'
    end as tipo_contrapartida,
    valor_contrapartida_informada as valor_contrapartida,
    fonte_classificacao as fonte_informacao,
    true as ic_valor_informado,
    'Cobertura parcial: não existe base completa de contrapartidas.'::text as ressalva_cobertura,
    dt_silver
from {{ ref('prata_linha_financiada_contrato') }}
where valor_contrapartida_informada is not null
  and valor_contrapartida_informada <> 0

union all

select
    id_contrato_linha_financiada,
    contrato,
    segmento_linha_financiada,
    fonte_recurso,
    'Não informado'::text,
    null::numeric,
    fonte_classificacao,
    false,
    'Contrato sem contrapartida na cobertura atual; requer base do agente financeiro.'::text,
    dt_silver
from {{ ref('prata_linha_financiada_contrato') }}
where valor_contrapartida_informada is null
   or valor_contrapartida_informada = 0
