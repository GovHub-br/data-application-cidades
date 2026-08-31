{{ config(materialized='table', schema='conjuntura_continuo_mart') }}

-- Diferença entre o retrato de schema mais recente e o anterior: colunas que
-- APARECERAM, SUMIRAM ou MUDARAM DE TIPO.
--
-- Item 5 do checklist. Enquanto só existir um retrato em
-- `gold_qualidade_schema`, este model devolve vazio — é o esperado, não é
-- erro. A partir do segundo dia de execução ele passa a ter conteúdo.

with retratos as (
    select distinct visto_em from {{ ref('gold_qualidade_schema') }}
),

marcos as (
    select
        max(visto_em) as atual,
        (select max(visto_em) from retratos
          where visto_em < (select max(visto_em) from retratos)) as anterior
    from retratos
),

agora as (
    select s.* from {{ ref('gold_qualidade_schema') }} s, marcos m
    where s.visto_em = m.atual
),

antes as (
    select s.* from {{ ref('gold_qualidade_schema') }} s, marcos m
    where s.visto_em = m.anterior
)

select
    coalesce(agora.model, antes.model)   as model,
    coalesce(agora.coluna, antes.coluna) as coluna,
    case
        when antes.coluna is null then 'coluna nova'
        when agora.coluna is null then 'coluna sumiu'
        else 'tipo mudou'
    end                                   as mudanca,
    antes.tipo                            as tipo_antes,
    agora.tipo                            as tipo_agora,
    (select anterior from marcos)         as retrato_anterior,
    (select atual from marcos)            as retrato_atual
from agora
full outer join antes
  on antes.model = agora.model and antes.coluna = agora.coluna
-- Sem retrato anterior não há o que comparar: sem esta guarda o full outer
-- join marcaria TODAS as colunas como "coluna nova" no primeiro dia, o que é
-- ruído e não achado.
where (select anterior from marcos) is not null
  and (
        antes.coluna is null
     or agora.coluna is null
     or antes.tipo is distinct from agora.tipo
  )
order by 1, 2
