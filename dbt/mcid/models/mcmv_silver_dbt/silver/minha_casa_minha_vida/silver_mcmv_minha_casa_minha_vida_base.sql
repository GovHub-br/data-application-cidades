{{ config(enabled=false) }}
{# ============================================================================
   DESATIVADO — domínio mcmv_silver_dbt (legado).
   Colisão de `alias="silver_historico_base"` entre as 10 frentes: com o bloco
   `mcmv_silver_dbt` do dbt_project.yml comentado, todas resolvem para o schema
   default e colidem no parse ("two resources with identical database
   representation"). Código original preservado abaixo, inerte.
   Reativar: remova o `config(enabled=false)` acima, descomente o bloco abaixo
   e restaure o bloco `mcmv_silver_dbt` no dbt_project.yml.
   ============================================================================ #}
{#
{{ config(materialized="table", alias="silver_historico_base") }}

-- Uniao direta das frentes (exceto Conjuntura, que nao e uma frente de
-- empreendimento). Antes lia do helper cross-frente silver_mcmv_frentes_base,
-- aposentado na convencao 2026-09-04 (cada frente materializa em schema
-- proprio; nao ha mais um schema unico onde um union all resolveria sozinho).
select *
from {{ ref("silver_mcmv_far_base") }}
union all
select *
from {{ ref("silver_mcmv_entidades_base") }}
union all
select *
from {{ ref("silver_mcmv_rural_base") }}
union all
select *
from {{ ref("silver_mcmv_classe_media_base") }}
union all
select *
from {{ ref("silver_mcmv_cidades_base") }}
union all
select *
from {{ ref("silver_mcmv_reforma_base") }}
union all
select *
from {{ ref("silver_mcmv_pro_moradia_base") }}
union all
select *
from {{ ref("silver_mcmv_sub50_base") }}
#}
