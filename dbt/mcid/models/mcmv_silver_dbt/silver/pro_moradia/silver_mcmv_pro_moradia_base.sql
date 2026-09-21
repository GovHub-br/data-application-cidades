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

{{ mcmv_silver_empty_contract(
    "Pro-Moradia",
    "Financiada",
    "Pro-Moradia",
    "Fonte Pro-Moradia ainda nao localizada no Postgres/sftp; manter contrato silver para receber a carga quando entrar no staging."
) }}
#}
