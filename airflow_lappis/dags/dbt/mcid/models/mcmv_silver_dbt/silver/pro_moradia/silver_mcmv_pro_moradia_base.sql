{{ config(materialized="table") }}

{{ mcmv_silver_empty_contract(
    "Pro-Moradia",
    "Financiada",
    "Pro-Moradia",
    "Fonte Pro-Moradia ainda nao localizada no Postgres/sftp; manter contrato silver para receber a carga quando entrar no staging."
) }}
