{{ config(materialized="table") }}

-- Gold: publica a prata_rural_evolucao_financeira como está (mesmo grão e mesmas colunas). A lógica mora na prata;
-- a ouro só lê prata (regra das camadas).

select * from {{ ref("prata_rural_evolucao_financeira") }}
