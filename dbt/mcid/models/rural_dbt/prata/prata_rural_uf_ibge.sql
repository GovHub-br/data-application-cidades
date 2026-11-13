{{ config(materialized="table") }}

-- Prata: referência do IBGE por UF — sigla, nome do estado e região. Uma linha por UF.
-- Fonte: bronze_seed_api_ibge_uf. O distinct tira a duplicação da origem (54 linhas para 27
-- siglas), que dobraria as somas nos joins das ouros do Rural.

select distinct sigla, nome as estado_nome, regiao_sigla, regiao_nome
from {{ ref("bronze_seed_api_ibge_uf") }}
