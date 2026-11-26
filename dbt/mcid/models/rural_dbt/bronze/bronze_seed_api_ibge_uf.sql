{{ config(materialized="table") }}

-- Bronze: referência do IBGE com as UFs (sigla, nome, região), como está em seeds.api_ibge_uf
-- (seed `api_ibge_uf`). Sem transformação: a tabela de origem traz cada UF duplicada.
select *
from {{ ref("api_ibge_uf") }}
