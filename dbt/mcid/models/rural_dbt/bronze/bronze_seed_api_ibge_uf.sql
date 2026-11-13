{{ config(materialized="table") }}

-- Bronze: referência do IBGE com as UFs (sigla, nome, região), como está em seeds.api_ibge_uf
-- (source raw.api_ibge_uf). Sem transformação: a tabela de origem traz cada UF duplicada.

select * from {{ source("raw", "api_ibge_uf") }}
