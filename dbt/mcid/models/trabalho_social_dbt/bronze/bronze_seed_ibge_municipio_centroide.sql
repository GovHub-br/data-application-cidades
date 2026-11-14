{{ config(materialized="table") }}

-- Bronze: espelho em texto do seed ibge_municipio_centroide (centro dos municípios (IBGE)).
-- A prata lê o seed por aqui (a prata só lê bronze). Macro bnz_seed_texto.

{{ bnz_seed_texto("ibge_municipio_centroide") }}
