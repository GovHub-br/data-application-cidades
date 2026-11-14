{{ config(materialized="table") }}

-- Bronze: espelho em texto do seed ts_marcos_portaria_464_2018 (régua de marcos da Portaria 464/2018).
-- A prata lê o seed por aqui (a prata só lê bronze). Macro bnz_seed_texto.

{{ bnz_seed_texto("ts_marcos_portaria_464_2018") }}
