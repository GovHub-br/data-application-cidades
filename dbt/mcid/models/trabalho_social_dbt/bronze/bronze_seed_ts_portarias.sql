{{ config(materialized="table") }}

-- Bronze: espelho em texto do seed ts_portarias (catálogo de vigência das portarias de TS).
-- A prata lê o seed por aqui (a prata só lê bronze). Macro bnz_seed_texto.

{{ bnz_seed_texto("ts_portarias") }}
