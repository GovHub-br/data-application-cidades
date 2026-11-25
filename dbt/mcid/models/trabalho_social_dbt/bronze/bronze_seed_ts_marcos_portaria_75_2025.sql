{{ config(materialized="table") }}

-- Bronze: espelho em texto do seed ts_marcos_portaria_75_2025 (régua de marcos da Portaria 75/2025).
-- A prata lê o seed por aqui (a prata só lê bronze). Macro bnz_seed_texto.

{{ bnz_seed_texto("ts_marcos_portaria_75_2025") }}
