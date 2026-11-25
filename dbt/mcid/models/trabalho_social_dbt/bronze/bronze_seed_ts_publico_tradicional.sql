{{ config(materialized="table") }}

-- Bronze: espelho em texto do seed ts_publico_tradicional (palavras-chave do público tradicional).
-- A prata lê o seed por aqui (a prata só lê bronze). Macro bnz_seed_texto.

{{ bnz_seed_texto("ts_publico_tradicional") }}
