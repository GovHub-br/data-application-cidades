{{ config(materialized="table") }}

-- Bronze: espelho em texto do seed ts_dominio_situacao (domínio de situação do TS (CAIXA × BB)).
-- A prata lê o seed por aqui (a prata só lê bronze). Macro bnz_seed_texto.

{{ bnz_seed_texto("ts_dominio_situacao") }}
