{{ config(materialized="table") }}

-- Prata: catálogo tipado das portarias e atos do MCMV FNHIS Sub-50 (regulamento, abertura, resultado e
-- alterações de cada ciclo, inclusive a Calamidade RS 2024). Grão: ato. Lê só bronze.
-- SQL em macros/corpos/corpos_portarias.sql (mesmo corpo do catálogo do Pró-Moradia).

{{ corpo_prata_catalogo_portarias("bronze_seed_portarias_fnhis_sub50") }}
