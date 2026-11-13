{{ config(materialized="table") }}

-- Prata: catálogo tipado dos atos do Pró-Moradia (resolução do Conselho Curador do FGTS, instruções normativas
-- do MCID e a seleção por carta-consulta, que não tem portaria de resultado). Grão: ato. Lê só bronze.
-- SQL em macros/corpos/corpos_portarias.sql (mesmo corpo do catálogo do Sub-50).

{{ corpo_prata_catalogo_portarias("bronze_seed_portarias_pro_moradia") }}
