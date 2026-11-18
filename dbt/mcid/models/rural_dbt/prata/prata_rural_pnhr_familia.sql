{{ config(materialized="table") }}

-- Prata: Famílias beneficiárias do PNHR (BB e CAIXA). Grão: agente financeiro × contrato individual.
-- O SQL deste modelo está em macros/corpos/corpos_familias.sql (corpo_prata_rural_pnhr_familia)

{{ corpo_prata_rural_pnhr_familia() }}
