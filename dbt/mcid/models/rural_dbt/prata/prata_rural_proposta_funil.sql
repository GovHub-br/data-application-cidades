{{ config(materialized="table") }}

-- Prata: funil de cada proposta selecionada do Rural — seleção → autorização de contratação → contrato.
-- Grão: linha do anexo da portaria de seleção (id_selecao). Lê só bronze (via macros).
--
-- O SQL está em macros/corpos/corpos_rural.sql (corpo_prata_rural_proposta_funil), porque a
-- prata_rural_vinculo_operacao usa o mesmo funil para dar a cada APF a sua portaria de seleção e de
-- autorização, e prata não lê prata.

{{ corpo_prata_rural_proposta_funil() }}
