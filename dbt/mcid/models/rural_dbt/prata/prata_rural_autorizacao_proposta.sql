{{ config(materialized="table") }}

-- Prata: propostas autorizadas a contratar pelas portarias de autorização do Rural (Seleção 2023).
-- Grão: portaria de autorização × proposta (id_autorizacao). Lê só bronze.
--
-- O SQL está em macros/corpos/corpos_rural.sql (corpo_prata_rural_autorizacao_proposta), porque a
-- prata_rural_proposta_funil usa a mesma lógica e prata não lê prata.

{{ corpo_prata_rural_autorizacao_proposta() }}
