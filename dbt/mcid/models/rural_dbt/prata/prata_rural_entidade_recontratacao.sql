{{ config(materialized="table") }}

-- O SQL deste modelo está em macros/corpos/corpos_rural.sql (corpo_prata_rural_entidade_recontratacao). Fica numa
-- macro porque outros modelos da mesma camada reaproveitam a mesma lógica a partir
-- da camada anterior, sem ler esta tabela (regra: prata só lê bronze; ouro só lê prata).

{{ corpo_prata_rural_entidade_recontratacao() }}
