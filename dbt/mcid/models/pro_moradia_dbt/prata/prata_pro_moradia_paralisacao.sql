{{ config(materialized="table") }}

-- O SQL deste modelo está em macros/corpos/corpos_pro_moradia.sql (corpo_prata_pro_moradia_paralisacao). Fica numa
-- macro porque outros modelos da mesma camada reaproveitam a mesma lógica a partir
-- da camada anterior, sem ler esta tabela (regra: prata só lê bronze; ouro só lê prata).

{{ corpo_prata_pro_moradia_paralisacao() }}
