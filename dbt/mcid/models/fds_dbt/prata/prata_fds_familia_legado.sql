{{ config(materialized="table") }}

-- Prata: Famílias do MCMV Entidades (FDS) no legado. Grão: contrato.
-- O SQL deste modelo está em macros/corpos/corpos_familias.sql (corpo_prata_fds_familia_legado)
{{ corpo_prata_fds_familia_legado() }}
