{{ config(materialized="table") }}

-- Prata: Regularidade dos entes no SNHIS, série semanal. Grão: ente × data de referência.
-- O SQL deste modelo está em macros/corpos/corpos_fnhis.sql (corpo_prata_fnhis_regularidade_serie)
{{ corpo_prata_fnhis_regularidade_serie() }}
