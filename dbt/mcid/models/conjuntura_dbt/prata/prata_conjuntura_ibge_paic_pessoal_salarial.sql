{{ config(materialized='table') }}

-- Prata do conjuntura: PAIC pessoal e salários (IBGE/SIDRA).
--
-- O achatamento do JSON da API (variável → resultados → séries → períodos)
-- acontece uma vez, no leitor `ibge_agregados_json` da ingestão
-- (plugins/datalake/readers.py); a tipagem, igual para todo agregado, mora no
-- macro `prata_ibge_agregado`.
{{ prata_ibge_agregado('bronze_ibge_paic_pessoal_salarial') }}
