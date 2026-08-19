{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: PIB da construção civil em % de crescimento.
-- Página 1 do boletim (trim/trim imediatamente anterior, acumulada no ano,
-- acumulada 4 trimestres). Dado MANUAL (schema manual_conjuntura), pois o
-- boletim usa a série dessazonalizada tabulada — não derivável direto da API.

select *
from manual_conjuntura.ibge_pib_construcao_civil
