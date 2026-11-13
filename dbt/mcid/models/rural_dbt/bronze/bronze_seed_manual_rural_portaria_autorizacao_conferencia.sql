{{ config(materialized="table") }}

-- Bronze: conferência da extração dos PDFs das portarias de autorização do Rural (uma linha por portaria:
-- propostas e UH lidas). Cópia fiel de seeds.rural_portaria_autorizacao_conferencia. Usada no teste que
-- confere se a bronze das autorizações tem o mesmo número de linhas que a extração leu.

select * from {{ source("seeds_manuais", "rural_portaria_autorizacao_conferencia") }}
