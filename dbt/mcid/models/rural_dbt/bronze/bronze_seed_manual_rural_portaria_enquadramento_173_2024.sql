{{ config(materialized="table") }}

-- Bronze: anexo da Portaria MCID 173/2024 (compilada) — propostas do MCMV Rural enquadradas no rito de
-- calamidade da Portaria 1.417/2023 (RS). Cópia fiel de seeds.rural_portaria_enquadramento_173_2024
-- (fonte seeds_manuais). Sem transformação.

select * from {{ source("seeds_manuais", "rural_portaria_enquadramento_173_2024") }}
