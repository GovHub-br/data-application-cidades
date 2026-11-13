{{ config(materialized="table") }}

-- Bronze: anexos vigentes da Calamidade RS (173/2024, 840/2025 e anexo próprio da 1.207/2025), na redação dada
-- pela Portaria MCID 1.207/2025, com a APF de cada proposta. Cópia fiel de seeds.rural_portaria_enquadramento_1207_2025.

select * from {{ source("seeds_manuais", "rural_portaria_enquadramento_1207_2025") }}
