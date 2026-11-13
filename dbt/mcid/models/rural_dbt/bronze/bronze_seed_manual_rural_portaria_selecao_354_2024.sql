{{ config(materialized="table") }}

-- Bronze: Anexo da Portaria MCID 354/2024 (resultado da Seleção 2023 do Rural), compilada de 18/11/2025.
-- Cópia fiel da tabela carregada no schema seeds do Postgres
-- (fonte seeds_manuais.rural_portaria_selecao_354_2024). Sem transformação.

select * from {{ source("seeds_manuais", "rural_portaria_selecao_354_2024") }}
