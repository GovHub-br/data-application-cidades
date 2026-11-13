{{ config(materialized="table") }}

-- Bronze: Anexo II da Portaria MCID 697/2026 (resultado da Seleção 2025 do Rural), compilada.
-- Cópia fiel da tabela carregada no schema seeds do Postgres
-- (fonte seeds_manuais.rural_portaria_selecao_697_2026). Sem transformação.

select * from {{ source("seeds_manuais", "rural_portaria_selecao_697_2026") }}
