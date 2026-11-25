{{ config(materialized="table") }}

-- Bronze: Anexo da Portaria MCID 698/2026 (seleção do MCMV-Entidades).
-- Cópia fiel da tabela carregada à mão no schema seeds do Postgres (fonte seeds_manuais.fds_portaria_selecao_698_2026).

select * from {{ source("seeds_manuais", "fds_portaria_selecao_698_2026") }}
