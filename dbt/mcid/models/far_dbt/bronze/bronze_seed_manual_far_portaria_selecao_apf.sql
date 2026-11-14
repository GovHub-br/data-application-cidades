{{ config(materialized="table") }}

-- Bronze: Portaria de seleção de cada APF do FAR.
-- Cópia fiel da tabela carregada à mão no schema seeds do Postgres (fonte seeds_manuais.far_portaria_selecao_apf).

select * from {{ source("seeds_manuais", "far_portaria_selecao_apf") }}
