{{ config(materialized="table") }}

-- Bronze: códigos dos campos de situação e andamento da obra do Rural (layout oficial da CAIXA),
-- como estão no seed perguntas_mcid/dominio_rural_obra (curado no repositório). Sem transformação.

select * from {{ ref("dominio_rural_obra") }}
