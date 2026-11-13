{{ config(materialized="table") }}

-- Bronze: catálogo das portarias de seleção do Rural (número, ano, data, papel no ciclo e se
-- existe lista nominal de selecionados), como está no seed perguntas_mcid/portarias_selecao_rural
-- (curado no repositório). Sem transformação; existe para a prata ler bronze, não o seed.

select * from {{ ref("portarias_selecao_rural") }}
