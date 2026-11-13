{{ config(materialized="table") }}

-- Bronze: linhas dos anexos das portarias de seleção do Rural (exceto a 162/2018, que vem do
-- SFTP), como estão no seed perguntas_mcid/selecao_portarias_rural. Uma linha por proposta
-- selecionada. O seed começa vazio: as listas da 697/2026 e da 948/2026 ainda não chegaram.
-- Sem transformação; existe para a prata ler bronze, não o seed.

select * from {{ ref("selecao_portarias_rural") }}
