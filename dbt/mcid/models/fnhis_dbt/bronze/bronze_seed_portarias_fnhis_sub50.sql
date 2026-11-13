{{ config(materialized="table") }}

-- Bronze: catálogo curado das portarias e demais atos (instruções normativas, resoluções) da linha, como está
-- no seed perguntas_mcid/portarias_fnhis_sub50 (curado no repositório a partir da página de regulamentação do MCID
-- e do fluxo da oficina). Sem transformação; existe para a prata ler bronze, não o seed.

select * from {{ ref("portarias_fnhis_sub50") }}
