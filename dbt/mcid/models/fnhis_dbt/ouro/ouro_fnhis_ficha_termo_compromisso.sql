{{ config(materialized="table") }}

-- Gold: publica a prata_fnhis_ficha_termo_compromisso como está (mesmo grão e mesmas colunas). A lógica mora na prata;
-- a ouro só lê prata (regra das camadas).

select * from {{ ref("prata_fnhis_ficha_termo_compromisso") }}
