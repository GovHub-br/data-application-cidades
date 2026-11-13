{{ config(materialized="table") }}

-- Gold: publica a prata_pro_moradia_tomador_recontratacao como está (mesmo grão e mesmas colunas). A lógica mora na prata;
-- a ouro só lê prata (regra das camadas).

select * from {{ ref("prata_pro_moradia_tomador_recontratacao") }}
