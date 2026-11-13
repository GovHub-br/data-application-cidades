{{ config(materialized="table") }}

-- Bronze: matriz pergunta × dado das perguntas da oficina com o MCID, como está no seed
-- perguntas_mcid/cobertura_perguntas_mcid (curado no repositório). Sem transformação.

select * from {{ ref("cobertura_perguntas_mcid") }}
