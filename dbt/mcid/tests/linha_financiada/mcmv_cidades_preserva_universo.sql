-- Todo contrato único recebido do MCMV Cidades deve aparecer na Prata,
-- mesmo quando ainda não existe correspondência em CCI/CCA ou Fundo Social.
with origem as (
    select distinct trim(numero_do_contrato::text) as contrato
    from {{ ref('bronze_sharepoint_novo_mcmv_cidades_emendas') }}
    where nullif(trim(numero_do_contrato::text), '') is not null
),
destino as (
    select distinct contrato
    from {{ ref('prata_linha_financiada_contrato') }}
    where ic_mcmv_cidades
)
select o.contrato
from origem o
left join destino d using (contrato)
where d.contrato is null
