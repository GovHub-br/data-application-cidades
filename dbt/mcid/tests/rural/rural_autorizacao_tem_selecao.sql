{#-
    Toda proposta autorizada a contratar tem de estar num anexo de seleção (o número da proposta é o mesmo nos
    dois anexos) e com as mesmas UH. Devolve a proposta autorizada que não aparece na seleção, ou que aparece
    com UH diferente. Em 03/11/2026 as 436 propostas das 4 autorizações carregadas batem com a 354/2024.
-#}
{{ config(severity="warn") }}

with
    aut as (select * from {{ ref("prata_rural_autorizacao_proposta") }}),
    sel as (select * from {{ ref("prata_rural_selecao_portarias") }})

select a.portaria_codigo, a.protocolo_proposta, a.quantidade_uh_autorizadas,
       s.portaria_codigo as portaria_selecao, s.quantidade_uh_selecionadas
from aut a
left join sel s on s.protocolo_proposta = a.protocolo_proposta
where s.protocolo_proposta is null
   or s.quantidade_uh_selecionadas is distinct from a.quantidade_uh_autorizadas
