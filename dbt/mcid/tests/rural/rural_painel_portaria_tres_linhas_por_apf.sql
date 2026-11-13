{#-
    A ouro_rural_painel_portaria desdobra cada APF em exatamente três linhas (Seleção, Autorização de
    contratação, Trabalho Social), e o universo de APFs tem de ser o mesmo da ouro_rural_painel_operacao —
    senão os totais do painel mudam conforme o tipo de portaria escolhido. Devolve a APF fora da regra.
-#}
{{ config(severity="error") }}

with
    pp as (
        select apf, count(*) as linhas, count(distinct tipo_portaria) as tipos
        from {{ ref("ouro_rural_painel_portaria") }}
        group by apf
    ),
    op as (select apf from {{ ref("ouro_rural_painel_operacao") }})

select coalesce(pp.apf, op.apf) as apf, pp.linhas, pp.tipos
from pp
full join op on op.apf = pp.apf
where pp.apf is null or op.apf is null or pp.linhas <> 3 or pp.tipos <> 3
