{#-
    Curva S do Rural: o % físico (realizado e previsto) fica entre 0 e 100, e a carteira não encolhe de um mês
    para o seguinte (contrato assinado não sai da curva). O % financeiro pode passar de 100 só por aporte; acima
    de 110 é sinal de erro de chave ou de sinal das liberações. Devolve o mês/recorte fora da regra.
-#}
{{ config(severity="warn") }}

with
    nacional as (
        select mes, sum(qt_contratos) as qt_contratos
        from {{ ref("ouro_rural_curva_s") }}
        where tipo_portaria = 'Seleção'
        group by mes
    )

select 'percentual fora da faixa' as problema, mes, tipo_portaria, portaria, uf,
       pct_fisico_realizado, pct_fisico_previsto, pct_financeiro
from {{ ref("ouro_rural_curva_s") }}
where pct_fisico_realizado not between 0 and 100
   or pct_fisico_previsto not between 0 and 100
   or pct_financeiro > 110

union all

select 'carteira encolheu', n.mes, 'Seleção', null, null, null, null, null
from nacional n
join nacional a on a.mes = (n.mes - interval '1 month')::date
where n.qt_contratos < a.qt_contratos
