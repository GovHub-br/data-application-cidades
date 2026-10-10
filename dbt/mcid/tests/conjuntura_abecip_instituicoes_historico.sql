-- A abertura por instituição da ABECIP é série, não um mês solto.
--
-- Veio da task `conferir` da ingestão antiga. Uma competência que some do raw do
-- outro time, ou um arquivo que chega quase vazio, faria a série encolher sem
-- erro nenhum, e o ouro passaria a mostrar uma série mais curta como se fosse o
-- mundo. Falha com menos de 2 competências ou menos de 10 linhas por competência
-- (o relatório traz dezenas por mês).

with resumo as (
    select
        count(distinct competencia) as competencias,
        count(*)                    as linhas
    from {{ ref('prata_conjuntura_abecip_instituicoes') }}
)

select competencias, linhas
from resumo
where competencias < 2
   or linhas < competencias * 10
