-- O grafo de nomes e a silver concordam: toda troca de nome que a task mapear_colunas prova
-- dentro de uma família (par `provavel`, com os mesmos valores dos dois lados em 90% ou
-- mais) cai na mesma coluna da silver. Devolve o par e as duas colunas da silver quando
-- caem em colunas diferentes. Par com valores abaixo de 90% é só indício e fica de fora
-- (ex.: contratacao_total e contratacao_total_pf medem coisas diferentes).
-- SQL do Postgres, fora do envelope.
with

    de_para as (
        select familia, coluna, left(coluna_cleansed, 63) as origem
        from {{ ref("prata_dhist_de_para") }}
        where coluna is not null
    ),

    provados as (
        select familia, coluna_antes, coluna_depois, competencia_troca, nota_valores
        from {{ ref("bronze_dhist_colunas_candidatos") }}
        where provavel and nota_valores >= 0.9
    )

select
    p.familia,
    p.coluna_antes,
    p.coluna_depois,
    p.competencia_troca,
    p.nota_valores,
    a.coluna as silver_antes,
    d.coluna as silver_depois
from provados as p
join de_para as a on a.familia = p.familia and a.origem = left(p.coluna_antes, 63)
join de_para as d on d.familia = p.familia and d.origem = left(p.coluna_depois, 63)
where a.coluna <> d.coluna
