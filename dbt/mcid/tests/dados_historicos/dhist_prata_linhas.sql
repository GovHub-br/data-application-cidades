-- Validação 2 (silver): a silver tem exatamente as linhas da bronze, saída por saída
-- (os joins de contexto, IBGE e situação não podem multiplicar nem perder linha).
{% for f in familias_prata_dhist() %}
    select
        '{{ f }}' as familia,
        coalesce(b.saida, p.saida) as saida,
        b.linhas as linhas_bronze,
        p.linhas as linhas_prata
    from (select saida, count(*) as linhas from {{ ref('bronze_dhist_' ~ f) }} group by saida) as b
    full outer join
        (select saida, count(*) as linhas from {{ ref('prata_dhist_' ~ f) }} group by saida) as p
        on b.saida = p.saida
    where b.linhas is distinct from p.linhas {{ 'union all' if not loop.last }}
{% endfor %}
