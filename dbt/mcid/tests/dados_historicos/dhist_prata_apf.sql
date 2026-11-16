-- Validação 2 (silver): o APF, chave da cadeia da Caixa, é numérico de 8 dígitos (2 APFs
-- do relatório executivo têm 7 em todas as épocas; nenhum APF de 8 começa com 0, então não
-- é zero perdido) e só falta no layout do relatório executivo anterior a 2012-07, que não
-- tinha a coluna.
{% for f in familias_prata_dhist() %}
    select '{{ f }}' as familia, saida, competencia, apf, count(*) as n
    from {{ ref('prata_dhist_' ~ f) }}
    where
        (apf is not null and not regexp_matches(apf, '^\d{7,8}$'))
        or (
            apf is null
            {%- if f == 'relatorio_executivo_caixa' %}
                and competencia >= date '2012-07-01'
            {% endif %}
        )
    group by all {{ 'union all' if not loop.last }}
{% endfor %}
