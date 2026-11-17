-- Toda família do cleansed tem silver (de_para_dhist) ou descarte com motivo
-- (descartes_dhist), nunca as duas coisas. Família sem destino aparece aqui.
{% call duckdb_query() %}
{%- set prata = familias_prata_dhist() %}
{%- set descartes = descartes_dhist().keys() | list %}
with
    familias(familia, tem_prata, descartada) as (
        values
            {%- for f in familias_dhist() %}
                ('{{ f }}', {{ f in prata }}, {{ f in descartes }}){{ ',' if not loop.last }}
            {%- endfor %}
    )
select
    familia,
    case when tem_prata and descartada then 'silver e descarte' else 'sem destino' end as problema
from familias
where tem_prata = descartada
{% endcall %}
