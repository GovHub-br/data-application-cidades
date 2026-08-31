-- Detecta colapso de volume nos parquets de `staging/`.
--
-- Item 2.13. Este é o teste que faltava contra o problema estrutural do
-- projeto: os arquivos em `staging/` são escritos por DAGs deste repositório
-- E por um job de outro time, com contratos de formato diferentes. Quando o
-- formato muda, a bronze continua construindo (é `select *`) — só que com 1
-- linha aninhada no lugar de 500 achatadas. O `dbt run` passa, e o problema
-- só aparece lá na frente.
--
-- O piso de cada fonte está em `sources.yml` (`meta.linhas_minimas`),
-- calibrado em metade do volume observado em 2026-08-29. Metade dá folga
-- para variação real de série sem deixar passar um colapso de ordem de
-- grandeza.
--
-- Complementa `gold_qualidade_schema_drift`: o drift avisa no dia seguinte,
-- comparando retratos; este falha na hora, dentro do build.

{% set linhas = [] %}
{% if execute %}
    {% for no in graph.sources.values() if no.source_name == 'lake_staging' %}
        {% set piso = no.meta.get('linhas_minimas') %}
        {% if piso %}
            {% do linhas.append(
                "select '" ~ no.name ~ "'::text as fonte, count(*)::bigint as linhas, "
                ~ piso ~ "::bigint as minimo from conjuntura_continuo_bronze.bronze_continuo_" ~ no.name
            ) %}
        {% endif %}
    {% endfor %}
{% endif %}

with medido as (
    {% if linhas %}
        {{ linhas | join('\n    union all\n    ') }}
    {% else %}
        select null::text as fonte, null::bigint as linhas, null::bigint as minimo where false
    {% endif %}
)

select fonte, linhas, minimo
from medido
where linhas < minimo
