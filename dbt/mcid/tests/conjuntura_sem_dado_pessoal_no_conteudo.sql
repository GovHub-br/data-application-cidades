-- Varre o CONTEÚDO das colunas de texto de silver/gold procurando dado
-- pessoal — CPF (com dígito verificador conferido), CNPJ, e-mail.
--
-- Complementa `conjuntura_sem_dado_sensivel`, que olha só o NOME da coluna.
-- Nome não basta: um CPF pode estar num campo `observacao`, ou a coluna pode
-- ter sido renomeada. Juntos, os dois cobrem a exigência de que nada
-- pessoal chegue à camada de consumo nem à documentação — **sem depender de
-- a anonimização a montante ter funcionado**.
--
-- Só varre silver e gold: a bronze espelha a origem e pode legitimamente
-- conter esses dados. Os volumes aqui são pequenos (~12 mil linhas no
-- total), então a varredura completa é barata.

{% set alvos = [] %}
{% if execute %}
    {% set colunas = run_query(
        "select table_schema, table_name, column_name
         from information_schema.columns
         where table_schema in ('conjuntura_silver','conjuntura_mart')
           and data_type in ('text','character varying','character')
         order by 1,2,3") %}
    {% for linha in colunas.rows %}
        {% do alvos.append(
            "select '" ~ linha[1] ~ "'::text as model, '" ~ linha[2] ~ "'::text as coluna, "
            ~ "count(*)::bigint as ocorrencias from " ~ linha[0] ~ '."' ~ linha[1] ~ '" '
            ~ "where " ~ parece_dado_pessoal('"' ~ linha[2] ~ '"')
        ) %}
    {% endfor %}
{% endif %}

with varredura as (
    {% if alvos %}
        {{ alvos | join('\n    union all\n    ') }}
    {% else %}
        select null::text as model, null::text as coluna, null::bigint as ocorrencias where false
    {% endif %}
)

select model, coluna, ocorrencias
from varredura
where ocorrencias > 0
