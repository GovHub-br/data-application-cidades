{% test date_year_between(model, column_name, min_year=1900, max_year=2100) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and extract(year from {{ column_name }}) not between {{ min_year }} and {{ max_year }}

{% endtest %}
