{#-
  corpo_prata_catalogo_portarias(nome_bronze): tipa um catálogo de portarias/atos de linha (Sub-50,
  Pró-Moradia) no mesmo formato do catálogo do Rural (prata_rural_portarias_selecao), com o tipo usado no
  filtro de portaria: Abertura do ciclo, Seleção (resultado, enquadramento, alteração do ciclo),
  Autorização de contratação ou Regulamento da linha.
-#}
{% macro corpo_prata_catalogo_portarias(nome_bronze) %}
select
    nullif(trim(portaria_codigo::text), '') as portaria_codigo,
    nullif(trim(ato::text), '') as ato,
    nullif(trim(especie_ato::text), '') as especie_ato,
    {{ parse_int('numero::text') }} as numero_ato,
    {{ parse_int('ano::text') }} as ano_ato,
    nullif(trim(dt_portaria::text), '')::date as dt_ato,
    case
        when nullif(trim(numero::text), '') is not null and nullif(trim(ano::text), '') is not null
            then regexp_replace(trim(numero::text), '(\d)(?=(\d{3})+$)', '\1.', 'g') || '/' || trim(ano::text)
    end as numero_ano,
    nullif(trim(papel::text), '') as papel_ato,
    case nullif(trim(papel::text), '')
        when 'Abertura do ciclo' then 'Abertura do ciclo'
        when 'Resultado da seleção' then 'Seleção'
        when 'Enquadramento (Calamidade)' then 'Seleção'
        when 'Alteração' then 'Alteração'
        when 'Autorização de contratação' then 'Autorização de contratação'
        when 'Regulamento da linha' then 'Regulamento da linha'
        else 'A confirmar'
    end as tipo_portaria,
    nullif(trim(ciclo_selecao::text), '') as ciclo_selecao,
    nullif(trim(tem_lista_nominal::text), '') as tem_lista_nominal,
    nullif(trim(fonte_lista::text), '') as fonte_lista,
    nullif(trim(observacao::text), '') as observacao
from {{ ref(nome_bronze) }}
where nullif(trim(portaria_codigo::text), '') is not null
{% endmacro %}
