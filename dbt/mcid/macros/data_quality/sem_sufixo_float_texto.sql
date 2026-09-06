{#-
  Teste genérico: falha se uma coluna de CÓDIGO carrega o sufixo ".0" de um
  float que virou texto.

  A montante, um identificador inteiro passou por `int -> float -> str`:
  "3550308" -> 3550308.0 -> "3550308.0". O bronze é cópia fiel, então o resíduo
  chega intacto. Num join com uma base que traz o mesmo código sem sufixo
  ("3550308"), nada casa — e ninguém é avisado. Daí o teste.

  Este teste NÃO corrige nada: é diagnóstico de bronze. A limpeza é da silver,
  via a macro `strip_float_text` (macros/historico/strip_float_text.sql).

  Só se aplica a colunas que devem seguir TEXTO (código IBGE, código de
  empreendimento, CNPJ). Valores numéricos de verdade não entram aqui — os
  parsers de macros/parse_hist_numeric.sql já absorvem "2113.0" como número.

  Padrão detectado (string inteira): inteiro opcional­mente negativo, ponto,
  um ou mais zeros e nada mais — `-?[0-9]+[.]0+`.

  Uso no schema.yml:

      columns:
        - name: codigo_do_ibge
          data_tests:
            - sem_sufixo_float_texto
-#}
{% macro test_sem_sufixo_float_texto(model, column_name) %}

select
    {{ column_name }} as valor,
    count(*) as ocorrencias
from {{ model }}
where trim(cast({{ column_name }} as varchar)) similar to '-?[0-9]+[.]0+'
group by {{ column_name }}
order by ocorrencias desc

{% endmacro %}
