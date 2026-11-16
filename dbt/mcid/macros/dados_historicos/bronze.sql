{#
    Bronze de uma família dos dados históricos: cópia fiel do cleansed, todas as
    competências empilhadas (os layouts se juntam pelo nome da coluna; coluna que não
    existe num layout vem nula). Só ganha as colunas técnicas:
    - `familia`;
    - `saida`: o arquivo do cleansed (liga ao `_cleansed_log`, que tem competência e corte);
    - `source_file`: o caminho no lake.
    As colunas de linhagem do cleansed continuam: `_linha_origem` (posição da linha na
    staging), `_source_file` (o CSV da raw) e `_source_hash`.
#}
{% macro bronze_dhist(familia) %}
    select
        * exclude (filename),
        '{{ familia }}' as familia,
        regexp_extract(filename, '([^/]+)\.parquet$', 1) as saida,
        filename as source_file
    from {{ fonte_dhist(familia) }}
{% endmacro %}
