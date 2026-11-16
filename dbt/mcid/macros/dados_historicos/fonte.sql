{#
    Leitura do cleansed dos dados históricos no MinIO.

    `fonte_dhist(familia)` registra a dependência na source `dhist_cleansed` (linhagem) e lê
    todos os Parquet da pasta da família, juntando os layouts pelo nome da coluna. A raiz vem
    da var `dhist_cleansed_prefixo` (hoje `tests/cleansed/dados_historicos/`): sair de
    `tests/` é mudar só a var. As versões substituídas ficam em `_versoes_substituidas/`,
    fora da pasta da família, e por isso não entram.
#}
{% macro dhist_uri(caminho) -%}
    's3://{{ var("lake_bucket", "data-lake-mcid") }}/{{ var("dhist_cleansed_prefixo") }}{{ caminho }}'
{%- endmacro %}

{% macro fonte_dhist(familia) -%}
    {%- set _ = source('dhist_cleansed', familia) -%}
    read_parquet({{ dhist_uri(familia ~ '/*.parquet') }}, union_by_name = true, filename = true)
{%- endmacro %}

{% macro fonte_dhist_log() -%}
    {%- set _ = source('dhist_cleansed', 'cleansed_log') -%}
    read_parquet({{ dhist_uri('_cleansed_log.parquet') }})
{%- endmacro %}
