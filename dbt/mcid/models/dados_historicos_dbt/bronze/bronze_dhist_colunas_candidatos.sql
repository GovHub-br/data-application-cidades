-- Grafo de nomes de coluna (task mapear_colunas da dados_historicos_dag): os pares de colunas que
-- trocam de nome dentro de uma família, com as notas de
-- valores, nome e formato e a marca `provavel`.
{% call duckdb_query() %}
    select * from {{ fonte_lake('colunas_candidatos', 'lake_staging_historico') }}
{% endcall %}
