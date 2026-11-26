-- Grafo de nomes de coluna (task mapear_colunas da dados_historicos_dag): cada coluna de cada
-- família, com a primeira e a última competência, o número de
-- arquivos e os layouts em que aparece.
{% call duckdb_query() %}
    select * from {{ fonte_lake('colunas_evolucao', 'lake_staging_historico') }}
{% endcall %}
