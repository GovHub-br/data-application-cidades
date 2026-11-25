-- Grafo de nomes de coluna (task mapear_colunas da dados_historicos_dag): o nome canônico que a
-- task propõe para cada coluna de cada família (proposta; a
-- decisão é o de-para da silver, em prata_dhist_de_para).
{% call duckdb_query() %}
    select * from {{ fonte_lake('colunas_conceitos', 'lake_staging_historico') }}
{% endcall %}
