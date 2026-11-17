-- O log do cleansed: uma linha por saída (arquivo ou bloco), com família, status
-- (tratado, versao_substituida, excluido_formatado), competência, data de corte, alertas
-- de data, colunas e o que o tratamento fez. É o contexto que a silver junta pela `saida`.
-- O mapa de descartes por motivo vai como texto: o Postgres não tem o tipo mapa.
{% call duckdb_query() %}
    select * replace (cast(descartes_por_motivo as varchar) as descartes_por_motivo)
    from {{ fonte_lake('cleansed_log', 'lake_staging_historico') }}
{% endcall %}
