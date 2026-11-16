-- O log do cleansed: uma linha por saída (arquivo ou bloco), com família, status
-- (tratado, versao_substituida, excluido_formatado), competência, data de corte, alertas
-- de data, colunas e o que o tratamento fez. É o contexto que a silver junta pela `saida`.
select * from {{ fonte_dhist_log() }}
