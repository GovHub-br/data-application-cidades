-- Valida que a data de movimento informada pela fonte (dt_movimento) e
-- consistente com o mes de referencia extraido do nome do arquivo
-- (dt_referencia). Esperado: mesmo mes. Falha se houver divergencia.
select
    fonte_tabela,
    count(*) as divergencias
from {{ ref('historico_mcmv_empreendimentos_snapshot') }}
where dt_movimento is not null
  and date_trunc('month', dt_movimento) <> date_trunc('month', dt_referencia)
group by fonte_tabela
having count(*) > 0
