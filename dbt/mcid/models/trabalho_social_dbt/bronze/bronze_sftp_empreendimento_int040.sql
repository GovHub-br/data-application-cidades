{{ config(materialized="table", tags=["ts_lake"]) }}

-- Bronze: INT040 do SFTP/GEFUS (FAR CAIXA), TODAS as remessas empilhadas, cópia fiel.
-- Fonte da tipologia (dsc_tipologia) por APF da prata_ts_empreendimento_perfil, que escolhe a
-- remessa mais recente de cada APF por dt_movimento.
--
-- Mesmo nome e mesmo filtro da bronze do eixo histórico que saiu do dbt em 24/11 (8f33a8f7,
-- macro bronze_gefus): só remessas mensais (_AAAAMMDD.parquet) e nunca as de VALIDACAO, que são
-- dado de teste. Reentregas com outro sufixo ficam de fora para não duplicar APF x mês.
{% set padrao = caminho_fonte_lake('far_empreendimentos_caixa_int040', 'trabalho_social_lake') %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true)
where regexp_matches(filename, '_\d{8}\.parquet$') and filename not ilike '%validacao%'
