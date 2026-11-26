{{ config(materialized="table", tags=["ts_lake"]) }}

-- Bronze: INT059 do SFTP/GEFUS (FDS CAIXA), TODAS as remessas empilhadas, cópia fiel.
-- Fonte da tipologia (dsc_tipologia) por APF da prata_ts_empreendimento_perfil, que escolhe a
-- remessa mais recente de cada APF por dt_movimento.
--
-- Mesmo nome e mesmo filtro da bronze do eixo histórico que saiu do dbt em 24/11 (8f33a8f7,
-- macro bronze_gefus): só remessas mensais (_AAAAMMDD.parquet) e nunca as de VALIDACAO, que são
-- dado de teste. Reentregas com outro sufixo ficam de fora para não duplicar APF x mês.
{% set padrao = caminho_fonte_lake('fds_empreendimentos_caixa_int059', 'trabalho_social_lake') %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
-- Roda no Postgres pelo pg_duckdb (seletor dominios_postgres): a coluna do arquivo só é
-- acessível como r['filename'], e o sufixo _AAAAMMDD.parquet é conferido pelo tamanho do
-- último trecho do nome (16 caracteres), sem regex.
where
    length(split_part(cast(r['filename'] as varchar), '_', -1)) = 16
    and cast(r['filename'] as varchar) not ilike '%validacao%'
