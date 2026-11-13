{{ config(materialized="table") }}

-- Bronze: Execução do trabalho social do FDS por empreendimento.
-- Fonte: SHPT — sharepoint/Novo MCMV - FDS/
{% set padrao = caminho_fonte_lake('monit_exec_ts_fds_mensal', 'trabalho_social_lake') %}

select columns(*)::varchar
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
