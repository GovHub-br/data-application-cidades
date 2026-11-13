{{ config(materialized="table") }}

-- Bronze: Cadastro PJ mensal do FAR — dados cadastrais do empreendimento contratado.
-- Fonte: SHPT — sharepoint/Novo MCMV - FAR/
{% set padrao = caminho_fonte_lake('monit_cad_pj_far_mensal', 'trabalho_social_lake') %}

select columns(*)::varchar
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
