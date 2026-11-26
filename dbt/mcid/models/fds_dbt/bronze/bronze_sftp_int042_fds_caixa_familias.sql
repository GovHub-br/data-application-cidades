{{ config(materialized="table") }}

-- Bronze: Famílias (pessoa física) do MCMV Entidades (FDS) no legado, na CAIXA (integração INT042).
-- Fonte: SFTP — sftp/fabrica/GEFUS/ (ANTERIORES/ guarda o histórico)
-- Hoje chega como remessa trimestral _CONTINGENCIA; cada arquivo é um retrato completo e a bronze
-- lê o mais recente.
{% set padrao = "s3://data-lake-mcid/snh/staging/**/*INT042_MinisterioCidades_FDS_CAIXA_PF_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where
    cast(r['filename'] as varchar)
    = {{ arquivo_mais_recente(padrao, excluir=["VALIDACAO", "SUBSTITUIDO", "ALTERACAO_LAYOUT"]) }}
