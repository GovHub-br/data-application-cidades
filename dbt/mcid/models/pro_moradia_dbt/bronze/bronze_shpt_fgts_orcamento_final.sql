{{ config(materialized="table") }}

-- Bronze: Orçamento do FGTS por programa e região (prestação de contas anual ao
-- Conselho Curador): orçamento original, final e alocado. É o "Planejamento" do
-- Pró-Moradia, que entra aqui junto com os outros programas do fundo.
-- Fonte: SHPT — sharepoint/ (tpc_<ano>_fgts_canal_tab_dbo_pc_<ano>_<nn>_orcamento_final)
--
-- Diferente das outras bronzes, NÃO pega só o arquivo mais recente: cada arquivo é
-- a dotação de UM ano, e a série precisa de todos. `filename` fica para a prata
-- desempatar se o mesmo ano chegar em mais de um arquivo.
{% set padrao = "s3://data-lake-mcid/staging/**/tpc_*_fgts_canal_tab_dbo_pc_*_orcamento_final.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
