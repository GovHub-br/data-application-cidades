-- =====================================================================
-- verificar_nomenclatura_prod.sql — READ-ONLY. Rodar ANTES e DEPOIS da
-- migração e diffar as saídas.
--
-- GERADO por scripts/migracao/gerar_migracao.py — NÃO EDITAR À MÃO.
--   psql "$DSN" -f scripts/migracao/verificar_nomenclatura_prod.sql
-- =====================================================================

\echo '== tabelas presentes nos schemas de origem e destino =='
select table_schema, table_name
from information_schema.tables
where table_schema in ('bronze', 'dados_historicos', 'empreendimento_far', 'empreendimento_rural', 'empreendimentos_fds', 'ouro', 'prata', 'reloginho')
order by table_schema, table_name;

\echo '== contagem por tabela — nomes ANTIGOS (deve existir ANTES da migração) =='
select * from (
select 'dados_historicos.bronze_mcmv_historico_serie_entrada_bb' as tabela, (select count(*) from "dados_historicos"."bronze_mcmv_historico_serie_entrada_bb") as n_linhas
union all
select 'dados_historicos.bronze_mcmv_historico_empreendimento_snh_bb' as tabela, (select count(*) from "dados_historicos"."bronze_mcmv_historico_empreendimento_snh_bb") as n_linhas
union all
select 'reloginho.bronze_reloginho_snh_entregas_evento_bb' as tabela, (select count(*) from "reloginho"."bronze_reloginho_snh_entregas_evento_bb") as n_linhas
union all
select 'dados_historicos.bronze_mcmv_historico_empreendimento_int054' as tabela, (select count(*) from "dados_historicos"."bronze_mcmv_historico_empreendimento_int054") as n_linhas
union all
select 'dados_historicos.bronze_mcmv_historico_empreendimento_int059' as tabela, (select count(*) from "dados_historicos"."bronze_mcmv_historico_empreendimento_int059") as n_linhas
union all
select 'dados_historicos.bronze_mcmv_historico_empreendimento_int057' as tabela, (select count(*) from "dados_historicos"."bronze_mcmv_historico_empreendimento_int057") as n_linhas
union all
select 'dados_historicos.bronze_mcmv_historico_empreendimento_int040' as tabela, (select count(*) from "dados_historicos"."bronze_mcmv_historico_empreendimento_int040") as n_linhas
union all
select 'reloginho.bronze_reloginho_snh_entregas_evento_caixa' as tabela, (select count(*) from "reloginho"."bronze_reloginho_snh_entregas_evento_caixa") as n_linhas
union all
select 'dados_historicos.bronze_mcmv_historico_empreendimento_snh_caixa' as tabela, (select count(*) from "dados_historicos"."bronze_mcmv_historico_empreendimento_snh_caixa") as n_linhas
union all
select 'dados_historicos.bronze_mcmv_historico_empreendimento_int065' as tabela, (select count(*) from "dados_historicos"."bronze_mcmv_historico_empreendimento_int065") as n_linhas
union all
select 'dados_historicos.bronze_mcmv_historico_serie_bases_relatorio_executivo' as tabela, (select count(*) from "dados_historicos"."bronze_mcmv_historico_serie_bases_relatorio_executivo") as n_linhas
union all
select 'dados_historicos.bronze_mcmv_historico_serie_min_cidades' as tabela, (select count(*) from "dados_historicos"."bronze_mcmv_historico_serie_min_cidades") as n_linhas
union all
select 'dados_historicos.bronze_mcmv_historico_serie_bext' as tabela, (select count(*) from "dados_historicos"."bronze_mcmv_historico_serie_bext") as n_linhas
union all
select 'reloginho.silver_historico_snh_entregas_mes' as tabela, (select count(*) from "reloginho"."silver_historico_snh_entregas_mes") as n_linhas
union all
select 'reloginho.silver_historico_snh_apf_mes' as tabela, (select count(*) from "reloginho"."silver_historico_snh_apf_mes") as n_linhas
union all
select 'reloginho.gold_indicadores_reloginho' as tabela, (select count(*) from "reloginho"."gold_indicadores_reloginho") as n_linhas
union all
select 'reloginho.gold_indicadores_reloginho_frente' as tabela, (select count(*) from "reloginho"."gold_indicadores_reloginho_frente") as n_linhas
union all
select 'reloginho.gold_indicadores_reloginho_entregas' as tabela, (select count(*) from "reloginho"."gold_indicadores_reloginho_entregas") as n_linhas
union all
select 'reloginho.gold_resumo_reloginho_dashboard' as tabela, (select count(*) from "reloginho"."gold_resumo_reloginho_dashboard") as n_linhas
union all
select 'reloginho.gold_indicadores_gargalo_desempenho' as tabela, (select count(*) from "reloginho"."gold_indicadores_gargalo_desempenho") as n_linhas
union all
select 'reloginho.gold_resumo_gargalo_desempenho_dashboard' as tabela, (select count(*) from "reloginho"."gold_resumo_gargalo_desempenho_dashboard") as n_linhas
union all
select 'empreendimentos_fds.silver_historico_empreendimento' as tabela, (select count(*) from "empreendimentos_fds"."silver_historico_empreendimento") as n_linhas
union all
select 'empreendimento_far.silver_historico_empreendimento' as tabela, (select count(*) from "empreendimento_far"."silver_historico_empreendimento") as n_linhas
union all
select 'empreendimento_rural.silver_historico_empreendimento' as tabela, (select count(*) from "empreendimento_rural"."silver_historico_empreendimento") as n_linhas
union all
select 'dados_historicos.gold_serie_mensal' as tabela, (select count(*) from "dados_historicos"."gold_serie_mensal") as n_linhas
union all
select 'dados_historicos.gold_snapshot_empreendimento_atual' as tabela, (select count(*) from "dados_historicos"."gold_snapshot_empreendimento_atual") as n_linhas
union all
select 'dados_historicos.gold_marco_empreendimento' as tabela, (select count(*) from "dados_historicos"."gold_marco_empreendimento") as n_linhas
union all
select 'dados_historicos.gold_serie_situacao_mensal' as tabela, (select count(*) from "dados_historicos"."gold_serie_situacao_mensal") as n_linhas
union all
select 'dados_historicos.silver_mcmv_historico_serie_executiva' as tabela, (select count(*) from "dados_historicos"."silver_mcmv_historico_serie_executiva") as n_linhas
) t order by tabela;

\echo '== contagem por tabela — nomes NOVOS (deve existir DEPOIS da migração) =='
select * from (
select 'bronze.bronze_dhist_serie_entrada_bb' as tabela, (select count(*) from "bronze"."bronze_dhist_serie_entrada_bb") as n_linhas
union all
select 'bronze.bronze_dhist_empreendimento_snh_bb' as tabela, (select count(*) from "bronze"."bronze_dhist_empreendimento_snh_bb") as n_linhas
union all
select 'bronze.bronze_dhist_snh_entregas_evento_bb' as tabela, (select count(*) from "bronze"."bronze_dhist_snh_entregas_evento_bb") as n_linhas
union all
select 'bronze.bronze_sftp_empreendimento_int054' as tabela, (select count(*) from "bronze"."bronze_sftp_empreendimento_int054") as n_linhas
union all
select 'bronze.bronze_sftp_empreendimento_int059' as tabela, (select count(*) from "bronze"."bronze_sftp_empreendimento_int059") as n_linhas
union all
select 'bronze.bronze_sftp_empreendimento_int057' as tabela, (select count(*) from "bronze"."bronze_sftp_empreendimento_int057") as n_linhas
union all
select 'bronze.bronze_sftp_empreendimento_int040' as tabela, (select count(*) from "bronze"."bronze_sftp_empreendimento_int040") as n_linhas
union all
select 'bronze.bronze_dhist_snh_entregas_evento_caixa' as tabela, (select count(*) from "bronze"."bronze_dhist_snh_entregas_evento_caixa") as n_linhas
union all
select 'bronze.bronze_dhist_empreendimento_snh_caixa' as tabela, (select count(*) from "bronze"."bronze_dhist_empreendimento_snh_caixa") as n_linhas
union all
select 'bronze.bronze_sftp_empreendimento_int065' as tabela, (select count(*) from "bronze"."bronze_sftp_empreendimento_int065") as n_linhas
union all
select 'bronze.bronze_dhist_serie_bases_relatorio_executivo' as tabela, (select count(*) from "bronze"."bronze_dhist_serie_bases_relatorio_executivo") as n_linhas
union all
select 'bronze.bronze_dhist_serie_min_cidades' as tabela, (select count(*) from "bronze"."bronze_dhist_serie_min_cidades") as n_linhas
union all
select 'bronze.bronze_dhist_serie_bext' as tabela, (select count(*) from "bronze"."bronze_dhist_serie_bext") as n_linhas
union all
select 'prata.prata_dhist_snh_entregas_mes' as tabela, (select count(*) from "prata"."prata_dhist_snh_entregas_mes") as n_linhas
union all
select 'prata.prata_dhist_snh_apf_mes' as tabela, (select count(*) from "prata"."prata_dhist_snh_apf_mes") as n_linhas
union all
select 'ouro.ouro_reloginho_indicadores' as tabela, (select count(*) from "ouro"."ouro_reloginho_indicadores") as n_linhas
union all
select 'ouro.ouro_reloginho_indicadores_frente' as tabela, (select count(*) from "ouro"."ouro_reloginho_indicadores_frente") as n_linhas
union all
select 'ouro.ouro_reloginho_indicadores_entregas' as tabela, (select count(*) from "ouro"."ouro_reloginho_indicadores_entregas") as n_linhas
union all
select 'ouro.ouro_reloginho_resumo_dashboard' as tabela, (select count(*) from "ouro"."ouro_reloginho_resumo_dashboard") as n_linhas
union all
select 'ouro.ouro_reloginho_indicadores_gargalo_desempenho' as tabela, (select count(*) from "ouro"."ouro_reloginho_indicadores_gargalo_desempenho") as n_linhas
union all
select 'ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard' as tabela, (select count(*) from "ouro"."ouro_reloginho_resumo_gargalo_desempenho_dashboard") as n_linhas
union all
select 'prata.prata_fds_historico_empreendimento' as tabela, (select count(*) from "prata"."prata_fds_historico_empreendimento") as n_linhas
union all
select 'prata.prata_far_historico_empreendimento' as tabela, (select count(*) from "prata"."prata_far_historico_empreendimento") as n_linhas
union all
select 'prata.prata_rural_historico_empreendimento' as tabela, (select count(*) from "prata"."prata_rural_historico_empreendimento") as n_linhas
union all
select 'ouro.ouro_dhist_serie_mensal' as tabela, (select count(*) from "ouro"."ouro_dhist_serie_mensal") as n_linhas
union all
select 'ouro.ouro_dhist_snapshot_empreendimento_atual' as tabela, (select count(*) from "ouro"."ouro_dhist_snapshot_empreendimento_atual") as n_linhas
union all
select 'ouro.ouro_dhist_marco_empreendimento' as tabela, (select count(*) from "ouro"."ouro_dhist_marco_empreendimento") as n_linhas
union all
select 'ouro.ouro_dhist_serie_situacao_mensal' as tabela, (select count(*) from "ouro"."ouro_dhist_serie_situacao_mensal") as n_linhas
union all
select 'prata.prata_dhist_serie_executiva' as tabela, (select count(*) from "prata"."prata_dhist_serie_executiva") as n_linhas
) t order by tabela;
