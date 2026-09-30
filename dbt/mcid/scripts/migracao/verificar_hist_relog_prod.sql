-- =====================================================================
-- verificar_hist_relog_prod.sql — READ-ONLY. Rodar ANTES e DEPOIS da
-- migração e diffar as saídas.
--
-- GERADO por scripts/migracao/gerar_migracao_hist_relog.py — NÃO EDITAR À MÃO.
--   psql "$DSN" -f scripts/migracao/verificar_hist_relog_prod.sql
-- =====================================================================

\echo '== tabelas presentes nos schemas prata/ouro cujo nome contem historico/reloginho ou hist/relog =='
select table_schema, table_name
from information_schema.tables
where table_schema in ('ouro', 'prata')
    and (table_name like '%historico%' or table_name like '%reloginho%'
         or table_name like '%hist%' or table_name like '%relog%')
order by table_schema, table_name;

\echo '== contagem por tabela — nomes ANTIGOS (deve existir ANTES da migração) =='
select * from (
select 'prata.prata_classe_media_historico_contrato' as tabela, (select count(*) from "prata"."prata_classe_media_historico_contrato") as n_linhas
union all
select 'prata.prata_far_historico_empreendimento' as tabela, (select count(*) from "prata"."prata_far_historico_empreendimento") as n_linhas
union all
select 'prata.prata_fds_historico_empreendimento' as tabela, (select count(*) from "prata"."prata_fds_historico_empreendimento") as n_linhas
union all
select 'prata.prata_fnhis_historico_proposta' as tabela, (select count(*) from "prata"."prata_fnhis_historico_proposta") as n_linhas
union all
select 'prata.prata_historico_entrega_apf' as tabela, (select count(*) from "prata"."prata_historico_entrega_apf") as n_linhas
union all
select 'prata.prata_historico_serie_executiva' as tabela, (select count(*) from "prata"."prata_historico_serie_executiva") as n_linhas
union all
select 'prata.prata_mcmv_cidades_historico_contrato' as tabela, (select count(*) from "prata"."prata_mcmv_cidades_historico_contrato") as n_linhas
union all
select 'prata.prata_pro_moradia_historico_contrato' as tabela, (select count(*) from "prata"."prata_pro_moradia_historico_contrato") as n_linhas
union all
select 'prata.prata_pro_moradia_historico_desembolso_mensal' as tabela, (select count(*) from "prata"."prata_pro_moradia_historico_desembolso_mensal") as n_linhas
union all
select 'prata.prata_pro_moradia_historico_paralisacao' as tabela, (select count(*) from "prata"."prata_pro_moradia_historico_paralisacao") as n_linhas
union all
select 'prata.prata_reforma_casa_brasil_historico_contrato' as tabela, (select count(*) from "prata"."prata_reforma_casa_brasil_historico_contrato") as n_linhas
union all
select 'prata.prata_rural_historico_empreendimento' as tabela, (select count(*) from "prata"."prata_rural_historico_empreendimento") as n_linhas
union all
select 'ouro.ouro_historico_marco_empreendimento' as tabela, (select count(*) from "ouro"."ouro_historico_marco_empreendimento") as n_linhas
union all
select 'ouro.ouro_historico_pro_moradia_evolucao_financeira' as tabela, (select count(*) from "ouro"."ouro_historico_pro_moradia_evolucao_financeira") as n_linhas
union all
select 'ouro.ouro_historico_pro_moradia_paralisacao' as tabela, (select count(*) from "ouro"."ouro_historico_pro_moradia_paralisacao") as n_linhas
union all
select 'ouro.ouro_historico_serie_mensal_frentes_novas' as tabela, (select count(*) from "ouro"."ouro_historico_serie_mensal_frentes_novas") as n_linhas
union all
select 'ouro.ouro_historico_serie_mensal' as tabela, (select count(*) from "ouro"."ouro_historico_serie_mensal") as n_linhas
union all
select 'ouro.ouro_historico_serie_situacao_mensal' as tabela, (select count(*) from "ouro"."ouro_historico_serie_situacao_mensal") as n_linhas
union all
select 'ouro.ouro_historico_snapshot_empreendimento_atual' as tabela, (select count(*) from "ouro"."ouro_historico_snapshot_empreendimento_atual") as n_linhas
union all
select 'prata.prata_historico_snh_apf_mes' as tabela, (select count(*) from "prata"."prata_historico_snh_apf_mes") as n_linhas
union all
select 'prata.prata_historico_snh_entregas_mes' as tabela, (select count(*) from "prata"."prata_historico_snh_entregas_mes") as n_linhas
union all
select 'prata.prata_reloginho_classe_media_reforma_resumo_faixa_renda_mes' as tabela, (select count(*) from "prata"."prata_reloginho_classe_media_reforma_resumo_faixa_renda_mes") as n_linhas
union all
select 'prata.prata_reloginho_frentes_novas_resumo_mes_uf' as tabela, (select count(*) from "prata"."prata_reloginho_frentes_novas_resumo_mes_uf") as n_linhas
union all
select 'prata.prata_reloginho_frentes_novas_resumo_mes' as tabela, (select count(*) from "prata"."prata_reloginho_frentes_novas_resumo_mes") as n_linhas
union all
select 'ouro.ouro_reloginho_classe_media_reforma_indicadores_faixa_renda' as tabela, (select count(*) from "ouro"."ouro_reloginho_classe_media_reforma_indicadores_faixa_renda") as n_linhas
union all
select 'ouro.ouro_reloginho_indicadores_entregas' as tabela, (select count(*) from "ouro"."ouro_reloginho_indicadores_entregas") as n_linhas
union all
select 'ouro.ouro_reloginho_indicadores_frentes_novas_regiao' as tabela, (select count(*) from "ouro"."ouro_reloginho_indicadores_frentes_novas_regiao") as n_linhas
union all
select 'ouro.ouro_reloginho_indicadores_frentes_novas' as tabela, (select count(*) from "ouro"."ouro_reloginho_indicadores_frentes_novas") as n_linhas
union all
select 'ouro.ouro_reloginho_indicadores_frente' as tabela, (select count(*) from "ouro"."ouro_reloginho_indicadores_frente") as n_linhas
union all
select 'ouro.ouro_reloginho_indicadores_gargalo_desempenho' as tabela, (select count(*) from "ouro"."ouro_reloginho_indicadores_gargalo_desempenho") as n_linhas
union all
select 'ouro.ouro_reloginho_indicadores' as tabela, (select count(*) from "ouro"."ouro_reloginho_indicadores") as n_linhas
union all
select 'ouro.ouro_reloginho_resumo_dashboard_frentes_novas' as tabela, (select count(*) from "ouro"."ouro_reloginho_resumo_dashboard_frentes_novas") as n_linhas
union all
select 'ouro.ouro_reloginho_resumo_dashboard' as tabela, (select count(*) from "ouro"."ouro_reloginho_resumo_dashboard") as n_linhas
union all
select 'ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard' as tabela, (select count(*) from "ouro"."ouro_reloginho_resumo_gargalo_desempenho_dashboard") as n_linhas
) t order by tabela;

\echo '== contagem por tabela — nomes NOVOS (deve existir DEPOIS da migração) =='
select * from (
select 'prata.prata_hist_classe_media_contrato' as tabela, (select count(*) from "prata"."prata_hist_classe_media_contrato") as n_linhas
union all
select 'prata.prata_hist_far_empreendimento' as tabela, (select count(*) from "prata"."prata_hist_far_empreendimento") as n_linhas
union all
select 'prata.prata_hist_fds_empreendimento' as tabela, (select count(*) from "prata"."prata_hist_fds_empreendimento") as n_linhas
union all
select 'prata.prata_hist_fnhis_proposta' as tabela, (select count(*) from "prata"."prata_hist_fnhis_proposta") as n_linhas
union all
select 'prata.prata_hist_entrega_apf' as tabela, (select count(*) from "prata"."prata_hist_entrega_apf") as n_linhas
union all
select 'prata.prata_hist_serie_executiva' as tabela, (select count(*) from "prata"."prata_hist_serie_executiva") as n_linhas
union all
select 'prata.prata_hist_mcmv_cidades_contrato' as tabela, (select count(*) from "prata"."prata_hist_mcmv_cidades_contrato") as n_linhas
union all
select 'prata.prata_hist_pro_moradia_contrato' as tabela, (select count(*) from "prata"."prata_hist_pro_moradia_contrato") as n_linhas
union all
select 'prata.prata_hist_pro_moradia_desembolso_mensal' as tabela, (select count(*) from "prata"."prata_hist_pro_moradia_desembolso_mensal") as n_linhas
union all
select 'prata.prata_hist_pro_moradia_paralisacao' as tabela, (select count(*) from "prata"."prata_hist_pro_moradia_paralisacao") as n_linhas
union all
select 'prata.prata_hist_reforma_casa_brasil_contrato' as tabela, (select count(*) from "prata"."prata_hist_reforma_casa_brasil_contrato") as n_linhas
union all
select 'prata.prata_hist_rural_empreendimento' as tabela, (select count(*) from "prata"."prata_hist_rural_empreendimento") as n_linhas
union all
select 'ouro.ouro_hist_marco_empreendimento' as tabela, (select count(*) from "ouro"."ouro_hist_marco_empreendimento") as n_linhas
union all
select 'ouro.ouro_hist_pro_moradia_evolucao_financeira' as tabela, (select count(*) from "ouro"."ouro_hist_pro_moradia_evolucao_financeira") as n_linhas
union all
select 'ouro.ouro_hist_pro_moradia_paralisacao' as tabela, (select count(*) from "ouro"."ouro_hist_pro_moradia_paralisacao") as n_linhas
union all
select 'ouro.ouro_hist_serie_mensal_frentes_novas' as tabela, (select count(*) from "ouro"."ouro_hist_serie_mensal_frentes_novas") as n_linhas
union all
select 'ouro.ouro_hist_serie_mensal' as tabela, (select count(*) from "ouro"."ouro_hist_serie_mensal") as n_linhas
union all
select 'ouro.ouro_hist_serie_situacao_mensal' as tabela, (select count(*) from "ouro"."ouro_hist_serie_situacao_mensal") as n_linhas
union all
select 'ouro.ouro_hist_snapshot_empreendimento_atual' as tabela, (select count(*) from "ouro"."ouro_hist_snapshot_empreendimento_atual") as n_linhas
union all
select 'prata.prata_hist_snh_apf_mes' as tabela, (select count(*) from "prata"."prata_hist_snh_apf_mes") as n_linhas
union all
select 'prata.prata_hist_snh_entregas_mes' as tabela, (select count(*) from "prata"."prata_hist_snh_entregas_mes") as n_linhas
union all
select 'prata.prata_relog_classe_media_reforma_resumo_faixa_renda_mes' as tabela, (select count(*) from "prata"."prata_relog_classe_media_reforma_resumo_faixa_renda_mes") as n_linhas
union all
select 'prata.prata_relog_frentes_novas_resumo_mes_uf' as tabela, (select count(*) from "prata"."prata_relog_frentes_novas_resumo_mes_uf") as n_linhas
union all
select 'prata.prata_relog_frentes_novas_resumo_mes' as tabela, (select count(*) from "prata"."prata_relog_frentes_novas_resumo_mes") as n_linhas
union all
select 'ouro.ouro_relog_classe_media_reforma_indicadores_faixa_renda' as tabela, (select count(*) from "ouro"."ouro_relog_classe_media_reforma_indicadores_faixa_renda") as n_linhas
union all
select 'ouro.ouro_relog_indicadores_entregas' as tabela, (select count(*) from "ouro"."ouro_relog_indicadores_entregas") as n_linhas
union all
select 'ouro.ouro_relog_indicadores_frentes_novas_regiao' as tabela, (select count(*) from "ouro"."ouro_relog_indicadores_frentes_novas_regiao") as n_linhas
union all
select 'ouro.ouro_relog_indicadores_frentes_novas' as tabela, (select count(*) from "ouro"."ouro_relog_indicadores_frentes_novas") as n_linhas
union all
select 'ouro.ouro_relog_indicadores_frente' as tabela, (select count(*) from "ouro"."ouro_relog_indicadores_frente") as n_linhas
union all
select 'ouro.ouro_relog_indicadores_gargalo_desempenho' as tabela, (select count(*) from "ouro"."ouro_relog_indicadores_gargalo_desempenho") as n_linhas
union all
select 'ouro.ouro_relog_indicadores' as tabela, (select count(*) from "ouro"."ouro_relog_indicadores") as n_linhas
union all
select 'ouro.ouro_relog_resumo_dashboard_frentes_novas' as tabela, (select count(*) from "ouro"."ouro_relog_resumo_dashboard_frentes_novas") as n_linhas
union all
select 'ouro.ouro_relog_resumo_dashboard' as tabela, (select count(*) from "ouro"."ouro_relog_resumo_dashboard") as n_linhas
union all
select 'ouro.ouro_relog_resumo_gargalo_desempenho_dashboard' as tabela, (select count(*) from "ouro"."ouro_relog_resumo_gargalo_desempenho_dashboard") as n_linhas
) t order by tabela;
