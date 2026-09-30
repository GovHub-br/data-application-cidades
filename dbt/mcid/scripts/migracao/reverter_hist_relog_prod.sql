-- =====================================================================
-- reverter_hist_relog_prod.sql — ROLLBACK: hist/relog -> historico/reloginho
--
-- GERADO por scripts/migracao/gerar_migracao_hist_relog.py a partir de
-- mapa_hist_relog.csv. NÃO EDITAR À MÃO — regenerar com o script.
--
-- ATENÇÃO: script MANUAL, roda contra o Postgres `prod`. NÃO deve ser chamado
-- por publicar_historico.py, publicar-historico.sh, run-*.sh, rebuild-local.sh
-- nem por hooks on-run-* do dbt. Renomeia METADADO (ALTER TABLE ... RENAME TO,
-- dentro do MESMO schema) — instantâneo, não copia dados. Consumidores que
-- referenciam as tabelas por string de nome (Superset / OpenMetadata /
-- notebooks) quebram — usuário já confirmou que o inventário abaixo é seguro.
--
-- Uso:
--   psql "$DSN" -v ON_ERROR_STOP=1 -f scripts/migracao/reverter_hist_relog_prod.sql
-- (ordem e pré-requisitos: ver README.md)
-- =====================================================================

\set ON_ERROR_STOP on

begin;

-- 1) preflight -------------------------------------------------------
do $$
begin
    -- preflight: 34 tabelas no inventario
    if to_regclass('prata.prata_hist_classe_media_contrato') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_classe_media_contrato';
    end if;
    if to_regclass('prata.prata_hist_far_empreendimento') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_far_empreendimento';
    end if;
    if to_regclass('prata.prata_hist_fds_empreendimento') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_fds_empreendimento';
    end if;
    if to_regclass('prata.prata_hist_fnhis_proposta') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_fnhis_proposta';
    end if;
    if to_regclass('prata.prata_hist_entrega_apf') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_entrega_apf';
    end if;
    if to_regclass('prata.prata_hist_serie_executiva') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_serie_executiva';
    end if;
    if to_regclass('prata.prata_hist_mcmv_cidades_contrato') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_mcmv_cidades_contrato';
    end if;
    if to_regclass('prata.prata_hist_pro_moradia_contrato') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_pro_moradia_contrato';
    end if;
    if to_regclass('prata.prata_hist_pro_moradia_desembolso_mensal') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_pro_moradia_desembolso_mensal';
    end if;
    if to_regclass('prata.prata_hist_pro_moradia_paralisacao') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_pro_moradia_paralisacao';
    end if;
    if to_regclass('prata.prata_hist_reforma_casa_brasil_contrato') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_reforma_casa_brasil_contrato';
    end if;
    if to_regclass('prata.prata_hist_rural_empreendimento') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_rural_empreendimento';
    end if;
    if to_regclass('ouro.ouro_hist_marco_empreendimento') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_hist_marco_empreendimento';
    end if;
    if to_regclass('ouro.ouro_hist_pro_moradia_evolucao_financeira') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_hist_pro_moradia_evolucao_financeira';
    end if;
    if to_regclass('ouro.ouro_hist_pro_moradia_paralisacao') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_hist_pro_moradia_paralisacao';
    end if;
    if to_regclass('ouro.ouro_hist_serie_mensal_frentes_novas') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_hist_serie_mensal_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_hist_serie_mensal') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_hist_serie_mensal';
    end if;
    if to_regclass('ouro.ouro_hist_serie_situacao_mensal') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_hist_serie_situacao_mensal';
    end if;
    if to_regclass('ouro.ouro_hist_snapshot_empreendimento_atual') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_hist_snapshot_empreendimento_atual';
    end if;
    if to_regclass('prata.prata_hist_snh_apf_mes') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_snh_apf_mes';
    end if;
    if to_regclass('prata.prata_hist_snh_entregas_mes') is null then
        raise exception 'origem ausente: %', 'prata.prata_hist_snh_entregas_mes';
    end if;
    if to_regclass('prata.prata_relog_classe_media_reforma_resumo_faixa_renda_mes') is null then
        raise exception 'origem ausente: %', 'prata.prata_relog_classe_media_reforma_resumo_faixa_renda_mes';
    end if;
    if to_regclass('prata.prata_relog_frentes_novas_resumo_mes_uf') is null then
        raise exception 'origem ausente: %', 'prata.prata_relog_frentes_novas_resumo_mes_uf';
    end if;
    if to_regclass('prata.prata_relog_frentes_novas_resumo_mes') is null then
        raise exception 'origem ausente: %', 'prata.prata_relog_frentes_novas_resumo_mes';
    end if;
    if to_regclass('ouro.ouro_relog_classe_media_reforma_indicadores_faixa_renda') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_relog_classe_media_reforma_indicadores_faixa_renda';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores_entregas') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_relog_indicadores_entregas';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores_frentes_novas_regiao') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_relog_indicadores_frentes_novas_regiao';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores_frentes_novas') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_relog_indicadores_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores_frente') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_relog_indicadores_frente';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores_gargalo_desempenho') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_relog_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_relog_indicadores';
    end if;
    if to_regclass('ouro.ouro_relog_resumo_dashboard_frentes_novas') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_relog_resumo_dashboard_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_relog_resumo_dashboard') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_relog_resumo_dashboard';
    end if;
    if to_regclass('ouro.ouro_relog_resumo_gargalo_desempenho_dashboard') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_relog_resumo_gargalo_desempenho_dashboard';
    end if;
    if to_regclass('prata.prata_classe_media_historico_contrato') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_classe_media_historico_contrato';
    end if;
    if to_regclass('prata.prata_far_historico_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_far_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_fds_historico_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_fds_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_fnhis_historico_proposta') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_fnhis_historico_proposta';
    end if;
    if to_regclass('prata.prata_historico_entrega_apf') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_historico_entrega_apf';
    end if;
    if to_regclass('prata.prata_historico_serie_executiva') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_historico_serie_executiva';
    end if;
    if to_regclass('prata.prata_mcmv_cidades_historico_contrato') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_mcmv_cidades_historico_contrato';
    end if;
    if to_regclass('prata.prata_pro_moradia_historico_contrato') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_pro_moradia_historico_contrato';
    end if;
    if to_regclass('prata.prata_pro_moradia_historico_desembolso_mensal') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_pro_moradia_historico_desembolso_mensal';
    end if;
    if to_regclass('prata.prata_pro_moradia_historico_paralisacao') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_pro_moradia_historico_paralisacao';
    end if;
    if to_regclass('prata.prata_reforma_casa_brasil_historico_contrato') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_reforma_casa_brasil_historico_contrato';
    end if;
    if to_regclass('prata.prata_rural_historico_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_rural_historico_empreendimento';
    end if;
    if to_regclass('ouro.ouro_historico_marco_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_historico_marco_empreendimento';
    end if;
    if to_regclass('ouro.ouro_historico_pro_moradia_evolucao_financeira') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_historico_pro_moradia_evolucao_financeira';
    end if;
    if to_regclass('ouro.ouro_historico_pro_moradia_paralisacao') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_historico_pro_moradia_paralisacao';
    end if;
    if to_regclass('ouro.ouro_historico_serie_mensal_frentes_novas') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_historico_serie_mensal_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_historico_serie_mensal') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_historico_serie_mensal';
    end if;
    if to_regclass('ouro.ouro_historico_serie_situacao_mensal') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_historico_serie_situacao_mensal';
    end if;
    if to_regclass('ouro.ouro_historico_snapshot_empreendimento_atual') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_historico_snapshot_empreendimento_atual';
    end if;
    if to_regclass('prata.prata_historico_snh_apf_mes') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_historico_snh_apf_mes';
    end if;
    if to_regclass('prata.prata_historico_snh_entregas_mes') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_historico_snh_entregas_mes';
    end if;
    if to_regclass('prata.prata_reloginho_classe_media_reforma_resumo_faixa_renda_mes') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_reloginho_classe_media_reforma_resumo_faixa_renda_mes';
    end if;
    if to_regclass('prata.prata_reloginho_frentes_novas_resumo_mes_uf') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_reloginho_frentes_novas_resumo_mes_uf';
    end if;
    if to_regclass('prata.prata_reloginho_frentes_novas_resumo_mes') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_reloginho_frentes_novas_resumo_mes';
    end if;
    if to_regclass('ouro.ouro_reloginho_classe_media_reforma_indicadores_faixa_renda') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_classe_media_reforma_indicadores_faixa_renda';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_entregas') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_indicadores_entregas';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_frentes_novas_regiao') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_indicadores_frentes_novas_regiao';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_frentes_novas') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_indicadores_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_frente') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_indicadores_frente';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_gargalo_desempenho') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_indicadores';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_dashboard_frentes_novas') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_resumo_dashboard_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_dashboard') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_resumo_dashboard';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard';
    end if;
end $$;

-- 2) renomeia (mesmo schema — sem SET SCHEMA) ------------------------
alter table "prata"."prata_hist_classe_media_contrato" rename to "prata_classe_media_historico_contrato";
alter table "prata"."prata_hist_far_empreendimento" rename to "prata_far_historico_empreendimento";
alter table "prata"."prata_hist_fds_empreendimento" rename to "prata_fds_historico_empreendimento";
alter table "prata"."prata_hist_fnhis_proposta" rename to "prata_fnhis_historico_proposta";
alter table "prata"."prata_hist_entrega_apf" rename to "prata_historico_entrega_apf";
alter table "prata"."prata_hist_serie_executiva" rename to "prata_historico_serie_executiva";
alter table "prata"."prata_hist_mcmv_cidades_contrato" rename to "prata_mcmv_cidades_historico_contrato";
alter table "prata"."prata_hist_pro_moradia_contrato" rename to "prata_pro_moradia_historico_contrato";
alter table "prata"."prata_hist_pro_moradia_desembolso_mensal" rename to "prata_pro_moradia_historico_desembolso_mensal";
alter table "prata"."prata_hist_pro_moradia_paralisacao" rename to "prata_pro_moradia_historico_paralisacao";
alter table "prata"."prata_hist_reforma_casa_brasil_contrato" rename to "prata_reforma_casa_brasil_historico_contrato";
alter table "prata"."prata_hist_rural_empreendimento" rename to "prata_rural_historico_empreendimento";
alter table "ouro"."ouro_hist_marco_empreendimento" rename to "ouro_historico_marco_empreendimento";
alter table "ouro"."ouro_hist_pro_moradia_evolucao_financeira" rename to "ouro_historico_pro_moradia_evolucao_financeira";
alter table "ouro"."ouro_hist_pro_moradia_paralisacao" rename to "ouro_historico_pro_moradia_paralisacao";
alter table "ouro"."ouro_hist_serie_mensal_frentes_novas" rename to "ouro_historico_serie_mensal_frentes_novas";
alter table "ouro"."ouro_hist_serie_mensal" rename to "ouro_historico_serie_mensal";
alter table "ouro"."ouro_hist_serie_situacao_mensal" rename to "ouro_historico_serie_situacao_mensal";
alter table "ouro"."ouro_hist_snapshot_empreendimento_atual" rename to "ouro_historico_snapshot_empreendimento_atual";
alter table "prata"."prata_hist_snh_apf_mes" rename to "prata_historico_snh_apf_mes";
alter table "prata"."prata_hist_snh_entregas_mes" rename to "prata_historico_snh_entregas_mes";
alter table "prata"."prata_relog_classe_media_reforma_resumo_faixa_renda_mes" rename to "prata_reloginho_classe_media_reforma_resumo_faixa_renda_mes";
alter table "prata"."prata_relog_frentes_novas_resumo_mes_uf" rename to "prata_reloginho_frentes_novas_resumo_mes_uf";
alter table "prata"."prata_relog_frentes_novas_resumo_mes" rename to "prata_reloginho_frentes_novas_resumo_mes";
alter table "ouro"."ouro_relog_classe_media_reforma_indicadores_faixa_renda" rename to "ouro_reloginho_classe_media_reforma_indicadores_faixa_renda";
alter table "ouro"."ouro_relog_indicadores_entregas" rename to "ouro_reloginho_indicadores_entregas";
alter table "ouro"."ouro_relog_indicadores_frentes_novas_regiao" rename to "ouro_reloginho_indicadores_frentes_novas_regiao";
alter table "ouro"."ouro_relog_indicadores_frentes_novas" rename to "ouro_reloginho_indicadores_frentes_novas";
alter table "ouro"."ouro_relog_indicadores_frente" rename to "ouro_reloginho_indicadores_frente";
alter table "ouro"."ouro_relog_indicadores_gargalo_desempenho" rename to "ouro_reloginho_indicadores_gargalo_desempenho";
alter table "ouro"."ouro_relog_indicadores" rename to "ouro_reloginho_indicadores";
alter table "ouro"."ouro_relog_resumo_dashboard_frentes_novas" rename to "ouro_reloginho_resumo_dashboard_frentes_novas";
alter table "ouro"."ouro_relog_resumo_dashboard" rename to "ouro_reloginho_resumo_dashboard";
alter table "ouro"."ouro_relog_resumo_gargalo_desempenho_dashboard" rename to "ouro_reloginho_resumo_gargalo_desempenho_dashboard";

-- 3) postflight -----------------------------------------------------
do $$
begin
    if to_regclass('prata.prata_classe_media_historico_contrato') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_classe_media_historico_contrato';
    end if;
    if to_regclass('prata.prata_hist_classe_media_contrato') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_classe_media_contrato';
    end if;
    if to_regclass('prata.prata_far_historico_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_far_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_hist_far_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_far_empreendimento';
    end if;
    if to_regclass('prata.prata_fds_historico_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_fds_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_hist_fds_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_fds_empreendimento';
    end if;
    if to_regclass('prata.prata_fnhis_historico_proposta') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_fnhis_historico_proposta';
    end if;
    if to_regclass('prata.prata_hist_fnhis_proposta') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_fnhis_proposta';
    end if;
    if to_regclass('prata.prata_historico_entrega_apf') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_historico_entrega_apf';
    end if;
    if to_regclass('prata.prata_hist_entrega_apf') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_entrega_apf';
    end if;
    if to_regclass('prata.prata_historico_serie_executiva') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_historico_serie_executiva';
    end if;
    if to_regclass('prata.prata_hist_serie_executiva') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_serie_executiva';
    end if;
    if to_regclass('prata.prata_mcmv_cidades_historico_contrato') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_mcmv_cidades_historico_contrato';
    end if;
    if to_regclass('prata.prata_hist_mcmv_cidades_contrato') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_mcmv_cidades_contrato';
    end if;
    if to_regclass('prata.prata_pro_moradia_historico_contrato') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_pro_moradia_historico_contrato';
    end if;
    if to_regclass('prata.prata_hist_pro_moradia_contrato') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_pro_moradia_contrato';
    end if;
    if to_regclass('prata.prata_pro_moradia_historico_desembolso_mensal') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_pro_moradia_historico_desembolso_mensal';
    end if;
    if to_regclass('prata.prata_hist_pro_moradia_desembolso_mensal') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_pro_moradia_desembolso_mensal';
    end if;
    if to_regclass('prata.prata_pro_moradia_historico_paralisacao') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_pro_moradia_historico_paralisacao';
    end if;
    if to_regclass('prata.prata_hist_pro_moradia_paralisacao') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_pro_moradia_paralisacao';
    end if;
    if to_regclass('prata.prata_reforma_casa_brasil_historico_contrato') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_reforma_casa_brasil_historico_contrato';
    end if;
    if to_regclass('prata.prata_hist_reforma_casa_brasil_contrato') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_reforma_casa_brasil_contrato';
    end if;
    if to_regclass('prata.prata_rural_historico_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_rural_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_hist_rural_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_rural_empreendimento';
    end if;
    if to_regclass('ouro.ouro_historico_marco_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_historico_marco_empreendimento';
    end if;
    if to_regclass('ouro.ouro_hist_marco_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_hist_marco_empreendimento';
    end if;
    if to_regclass('ouro.ouro_historico_pro_moradia_evolucao_financeira') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_historico_pro_moradia_evolucao_financeira';
    end if;
    if to_regclass('ouro.ouro_hist_pro_moradia_evolucao_financeira') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_hist_pro_moradia_evolucao_financeira';
    end if;
    if to_regclass('ouro.ouro_historico_pro_moradia_paralisacao') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_historico_pro_moradia_paralisacao';
    end if;
    if to_regclass('ouro.ouro_hist_pro_moradia_paralisacao') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_hist_pro_moradia_paralisacao';
    end if;
    if to_regclass('ouro.ouro_historico_serie_mensal_frentes_novas') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_historico_serie_mensal_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_hist_serie_mensal_frentes_novas') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_hist_serie_mensal_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_historico_serie_mensal') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_historico_serie_mensal';
    end if;
    if to_regclass('ouro.ouro_hist_serie_mensal') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_hist_serie_mensal';
    end if;
    if to_regclass('ouro.ouro_historico_serie_situacao_mensal') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_historico_serie_situacao_mensal';
    end if;
    if to_regclass('ouro.ouro_hist_serie_situacao_mensal') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_hist_serie_situacao_mensal';
    end if;
    if to_regclass('ouro.ouro_historico_snapshot_empreendimento_atual') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_historico_snapshot_empreendimento_atual';
    end if;
    if to_regclass('ouro.ouro_hist_snapshot_empreendimento_atual') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_hist_snapshot_empreendimento_atual';
    end if;
    if to_regclass('prata.prata_historico_snh_apf_mes') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_historico_snh_apf_mes';
    end if;
    if to_regclass('prata.prata_hist_snh_apf_mes') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_snh_apf_mes';
    end if;
    if to_regclass('prata.prata_historico_snh_entregas_mes') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_historico_snh_entregas_mes';
    end if;
    if to_regclass('prata.prata_hist_snh_entregas_mes') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_hist_snh_entregas_mes';
    end if;
    if to_regclass('prata.prata_reloginho_classe_media_reforma_resumo_faixa_renda_mes') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_reloginho_classe_media_reforma_resumo_faixa_renda_mes';
    end if;
    if to_regclass('prata.prata_relog_classe_media_reforma_resumo_faixa_renda_mes') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_relog_classe_media_reforma_resumo_faixa_renda_mes';
    end if;
    if to_regclass('prata.prata_reloginho_frentes_novas_resumo_mes_uf') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_reloginho_frentes_novas_resumo_mes_uf';
    end if;
    if to_regclass('prata.prata_relog_frentes_novas_resumo_mes_uf') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_relog_frentes_novas_resumo_mes_uf';
    end if;
    if to_regclass('prata.prata_reloginho_frentes_novas_resumo_mes') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_reloginho_frentes_novas_resumo_mes';
    end if;
    if to_regclass('prata.prata_relog_frentes_novas_resumo_mes') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_relog_frentes_novas_resumo_mes';
    end if;
    if to_regclass('ouro.ouro_reloginho_classe_media_reforma_indicadores_faixa_renda') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_classe_media_reforma_indicadores_faixa_renda';
    end if;
    if to_regclass('ouro.ouro_relog_classe_media_reforma_indicadores_faixa_renda') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_relog_classe_media_reforma_indicadores_faixa_renda';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_entregas') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_indicadores_entregas';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores_entregas') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_relog_indicadores_entregas';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_frentes_novas_regiao') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_indicadores_frentes_novas_regiao';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores_frentes_novas_regiao') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_relog_indicadores_frentes_novas_regiao';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_frentes_novas') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_indicadores_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores_frentes_novas') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_relog_indicadores_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_frente') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_indicadores_frente';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores_frente') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_relog_indicadores_frente';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_gargalo_desempenho') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores_gargalo_desempenho') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_relog_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_indicadores';
    end if;
    if to_regclass('ouro.ouro_relog_indicadores') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_relog_indicadores';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_dashboard_frentes_novas') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_resumo_dashboard_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_relog_resumo_dashboard_frentes_novas') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_relog_resumo_dashboard_frentes_novas';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_dashboard') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_resumo_dashboard';
    end if;
    if to_regclass('ouro.ouro_relog_resumo_dashboard') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_relog_resumo_dashboard';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard';
    end if;
    if to_regclass('ouro.ouro_relog_resumo_gargalo_desempenho_dashboard') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_relog_resumo_gargalo_desempenho_dashboard';
    end if;
end $$;

commit;
