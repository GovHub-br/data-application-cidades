-- =====================================================================
-- reverter_nomenclatura_prod.sql — ROLLBACK: bronze/prata/ouro -> schemas antigos
--
-- GERADO por scripts/migracao/gerar_migracao.py a partir de mapa_nomenclatura.csv.
-- NÃO EDITAR À MÃO — regenerar com o script.
--
-- ATENÇÃO: script MANUAL, roda contra o Postgres `prod`. NÃO deve ser chamado
-- por publicar_historico.py, publicar-historico.sh, run-*.sh, rebuild-local.sh
-- nem por hooks on-run-* do dbt. Renomeia METADADO (ALTER TABLE) — instantâneo,
-- não copia dados. Consumidores que referenciam as tabelas por string de nome
-- (Superset / OpenMetadata / notebooks) quebram: inventariar antes.
--
-- Uso:
--   psql "$DSN" -v ON_ERROR_STOP=1 -f scripts/migracao/reverter_nomenclatura_prod.sql
-- (ordem e pré-requisitos: ver README.md)
-- =====================================================================

\set ON_ERROR_STOP on

begin;

-- 1) preflight -------------------------------------------------------
do $$
begin
    -- preflight: 29 tabelas no inventario
    if to_regclass('bronze.bronze_dhist_serie_entrada_bb') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_dhist_serie_entrada_bb';
    end if;
    if to_regclass('bronze.bronze_dhist_empreendimento_snh_bb') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_dhist_empreendimento_snh_bb';
    end if;
    if to_regclass('bronze.bronze_dhist_snh_entregas_evento_bb') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_dhist_snh_entregas_evento_bb';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int054') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_sftp_empreendimento_int054';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int059') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_sftp_empreendimento_int059';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int057') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_sftp_empreendimento_int057';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int040') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_sftp_empreendimento_int040';
    end if;
    if to_regclass('bronze.bronze_dhist_snh_entregas_evento_caixa') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_dhist_snh_entregas_evento_caixa';
    end if;
    if to_regclass('bronze.bronze_dhist_empreendimento_snh_caixa') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_dhist_empreendimento_snh_caixa';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int065') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_sftp_empreendimento_int065';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_bases_relatorio_executivo') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_dhist_serie_bases_relatorio_executivo';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_min_cidades') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_dhist_serie_min_cidades';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_bext') is null then
        raise exception 'origem ausente: %', 'bronze.bronze_dhist_serie_bext';
    end if;
    if to_regclass('prata.prata_dhist_snh_entregas_mes') is null then
        raise exception 'origem ausente: %', 'prata.prata_dhist_snh_entregas_mes';
    end if;
    if to_regclass('prata.prata_dhist_snh_apf_mes') is null then
        raise exception 'origem ausente: %', 'prata.prata_dhist_snh_apf_mes';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_reloginho_indicadores';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_frente') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_reloginho_indicadores_frente';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_entregas') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_reloginho_indicadores_entregas';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_dashboard') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_reloginho_resumo_dashboard';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_gargalo_desempenho') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_reloginho_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard';
    end if;
    if to_regclass('prata.prata_fds_historico_empreendimento') is null then
        raise exception 'origem ausente: %', 'prata.prata_fds_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_far_historico_empreendimento') is null then
        raise exception 'origem ausente: %', 'prata.prata_far_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_rural_historico_empreendimento') is null then
        raise exception 'origem ausente: %', 'prata.prata_rural_historico_empreendimento';
    end if;
    if to_regclass('ouro.ouro_dhist_serie_mensal') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_dhist_serie_mensal';
    end if;
    if to_regclass('ouro.ouro_dhist_snapshot_empreendimento_atual') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_dhist_snapshot_empreendimento_atual';
    end if;
    if to_regclass('ouro.ouro_dhist_marco_empreendimento') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_dhist_marco_empreendimento';
    end if;
    if to_regclass('ouro.ouro_dhist_serie_situacao_mensal') is null then
        raise exception 'origem ausente: %', 'ouro.ouro_dhist_serie_situacao_mensal';
    end if;
    if to_regclass('prata.prata_dhist_serie_executiva') is null then
        raise exception 'origem ausente: %', 'prata.prata_dhist_serie_executiva';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_entrada_bb') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.bronze_mcmv_historico_serie_entrada_bb';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_snh_bb') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_snh_bb';
    end if;
    if to_regclass('reloginho.bronze_reloginho_snh_entregas_evento_bb') is not null then
        raise exception 'destino ja existe: %', 'reloginho.bronze_reloginho_snh_entregas_evento_bb';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int054') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int054';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int059') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int059';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int057') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int057';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int040') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int040';
    end if;
    if to_regclass('reloginho.bronze_reloginho_snh_entregas_evento_caixa') is not null then
        raise exception 'destino ja existe: %', 'reloginho.bronze_reloginho_snh_entregas_evento_caixa';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_snh_caixa') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_snh_caixa';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int065') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int065';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_bases_relatorio_executivo') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.bronze_mcmv_historico_serie_bases_relatorio_executivo';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_min_cidades') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.bronze_mcmv_historico_serie_min_cidades';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_bext') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.bronze_mcmv_historico_serie_bext';
    end if;
    if to_regclass('reloginho.silver_historico_snh_entregas_mes') is not null then
        raise exception 'destino ja existe: %', 'reloginho.silver_historico_snh_entregas_mes';
    end if;
    if to_regclass('reloginho.silver_historico_snh_apf_mes') is not null then
        raise exception 'destino ja existe: %', 'reloginho.silver_historico_snh_apf_mes';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho') is not null then
        raise exception 'destino ja existe: %', 'reloginho.gold_indicadores_reloginho';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho_frente') is not null then
        raise exception 'destino ja existe: %', 'reloginho.gold_indicadores_reloginho_frente';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho_entregas') is not null then
        raise exception 'destino ja existe: %', 'reloginho.gold_indicadores_reloginho_entregas';
    end if;
    if to_regclass('reloginho.gold_resumo_reloginho_dashboard') is not null then
        raise exception 'destino ja existe: %', 'reloginho.gold_resumo_reloginho_dashboard';
    end if;
    if to_regclass('reloginho.gold_indicadores_gargalo_desempenho') is not null then
        raise exception 'destino ja existe: %', 'reloginho.gold_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('reloginho.gold_resumo_gargalo_desempenho_dashboard') is not null then
        raise exception 'destino ja existe: %', 'reloginho.gold_resumo_gargalo_desempenho_dashboard';
    end if;
    if to_regclass('empreendimentos_fds.silver_historico_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'empreendimentos_fds.silver_historico_empreendimento';
    end if;
    if to_regclass('empreendimento_far.silver_historico_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'empreendimento_far.silver_historico_empreendimento';
    end if;
    if to_regclass('empreendimento_rural.silver_historico_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'empreendimento_rural.silver_historico_empreendimento';
    end if;
    if to_regclass('dados_historicos.gold_serie_mensal') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.gold_serie_mensal';
    end if;
    if to_regclass('dados_historicos.gold_snapshot_empreendimento_atual') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.gold_snapshot_empreendimento_atual';
    end if;
    if to_regclass('dados_historicos.gold_marco_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.gold_marco_empreendimento';
    end if;
    if to_regclass('dados_historicos.gold_serie_situacao_mensal') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.gold_serie_situacao_mensal';
    end if;
    if to_regclass('dados_historicos.silver_mcmv_historico_serie_executiva') is not null then
        raise exception 'destino ja existe: %', 'dados_historicos.silver_mcmv_historico_serie_executiva';
    end if;
end $$;

-- 2) renomeia + move schema (rename ANTES de set schema) ------------
alter table "bronze"."bronze_dhist_serie_entrada_bb" rename to "bronze_mcmv_historico_serie_entrada_bb";
alter table "bronze"."bronze_mcmv_historico_serie_entrada_bb" set schema "dados_historicos";
alter table "bronze"."bronze_dhist_empreendimento_snh_bb" rename to "bronze_mcmv_historico_empreendimento_snh_bb";
alter table "bronze"."bronze_mcmv_historico_empreendimento_snh_bb" set schema "dados_historicos";
alter table "bronze"."bronze_dhist_snh_entregas_evento_bb" rename to "bronze_reloginho_snh_entregas_evento_bb";
alter table "bronze"."bronze_reloginho_snh_entregas_evento_bb" set schema "reloginho";
alter table "bronze"."bronze_sftp_empreendimento_int054" rename to "bronze_mcmv_historico_empreendimento_int054";
alter table "bronze"."bronze_mcmv_historico_empreendimento_int054" set schema "dados_historicos";
alter table "bronze"."bronze_sftp_empreendimento_int059" rename to "bronze_mcmv_historico_empreendimento_int059";
alter table "bronze"."bronze_mcmv_historico_empreendimento_int059" set schema "dados_historicos";
alter table "bronze"."bronze_sftp_empreendimento_int057" rename to "bronze_mcmv_historico_empreendimento_int057";
alter table "bronze"."bronze_mcmv_historico_empreendimento_int057" set schema "dados_historicos";
alter table "bronze"."bronze_sftp_empreendimento_int040" rename to "bronze_mcmv_historico_empreendimento_int040";
alter table "bronze"."bronze_mcmv_historico_empreendimento_int040" set schema "dados_historicos";
alter table "bronze"."bronze_dhist_snh_entregas_evento_caixa" rename to "bronze_reloginho_snh_entregas_evento_caixa";
alter table "bronze"."bronze_reloginho_snh_entregas_evento_caixa" set schema "reloginho";
alter table "bronze"."bronze_dhist_empreendimento_snh_caixa" rename to "bronze_mcmv_historico_empreendimento_snh_caixa";
alter table "bronze"."bronze_mcmv_historico_empreendimento_snh_caixa" set schema "dados_historicos";
alter table "bronze"."bronze_sftp_empreendimento_int065" rename to "bronze_mcmv_historico_empreendimento_int065";
alter table "bronze"."bronze_mcmv_historico_empreendimento_int065" set schema "dados_historicos";
alter table "bronze"."bronze_dhist_serie_bases_relatorio_executivo" rename to "bronze_mcmv_historico_serie_bases_relatorio_executivo";
alter table "bronze"."bronze_mcmv_historico_serie_bases_relatorio_executivo" set schema "dados_historicos";
alter table "bronze"."bronze_dhist_serie_min_cidades" rename to "bronze_mcmv_historico_serie_min_cidades";
alter table "bronze"."bronze_mcmv_historico_serie_min_cidades" set schema "dados_historicos";
alter table "bronze"."bronze_dhist_serie_bext" rename to "bronze_mcmv_historico_serie_bext";
alter table "bronze"."bronze_mcmv_historico_serie_bext" set schema "dados_historicos";
alter table "prata"."prata_dhist_snh_entregas_mes" rename to "silver_historico_snh_entregas_mes";
alter table "prata"."silver_historico_snh_entregas_mes" set schema "reloginho";
alter table "prata"."prata_dhist_snh_apf_mes" rename to "silver_historico_snh_apf_mes";
alter table "prata"."silver_historico_snh_apf_mes" set schema "reloginho";
alter table "ouro"."ouro_reloginho_indicadores" rename to "gold_indicadores_reloginho";
alter table "ouro"."gold_indicadores_reloginho" set schema "reloginho";
alter table "ouro"."ouro_reloginho_indicadores_frente" rename to "gold_indicadores_reloginho_frente";
alter table "ouro"."gold_indicadores_reloginho_frente" set schema "reloginho";
alter table "ouro"."ouro_reloginho_indicadores_entregas" rename to "gold_indicadores_reloginho_entregas";
alter table "ouro"."gold_indicadores_reloginho_entregas" set schema "reloginho";
alter table "ouro"."ouro_reloginho_resumo_dashboard" rename to "gold_resumo_reloginho_dashboard";
alter table "ouro"."gold_resumo_reloginho_dashboard" set schema "reloginho";
alter table "ouro"."ouro_reloginho_indicadores_gargalo_desempenho" rename to "gold_indicadores_gargalo_desempenho";
alter table "ouro"."gold_indicadores_gargalo_desempenho" set schema "reloginho";
alter table "ouro"."ouro_reloginho_resumo_gargalo_desempenho_dashboard" rename to "gold_resumo_gargalo_desempenho_dashboard";
alter table "ouro"."gold_resumo_gargalo_desempenho_dashboard" set schema "reloginho";
alter table "prata"."prata_fds_historico_empreendimento" rename to "silver_historico_empreendimento";
alter table "prata"."silver_historico_empreendimento" set schema "empreendimentos_fds";
alter table "prata"."prata_far_historico_empreendimento" rename to "silver_historico_empreendimento";
alter table "prata"."silver_historico_empreendimento" set schema "empreendimento_far";
alter table "prata"."prata_rural_historico_empreendimento" rename to "silver_historico_empreendimento";
alter table "prata"."silver_historico_empreendimento" set schema "empreendimento_rural";
alter table "ouro"."ouro_dhist_serie_mensal" rename to "gold_serie_mensal";
alter table "ouro"."gold_serie_mensal" set schema "dados_historicos";
alter table "ouro"."ouro_dhist_snapshot_empreendimento_atual" rename to "gold_snapshot_empreendimento_atual";
alter table "ouro"."gold_snapshot_empreendimento_atual" set schema "dados_historicos";
alter table "ouro"."ouro_dhist_marco_empreendimento" rename to "gold_marco_empreendimento";
alter table "ouro"."gold_marco_empreendimento" set schema "dados_historicos";
alter table "ouro"."ouro_dhist_serie_situacao_mensal" rename to "gold_serie_situacao_mensal";
alter table "ouro"."gold_serie_situacao_mensal" set schema "dados_historicos";
alter table "prata"."prata_dhist_serie_executiva" rename to "silver_mcmv_historico_serie_executiva";
alter table "prata"."silver_mcmv_historico_serie_executiva" set schema "dados_historicos";

-- 3) postflight -----------------------------------------------------
do $$
begin
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_entrada_bb') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.bronze_mcmv_historico_serie_entrada_bb';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_entrada_bb') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_dhist_serie_entrada_bb';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_snh_bb') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.bronze_mcmv_historico_empreendimento_snh_bb';
    end if;
    if to_regclass('bronze.bronze_dhist_empreendimento_snh_bb') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_dhist_empreendimento_snh_bb';
    end if;
    if to_regclass('reloginho.bronze_reloginho_snh_entregas_evento_bb') is null then
        raise exception 'postflight: destino ausente %', 'reloginho.bronze_reloginho_snh_entregas_evento_bb';
    end if;
    if to_regclass('bronze.bronze_dhist_snh_entregas_evento_bb') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_dhist_snh_entregas_evento_bb';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int054') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int054';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int054') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_sftp_empreendimento_int054';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int059') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int059';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int059') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_sftp_empreendimento_int059';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int057') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int057';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int057') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_sftp_empreendimento_int057';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int040') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int040';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int040') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_sftp_empreendimento_int040';
    end if;
    if to_regclass('reloginho.bronze_reloginho_snh_entregas_evento_caixa') is null then
        raise exception 'postflight: destino ausente %', 'reloginho.bronze_reloginho_snh_entregas_evento_caixa';
    end if;
    if to_regclass('bronze.bronze_dhist_snh_entregas_evento_caixa') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_dhist_snh_entregas_evento_caixa';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_snh_caixa') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.bronze_mcmv_historico_empreendimento_snh_caixa';
    end if;
    if to_regclass('bronze.bronze_dhist_empreendimento_snh_caixa') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_dhist_empreendimento_snh_caixa';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int065') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int065';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int065') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_sftp_empreendimento_int065';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_bases_relatorio_executivo') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.bronze_mcmv_historico_serie_bases_relatorio_executivo';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_bases_relatorio_executivo') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_dhist_serie_bases_relatorio_executivo';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_min_cidades') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.bronze_mcmv_historico_serie_min_cidades';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_min_cidades') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_dhist_serie_min_cidades';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_bext') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.bronze_mcmv_historico_serie_bext';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_bext') is not null then
        raise exception 'postflight: origem ainda existe %', 'bronze.bronze_dhist_serie_bext';
    end if;
    if to_regclass('reloginho.silver_historico_snh_entregas_mes') is null then
        raise exception 'postflight: destino ausente %', 'reloginho.silver_historico_snh_entregas_mes';
    end if;
    if to_regclass('prata.prata_dhist_snh_entregas_mes') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_dhist_snh_entregas_mes';
    end if;
    if to_regclass('reloginho.silver_historico_snh_apf_mes') is null then
        raise exception 'postflight: destino ausente %', 'reloginho.silver_historico_snh_apf_mes';
    end if;
    if to_regclass('prata.prata_dhist_snh_apf_mes') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_dhist_snh_apf_mes';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho') is null then
        raise exception 'postflight: destino ausente %', 'reloginho.gold_indicadores_reloginho';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_reloginho_indicadores';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho_frente') is null then
        raise exception 'postflight: destino ausente %', 'reloginho.gold_indicadores_reloginho_frente';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_frente') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_reloginho_indicadores_frente';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho_entregas') is null then
        raise exception 'postflight: destino ausente %', 'reloginho.gold_indicadores_reloginho_entregas';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_entregas') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_reloginho_indicadores_entregas';
    end if;
    if to_regclass('reloginho.gold_resumo_reloginho_dashboard') is null then
        raise exception 'postflight: destino ausente %', 'reloginho.gold_resumo_reloginho_dashboard';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_dashboard') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_reloginho_resumo_dashboard';
    end if;
    if to_regclass('reloginho.gold_indicadores_gargalo_desempenho') is null then
        raise exception 'postflight: destino ausente %', 'reloginho.gold_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_gargalo_desempenho') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_reloginho_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('reloginho.gold_resumo_gargalo_desempenho_dashboard') is null then
        raise exception 'postflight: destino ausente %', 'reloginho.gold_resumo_gargalo_desempenho_dashboard';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard';
    end if;
    if to_regclass('empreendimentos_fds.silver_historico_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'empreendimentos_fds.silver_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_fds_historico_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_fds_historico_empreendimento';
    end if;
    if to_regclass('empreendimento_far.silver_historico_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'empreendimento_far.silver_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_far_historico_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_far_historico_empreendimento';
    end if;
    if to_regclass('empreendimento_rural.silver_historico_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'empreendimento_rural.silver_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_rural_historico_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_rural_historico_empreendimento';
    end if;
    if to_regclass('dados_historicos.gold_serie_mensal') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.gold_serie_mensal';
    end if;
    if to_regclass('ouro.ouro_dhist_serie_mensal') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_dhist_serie_mensal';
    end if;
    if to_regclass('dados_historicos.gold_snapshot_empreendimento_atual') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.gold_snapshot_empreendimento_atual';
    end if;
    if to_regclass('ouro.ouro_dhist_snapshot_empreendimento_atual') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_dhist_snapshot_empreendimento_atual';
    end if;
    if to_regclass('dados_historicos.gold_marco_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.gold_marco_empreendimento';
    end if;
    if to_regclass('ouro.ouro_dhist_marco_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_dhist_marco_empreendimento';
    end if;
    if to_regclass('dados_historicos.gold_serie_situacao_mensal') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.gold_serie_situacao_mensal';
    end if;
    if to_regclass('ouro.ouro_dhist_serie_situacao_mensal') is not null then
        raise exception 'postflight: origem ainda existe %', 'ouro.ouro_dhist_serie_situacao_mensal';
    end if;
    if to_regclass('dados_historicos.silver_mcmv_historico_serie_executiva') is null then
        raise exception 'postflight: destino ausente %', 'dados_historicos.silver_mcmv_historico_serie_executiva';
    end if;
    if to_regclass('prata.prata_dhist_serie_executiva') is not null then
        raise exception 'postflight: origem ainda existe %', 'prata.prata_dhist_serie_executiva';
    end if;
end $$;

commit;
