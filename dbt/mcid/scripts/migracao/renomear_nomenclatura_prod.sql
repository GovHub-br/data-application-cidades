-- =====================================================================
-- renomear_nomenclatura_prod.sql — schemas antigos -> bronze/prata/ouro
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
--   psql "$DSN" -v ON_ERROR_STOP=1 -f scripts/migracao/renomear_nomenclatura_prod.sql
-- (ordem e pré-requisitos: ver README.md)
-- =====================================================================

\set ON_ERROR_STOP on

begin;

-- 1) preflight -------------------------------------------------------
do $$
begin
    -- preflight: 29 tabelas no inventario
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_entrada_bb') is null then
        raise exception 'origem ausente: %', 'dados_historicos.bronze_mcmv_historico_serie_entrada_bb';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_snh_bb') is null then
        raise exception 'origem ausente: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_snh_bb';
    end if;
    if to_regclass('reloginho.bronze_reloginho_snh_entregas_evento_bb') is null then
        raise exception 'origem ausente: %', 'reloginho.bronze_reloginho_snh_entregas_evento_bb';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int054') is null then
        raise exception 'origem ausente: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int054';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int059') is null then
        raise exception 'origem ausente: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int059';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int057') is null then
        raise exception 'origem ausente: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int057';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int040') is null then
        raise exception 'origem ausente: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int040';
    end if;
    if to_regclass('reloginho.bronze_reloginho_snh_entregas_evento_caixa') is null then
        raise exception 'origem ausente: %', 'reloginho.bronze_reloginho_snh_entregas_evento_caixa';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_snh_caixa') is null then
        raise exception 'origem ausente: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_snh_caixa';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int065') is null then
        raise exception 'origem ausente: %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int065';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_bases_relatorio_executivo') is null then
        raise exception 'origem ausente: %', 'dados_historicos.bronze_mcmv_historico_serie_bases_relatorio_executivo';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_min_cidades') is null then
        raise exception 'origem ausente: %', 'dados_historicos.bronze_mcmv_historico_serie_min_cidades';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_bext') is null then
        raise exception 'origem ausente: %', 'dados_historicos.bronze_mcmv_historico_serie_bext';
    end if;
    if to_regclass('reloginho.silver_historico_snh_entregas_mes') is null then
        raise exception 'origem ausente: %', 'reloginho.silver_historico_snh_entregas_mes';
    end if;
    if to_regclass('reloginho.silver_historico_snh_apf_mes') is null then
        raise exception 'origem ausente: %', 'reloginho.silver_historico_snh_apf_mes';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho') is null then
        raise exception 'origem ausente: %', 'reloginho.gold_indicadores_reloginho';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho_frente') is null then
        raise exception 'origem ausente: %', 'reloginho.gold_indicadores_reloginho_frente';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho_entregas') is null then
        raise exception 'origem ausente: %', 'reloginho.gold_indicadores_reloginho_entregas';
    end if;
    if to_regclass('reloginho.gold_resumo_reloginho_dashboard') is null then
        raise exception 'origem ausente: %', 'reloginho.gold_resumo_reloginho_dashboard';
    end if;
    if to_regclass('reloginho.gold_indicadores_gargalo_desempenho') is null then
        raise exception 'origem ausente: %', 'reloginho.gold_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('reloginho.gold_resumo_gargalo_desempenho_dashboard') is null then
        raise exception 'origem ausente: %', 'reloginho.gold_resumo_gargalo_desempenho_dashboard';
    end if;
    if to_regclass('empreendimentos_fds.silver_historico_empreendimento') is null then
        raise exception 'origem ausente: %', 'empreendimentos_fds.silver_historico_empreendimento';
    end if;
    if to_regclass('empreendimento_far.silver_historico_empreendimento') is null then
        raise exception 'origem ausente: %', 'empreendimento_far.silver_historico_empreendimento';
    end if;
    if to_regclass('empreendimento_rural.silver_historico_empreendimento') is null then
        raise exception 'origem ausente: %', 'empreendimento_rural.silver_historico_empreendimento';
    end if;
    if to_regclass('dados_historicos.gold_serie_mensal') is null then
        raise exception 'origem ausente: %', 'dados_historicos.gold_serie_mensal';
    end if;
    if to_regclass('dados_historicos.gold_snapshot_empreendimento_atual') is null then
        raise exception 'origem ausente: %', 'dados_historicos.gold_snapshot_empreendimento_atual';
    end if;
    if to_regclass('dados_historicos.gold_marco_empreendimento') is null then
        raise exception 'origem ausente: %', 'dados_historicos.gold_marco_empreendimento';
    end if;
    if to_regclass('dados_historicos.gold_serie_situacao_mensal') is null then
        raise exception 'origem ausente: %', 'dados_historicos.gold_serie_situacao_mensal';
    end if;
    if to_regclass('dados_historicos.silver_mcmv_historico_serie_executiva') is null then
        raise exception 'origem ausente: %', 'dados_historicos.silver_mcmv_historico_serie_executiva';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_entrada_bb') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_dhist_serie_entrada_bb';
    end if;
    if to_regclass('bronze.bronze_dhist_empreendimento_snh_bb') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_dhist_empreendimento_snh_bb';
    end if;
    if to_regclass('bronze.bronze_dhist_snh_entregas_evento_bb') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_dhist_snh_entregas_evento_bb';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int054') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_sftp_empreendimento_int054';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int059') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_sftp_empreendimento_int059';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int057') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_sftp_empreendimento_int057';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int040') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_sftp_empreendimento_int040';
    end if;
    if to_regclass('bronze.bronze_dhist_snh_entregas_evento_caixa') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_dhist_snh_entregas_evento_caixa';
    end if;
    if to_regclass('bronze.bronze_dhist_empreendimento_snh_caixa') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_dhist_empreendimento_snh_caixa';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int065') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_sftp_empreendimento_int065';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_bases_relatorio_executivo') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_dhist_serie_bases_relatorio_executivo';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_min_cidades') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_dhist_serie_min_cidades';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_bext') is not null then
        raise exception 'destino ja existe: %', 'bronze.bronze_dhist_serie_bext';
    end if;
    if to_regclass('prata.prata_dhist_snh_entregas_mes') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_dhist_snh_entregas_mes';
    end if;
    if to_regclass('prata.prata_dhist_snh_apf_mes') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_dhist_snh_apf_mes';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_indicadores';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_frente') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_indicadores_frente';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_entregas') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_indicadores_entregas';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_dashboard') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_resumo_dashboard';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_gargalo_desempenho') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard';
    end if;
    if to_regclass('prata.prata_fds_historico_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_fds_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_far_historico_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_far_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_rural_historico_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_rural_historico_empreendimento';
    end if;
    if to_regclass('ouro.ouro_dhist_serie_mensal') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_dhist_serie_mensal';
    end if;
    if to_regclass('ouro.ouro_dhist_snapshot_empreendimento_atual') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_dhist_snapshot_empreendimento_atual';
    end if;
    if to_regclass('ouro.ouro_dhist_marco_empreendimento') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_dhist_marco_empreendimento';
    end if;
    if to_regclass('ouro.ouro_dhist_serie_situacao_mensal') is not null then
        raise exception 'destino ja existe: %', 'ouro.ouro_dhist_serie_situacao_mensal';
    end if;
    if to_regclass('prata.prata_dhist_serie_executiva') is not null then
        raise exception 'destino ja existe: %', 'prata.prata_dhist_serie_executiva';
    end if;
end $$;

-- 2) renomeia + move schema (rename ANTES de set schema) ------------
alter table "dados_historicos"."bronze_mcmv_historico_serie_entrada_bb" rename to "bronze_dhist_serie_entrada_bb";
alter table "dados_historicos"."bronze_dhist_serie_entrada_bb" set schema "bronze";
alter table "dados_historicos"."bronze_mcmv_historico_empreendimento_snh_bb" rename to "bronze_dhist_empreendimento_snh_bb";
alter table "dados_historicos"."bronze_dhist_empreendimento_snh_bb" set schema "bronze";
alter table "reloginho"."bronze_reloginho_snh_entregas_evento_bb" rename to "bronze_dhist_snh_entregas_evento_bb";
alter table "reloginho"."bronze_dhist_snh_entregas_evento_bb" set schema "bronze";
alter table "dados_historicos"."bronze_mcmv_historico_empreendimento_int054" rename to "bronze_sftp_empreendimento_int054";
alter table "dados_historicos"."bronze_sftp_empreendimento_int054" set schema "bronze";
alter table "dados_historicos"."bronze_mcmv_historico_empreendimento_int059" rename to "bronze_sftp_empreendimento_int059";
alter table "dados_historicos"."bronze_sftp_empreendimento_int059" set schema "bronze";
alter table "dados_historicos"."bronze_mcmv_historico_empreendimento_int057" rename to "bronze_sftp_empreendimento_int057";
alter table "dados_historicos"."bronze_sftp_empreendimento_int057" set schema "bronze";
alter table "dados_historicos"."bronze_mcmv_historico_empreendimento_int040" rename to "bronze_sftp_empreendimento_int040";
alter table "dados_historicos"."bronze_sftp_empreendimento_int040" set schema "bronze";
alter table "reloginho"."bronze_reloginho_snh_entregas_evento_caixa" rename to "bronze_dhist_snh_entregas_evento_caixa";
alter table "reloginho"."bronze_dhist_snh_entregas_evento_caixa" set schema "bronze";
alter table "dados_historicos"."bronze_mcmv_historico_empreendimento_snh_caixa" rename to "bronze_dhist_empreendimento_snh_caixa";
alter table "dados_historicos"."bronze_dhist_empreendimento_snh_caixa" set schema "bronze";
alter table "dados_historicos"."bronze_mcmv_historico_empreendimento_int065" rename to "bronze_sftp_empreendimento_int065";
alter table "dados_historicos"."bronze_sftp_empreendimento_int065" set schema "bronze";
alter table "dados_historicos"."bronze_mcmv_historico_serie_bases_relatorio_executivo" rename to "bronze_dhist_serie_bases_relatorio_executivo";
alter table "dados_historicos"."bronze_dhist_serie_bases_relatorio_executivo" set schema "bronze";
alter table "dados_historicos"."bronze_mcmv_historico_serie_min_cidades" rename to "bronze_dhist_serie_min_cidades";
alter table "dados_historicos"."bronze_dhist_serie_min_cidades" set schema "bronze";
alter table "dados_historicos"."bronze_mcmv_historico_serie_bext" rename to "bronze_dhist_serie_bext";
alter table "dados_historicos"."bronze_dhist_serie_bext" set schema "bronze";
alter table "reloginho"."silver_historico_snh_entregas_mes" rename to "prata_dhist_snh_entregas_mes";
alter table "reloginho"."prata_dhist_snh_entregas_mes" set schema "prata";
alter table "reloginho"."silver_historico_snh_apf_mes" rename to "prata_dhist_snh_apf_mes";
alter table "reloginho"."prata_dhist_snh_apf_mes" set schema "prata";
alter table "reloginho"."gold_indicadores_reloginho" rename to "ouro_reloginho_indicadores";
alter table "reloginho"."ouro_reloginho_indicadores" set schema "ouro";
alter table "reloginho"."gold_indicadores_reloginho_frente" rename to "ouro_reloginho_indicadores_frente";
alter table "reloginho"."ouro_reloginho_indicadores_frente" set schema "ouro";
alter table "reloginho"."gold_indicadores_reloginho_entregas" rename to "ouro_reloginho_indicadores_entregas";
alter table "reloginho"."ouro_reloginho_indicadores_entregas" set schema "ouro";
alter table "reloginho"."gold_resumo_reloginho_dashboard" rename to "ouro_reloginho_resumo_dashboard";
alter table "reloginho"."ouro_reloginho_resumo_dashboard" set schema "ouro";
alter table "reloginho"."gold_indicadores_gargalo_desempenho" rename to "ouro_reloginho_indicadores_gargalo_desempenho";
alter table "reloginho"."ouro_reloginho_indicadores_gargalo_desempenho" set schema "ouro";
alter table "reloginho"."gold_resumo_gargalo_desempenho_dashboard" rename to "ouro_reloginho_resumo_gargalo_desempenho_dashboard";
alter table "reloginho"."ouro_reloginho_resumo_gargalo_desempenho_dashboard" set schema "ouro";
alter table "empreendimentos_fds"."silver_historico_empreendimento" rename to "prata_fds_historico_empreendimento";
alter table "empreendimentos_fds"."prata_fds_historico_empreendimento" set schema "prata";
alter table "empreendimento_far"."silver_historico_empreendimento" rename to "prata_far_historico_empreendimento";
alter table "empreendimento_far"."prata_far_historico_empreendimento" set schema "prata";
alter table "empreendimento_rural"."silver_historico_empreendimento" rename to "prata_rural_historico_empreendimento";
alter table "empreendimento_rural"."prata_rural_historico_empreendimento" set schema "prata";
alter table "dados_historicos"."gold_serie_mensal" rename to "ouro_dhist_serie_mensal";
alter table "dados_historicos"."ouro_dhist_serie_mensal" set schema "ouro";
alter table "dados_historicos"."gold_snapshot_empreendimento_atual" rename to "ouro_dhist_snapshot_empreendimento_atual";
alter table "dados_historicos"."ouro_dhist_snapshot_empreendimento_atual" set schema "ouro";
alter table "dados_historicos"."gold_marco_empreendimento" rename to "ouro_dhist_marco_empreendimento";
alter table "dados_historicos"."ouro_dhist_marco_empreendimento" set schema "ouro";
alter table "dados_historicos"."gold_serie_situacao_mensal" rename to "ouro_dhist_serie_situacao_mensal";
alter table "dados_historicos"."ouro_dhist_serie_situacao_mensal" set schema "ouro";
alter table "dados_historicos"."silver_mcmv_historico_serie_executiva" rename to "prata_dhist_serie_executiva";
alter table "dados_historicos"."prata_dhist_serie_executiva" set schema "prata";

-- 3) postflight -----------------------------------------------------
do $$
begin
    if to_regclass('bronze.bronze_dhist_serie_entrada_bb') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_dhist_serie_entrada_bb';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_entrada_bb') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.bronze_mcmv_historico_serie_entrada_bb';
    end if;
    if to_regclass('bronze.bronze_dhist_empreendimento_snh_bb') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_dhist_empreendimento_snh_bb';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_snh_bb') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.bronze_mcmv_historico_empreendimento_snh_bb';
    end if;
    if to_regclass('bronze.bronze_dhist_snh_entregas_evento_bb') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_dhist_snh_entregas_evento_bb';
    end if;
    if to_regclass('reloginho.bronze_reloginho_snh_entregas_evento_bb') is not null then
        raise exception 'postflight: origem ainda existe %', 'reloginho.bronze_reloginho_snh_entregas_evento_bb';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int054') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_sftp_empreendimento_int054';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int054') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int054';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int059') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_sftp_empreendimento_int059';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int059') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int059';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int057') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_sftp_empreendimento_int057';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int057') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int057';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int040') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_sftp_empreendimento_int040';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int040') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int040';
    end if;
    if to_regclass('bronze.bronze_dhist_snh_entregas_evento_caixa') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_dhist_snh_entregas_evento_caixa';
    end if;
    if to_regclass('reloginho.bronze_reloginho_snh_entregas_evento_caixa') is not null then
        raise exception 'postflight: origem ainda existe %', 'reloginho.bronze_reloginho_snh_entregas_evento_caixa';
    end if;
    if to_regclass('bronze.bronze_dhist_empreendimento_snh_caixa') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_dhist_empreendimento_snh_caixa';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_snh_caixa') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.bronze_mcmv_historico_empreendimento_snh_caixa';
    end if;
    if to_regclass('bronze.bronze_sftp_empreendimento_int065') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_sftp_empreendimento_int065';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_empreendimento_int065') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.bronze_mcmv_historico_empreendimento_int065';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_bases_relatorio_executivo') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_dhist_serie_bases_relatorio_executivo';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_bases_relatorio_executivo') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.bronze_mcmv_historico_serie_bases_relatorio_executivo';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_min_cidades') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_dhist_serie_min_cidades';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_min_cidades') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.bronze_mcmv_historico_serie_min_cidades';
    end if;
    if to_regclass('bronze.bronze_dhist_serie_bext') is null then
        raise exception 'postflight: destino ausente %', 'bronze.bronze_dhist_serie_bext';
    end if;
    if to_regclass('dados_historicos.bronze_mcmv_historico_serie_bext') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.bronze_mcmv_historico_serie_bext';
    end if;
    if to_regclass('prata.prata_dhist_snh_entregas_mes') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_dhist_snh_entregas_mes';
    end if;
    if to_regclass('reloginho.silver_historico_snh_entregas_mes') is not null then
        raise exception 'postflight: origem ainda existe %', 'reloginho.silver_historico_snh_entregas_mes';
    end if;
    if to_regclass('prata.prata_dhist_snh_apf_mes') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_dhist_snh_apf_mes';
    end if;
    if to_regclass('reloginho.silver_historico_snh_apf_mes') is not null then
        raise exception 'postflight: origem ainda existe %', 'reloginho.silver_historico_snh_apf_mes';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_indicadores';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho') is not null then
        raise exception 'postflight: origem ainda existe %', 'reloginho.gold_indicadores_reloginho';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_frente') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_indicadores_frente';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho_frente') is not null then
        raise exception 'postflight: origem ainda existe %', 'reloginho.gold_indicadores_reloginho_frente';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_entregas') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_indicadores_entregas';
    end if;
    if to_regclass('reloginho.gold_indicadores_reloginho_entregas') is not null then
        raise exception 'postflight: origem ainda existe %', 'reloginho.gold_indicadores_reloginho_entregas';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_dashboard') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_resumo_dashboard';
    end if;
    if to_regclass('reloginho.gold_resumo_reloginho_dashboard') is not null then
        raise exception 'postflight: origem ainda existe %', 'reloginho.gold_resumo_reloginho_dashboard';
    end if;
    if to_regclass('ouro.ouro_reloginho_indicadores_gargalo_desempenho') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('reloginho.gold_indicadores_gargalo_desempenho') is not null then
        raise exception 'postflight: origem ainda existe %', 'reloginho.gold_indicadores_gargalo_desempenho';
    end if;
    if to_regclass('ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_reloginho_resumo_gargalo_desempenho_dashboard';
    end if;
    if to_regclass('reloginho.gold_resumo_gargalo_desempenho_dashboard') is not null then
        raise exception 'postflight: origem ainda existe %', 'reloginho.gold_resumo_gargalo_desempenho_dashboard';
    end if;
    if to_regclass('prata.prata_fds_historico_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_fds_historico_empreendimento';
    end if;
    if to_regclass('empreendimentos_fds.silver_historico_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'empreendimentos_fds.silver_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_far_historico_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_far_historico_empreendimento';
    end if;
    if to_regclass('empreendimento_far.silver_historico_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'empreendimento_far.silver_historico_empreendimento';
    end if;
    if to_regclass('prata.prata_rural_historico_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_rural_historico_empreendimento';
    end if;
    if to_regclass('empreendimento_rural.silver_historico_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'empreendimento_rural.silver_historico_empreendimento';
    end if;
    if to_regclass('ouro.ouro_dhist_serie_mensal') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_dhist_serie_mensal';
    end if;
    if to_regclass('dados_historicos.gold_serie_mensal') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.gold_serie_mensal';
    end if;
    if to_regclass('ouro.ouro_dhist_snapshot_empreendimento_atual') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_dhist_snapshot_empreendimento_atual';
    end if;
    if to_regclass('dados_historicos.gold_snapshot_empreendimento_atual') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.gold_snapshot_empreendimento_atual';
    end if;
    if to_regclass('ouro.ouro_dhist_marco_empreendimento') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_dhist_marco_empreendimento';
    end if;
    if to_regclass('dados_historicos.gold_marco_empreendimento') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.gold_marco_empreendimento';
    end if;
    if to_regclass('ouro.ouro_dhist_serie_situacao_mensal') is null then
        raise exception 'postflight: destino ausente %', 'ouro.ouro_dhist_serie_situacao_mensal';
    end if;
    if to_regclass('dados_historicos.gold_serie_situacao_mensal') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.gold_serie_situacao_mensal';
    end if;
    if to_regclass('prata.prata_dhist_serie_executiva') is null then
        raise exception 'postflight: destino ausente %', 'prata.prata_dhist_serie_executiva';
    end if;
    if to_regclass('dados_historicos.silver_mcmv_historico_serie_executiva') is not null then
        raise exception 'postflight: origem ainda existe %', 'dados_historicos.silver_mcmv_historico_serie_executiva';
    end if;
end $$;

commit;
