-- =====================================================================
-- reverter_seeds_prod.sql — ROLLBACK: seeds -> data_quality/conjuntura
--
-- GERADO por scripts/migracao/gerar_migracao_seeds.py a partir de mapa_seeds.csv.
-- NÃO EDITAR À MÃO — regenerar com o script.
--
-- ATENÇÃO: script MANUAL, roda contra o Postgres `prod`. NÃO deve ser chamado
-- por publicar_historico.py, publicar-historico.sh, run-*.sh, rebuild-local.sh
-- nem por hooks on-run-* do dbt. Move as SEEDS do eixo histórico/reloginho para
-- o schema `seeds` (METADADO — instantâneo, não copia dados). As seeds não
-- mudam de nome, só de schema.
--
-- Uso:
--   psql "$DSN" -v ON_ERROR_STOP=1 -f scripts/migracao/reverter_seeds_prod.sql
-- (ordem e pré-requisitos: ver README.md)
-- =====================================================================

\set ON_ERROR_STOP on

begin;

-- 1) preflight -------------------------------------------------------
do $$
begin
    -- preflight: 10 seeds no inventario
    if to_regclass('seeds.campos_obrigatorios') is null then
        raise exception 'origem ausente: %', 'seeds.campos_obrigatorios';
    end if;
    if to_regclass('seeds.colunas_bronze_ignoradas') is null then
        raise exception 'origem ausente: %', 'seeds.colunas_bronze_ignoradas';
    end if;
    if to_regclass('seeds.colunas_esperadas') is null then
        raise exception 'origem ausente: %', 'seeds.colunas_esperadas';
    end if;
    if to_regclass('seeds.dominio_motivo_paralisacao') is null then
        raise exception 'origem ausente: %', 'seeds.dominio_motivo_paralisacao';
    end if;
    if to_regclass('seeds.dominio_regiao_uf') is null then
        raise exception 'origem ausente: %', 'seeds.dominio_regiao_uf';
    end if;
    if to_regclass('seeds.dominio_retomada') is null then
        raise exception 'origem ausente: %', 'seeds.dominio_retomada';
    end if;
    if to_regclass('seeds.dominio_status') is null then
        raise exception 'origem ausente: %', 'seeds.dominio_status';
    end if;
    if to_regclass('seeds.faixa_valor_uh') is null then
        raise exception 'origem ausente: %', 'seeds.faixa_valor_uh';
    end if;
    if to_regclass('seeds.quarentena_valores_financeiros') is null then
        raise exception 'origem ausente: %', 'seeds.quarentena_valores_financeiros';
    end if;
    if to_regclass('seeds.issue_118_mcmv_serie_temporal_piloto') is null then
        raise exception 'origem ausente: %', 'seeds.issue_118_mcmv_serie_temporal_piloto';
    end if;
    if to_regclass('data_quality.campos_obrigatorios') is not null then
        raise exception 'destino ja existe: %', 'data_quality.campos_obrigatorios';
    end if;
    if to_regclass('data_quality.colunas_bronze_ignoradas') is not null then
        raise exception 'destino ja existe: %', 'data_quality.colunas_bronze_ignoradas';
    end if;
    if to_regclass('data_quality.colunas_esperadas') is not null then
        raise exception 'destino ja existe: %', 'data_quality.colunas_esperadas';
    end if;
    if to_regclass('data_quality.dominio_motivo_paralisacao') is not null then
        raise exception 'destino ja existe: %', 'data_quality.dominio_motivo_paralisacao';
    end if;
    if to_regclass('data_quality.dominio_regiao_uf') is not null then
        raise exception 'destino ja existe: %', 'data_quality.dominio_regiao_uf';
    end if;
    if to_regclass('data_quality.dominio_retomada') is not null then
        raise exception 'destino ja existe: %', 'data_quality.dominio_retomada';
    end if;
    if to_regclass('data_quality.dominio_status') is not null then
        raise exception 'destino ja existe: %', 'data_quality.dominio_status';
    end if;
    if to_regclass('data_quality.faixa_valor_uh') is not null then
        raise exception 'destino ja existe: %', 'data_quality.faixa_valor_uh';
    end if;
    if to_regclass('data_quality.quarentena_valores_financeiros') is not null then
        raise exception 'destino ja existe: %', 'data_quality.quarentena_valores_financeiros';
    end if;
    if to_regclass('conjuntura.issue_118_mcmv_serie_temporal_piloto') is not null then
        raise exception 'destino ja existe: %', 'conjuntura.issue_118_mcmv_serie_temporal_piloto';
    end if;
end $$;

-- 2) move de schema (SET SCHEMA; as seeds não mudam de nome) --------
alter table "seeds"."campos_obrigatorios" set schema "data_quality";
alter table "seeds"."colunas_bronze_ignoradas" set schema "data_quality";
alter table "seeds"."colunas_esperadas" set schema "data_quality";
alter table "seeds"."dominio_motivo_paralisacao" set schema "data_quality";
alter table "seeds"."dominio_regiao_uf" set schema "data_quality";
alter table "seeds"."dominio_retomada" set schema "data_quality";
alter table "seeds"."dominio_status" set schema "data_quality";
alter table "seeds"."faixa_valor_uh" set schema "data_quality";
alter table "seeds"."quarentena_valores_financeiros" set schema "data_quality";
alter table "seeds"."issue_118_mcmv_serie_temporal_piloto" set schema "conjuntura";

-- 3) postflight -----------------------------------------------------
do $$
begin
    if to_regclass('data_quality.campos_obrigatorios') is null then
        raise exception 'postflight: destino ausente: %', 'data_quality.campos_obrigatorios';
    end if;
    if to_regclass('data_quality.colunas_bronze_ignoradas') is null then
        raise exception 'postflight: destino ausente: %', 'data_quality.colunas_bronze_ignoradas';
    end if;
    if to_regclass('data_quality.colunas_esperadas') is null then
        raise exception 'postflight: destino ausente: %', 'data_quality.colunas_esperadas';
    end if;
    if to_regclass('data_quality.dominio_motivo_paralisacao') is null then
        raise exception 'postflight: destino ausente: %', 'data_quality.dominio_motivo_paralisacao';
    end if;
    if to_regclass('data_quality.dominio_regiao_uf') is null then
        raise exception 'postflight: destino ausente: %', 'data_quality.dominio_regiao_uf';
    end if;
    if to_regclass('data_quality.dominio_retomada') is null then
        raise exception 'postflight: destino ausente: %', 'data_quality.dominio_retomada';
    end if;
    if to_regclass('data_quality.dominio_status') is null then
        raise exception 'postflight: destino ausente: %', 'data_quality.dominio_status';
    end if;
    if to_regclass('data_quality.faixa_valor_uh') is null then
        raise exception 'postflight: destino ausente: %', 'data_quality.faixa_valor_uh';
    end if;
    if to_regclass('data_quality.quarentena_valores_financeiros') is null then
        raise exception 'postflight: destino ausente: %', 'data_quality.quarentena_valores_financeiros';
    end if;
    if to_regclass('conjuntura.issue_118_mcmv_serie_temporal_piloto') is null then
        raise exception 'postflight: destino ausente: %', 'conjuntura.issue_118_mcmv_serie_temporal_piloto';
    end if;
    if to_regclass('seeds.campos_obrigatorios') is not null then
        raise exception 'postflight: origem ainda existe: %', 'seeds.campos_obrigatorios';
    end if;
    if to_regclass('seeds.colunas_bronze_ignoradas') is not null then
        raise exception 'postflight: origem ainda existe: %', 'seeds.colunas_bronze_ignoradas';
    end if;
    if to_regclass('seeds.colunas_esperadas') is not null then
        raise exception 'postflight: origem ainda existe: %', 'seeds.colunas_esperadas';
    end if;
    if to_regclass('seeds.dominio_motivo_paralisacao') is not null then
        raise exception 'postflight: origem ainda existe: %', 'seeds.dominio_motivo_paralisacao';
    end if;
    if to_regclass('seeds.dominio_regiao_uf') is not null then
        raise exception 'postflight: origem ainda existe: %', 'seeds.dominio_regiao_uf';
    end if;
    if to_regclass('seeds.dominio_retomada') is not null then
        raise exception 'postflight: origem ainda existe: %', 'seeds.dominio_retomada';
    end if;
    if to_regclass('seeds.dominio_status') is not null then
        raise exception 'postflight: origem ainda existe: %', 'seeds.dominio_status';
    end if;
    if to_regclass('seeds.faixa_valor_uh') is not null then
        raise exception 'postflight: origem ainda existe: %', 'seeds.faixa_valor_uh';
    end if;
    if to_regclass('seeds.quarentena_valores_financeiros') is not null then
        raise exception 'postflight: origem ainda existe: %', 'seeds.quarentena_valores_financeiros';
    end if;
    if to_regclass('seeds.issue_118_mcmv_serie_temporal_piloto') is not null then
        raise exception 'postflight: origem ainda existe: %', 'seeds.issue_118_mcmv_serie_temporal_piloto';
    end if;
end $$;

commit;
