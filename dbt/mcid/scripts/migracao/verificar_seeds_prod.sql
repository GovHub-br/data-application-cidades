-- =====================================================================
-- verificar_seeds_prod.sql — READ-ONLY. Rodar ANTES e DEPOIS da migração
-- das seeds e diffar as saídas.
--
-- GERADO por scripts/migracao/gerar_migracao_seeds.py — NÃO EDITAR À MÃO.
--   psql "$DSN" -f scripts/migracao/verificar_seeds_prod.sql
-- =====================================================================

\echo '== seeds presentes nos schemas de origem e destino =='
select table_schema, table_name
from information_schema.tables
where table_schema in ('conjuntura', 'data_quality', 'seeds')
order by table_schema, table_name;

\echo '== contagem por seed — schemas ANTIGOS (deve existir ANTES) =='
select * from (
select 'data_quality.campos_obrigatorios' as tabela, (select count(*) from "data_quality"."campos_obrigatorios") as n_linhas
union all
select 'data_quality.colunas_bronze_ignoradas' as tabela, (select count(*) from "data_quality"."colunas_bronze_ignoradas") as n_linhas
union all
select 'data_quality.colunas_esperadas' as tabela, (select count(*) from "data_quality"."colunas_esperadas") as n_linhas
union all
select 'data_quality.dominio_motivo_paralisacao' as tabela, (select count(*) from "data_quality"."dominio_motivo_paralisacao") as n_linhas
union all
select 'data_quality.dominio_regiao_uf' as tabela, (select count(*) from "data_quality"."dominio_regiao_uf") as n_linhas
union all
select 'data_quality.dominio_retomada' as tabela, (select count(*) from "data_quality"."dominio_retomada") as n_linhas
union all
select 'data_quality.dominio_status' as tabela, (select count(*) from "data_quality"."dominio_status") as n_linhas
union all
select 'data_quality.faixa_valor_uh' as tabela, (select count(*) from "data_quality"."faixa_valor_uh") as n_linhas
union all
select 'data_quality.quarentena_valores_financeiros' as tabela, (select count(*) from "data_quality"."quarentena_valores_financeiros") as n_linhas
union all
select 'conjuntura.issue_118_mcmv_serie_temporal_piloto' as tabela, (select count(*) from "conjuntura"."issue_118_mcmv_serie_temporal_piloto") as n_linhas
) t order by tabela;

\echo '== contagem por seed — schema `seeds` (deve existir DEPOIS) =='
select * from (
select 'seeds.campos_obrigatorios' as tabela, (select count(*) from "seeds"."campos_obrigatorios") as n_linhas
union all
select 'seeds.colunas_bronze_ignoradas' as tabela, (select count(*) from "seeds"."colunas_bronze_ignoradas") as n_linhas
union all
select 'seeds.colunas_esperadas' as tabela, (select count(*) from "seeds"."colunas_esperadas") as n_linhas
union all
select 'seeds.dominio_motivo_paralisacao' as tabela, (select count(*) from "seeds"."dominio_motivo_paralisacao") as n_linhas
union all
select 'seeds.dominio_regiao_uf' as tabela, (select count(*) from "seeds"."dominio_regiao_uf") as n_linhas
union all
select 'seeds.dominio_retomada' as tabela, (select count(*) from "seeds"."dominio_retomada") as n_linhas
union all
select 'seeds.dominio_status' as tabela, (select count(*) from "seeds"."dominio_status") as n_linhas
union all
select 'seeds.faixa_valor_uh' as tabela, (select count(*) from "seeds"."faixa_valor_uh") as n_linhas
union all
select 'seeds.quarentena_valores_financeiros' as tabela, (select count(*) from "seeds"."quarentena_valores_financeiros") as n_linhas
union all
select 'seeds.issue_118_mcmv_serie_temporal_piloto' as tabela, (select count(*) from "seeds"."issue_118_mcmv_serie_temporal_piloto") as n_linhas
) t order by tabela;
