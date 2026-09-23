{{ config(materialized="table") }}

-- BRONZE — contratos do Canal FGTS (todas as linhas, não só Pró-Moradia),
-- `staging/sharepoint/fgts_canal_tab_ao_1_contratos_fgts.parquet`, cópia fiel.
--
-- 154.698 linhas. Fonte compartilhada em potencial por outras linhas do Canal
-- FGTS além de Pró-Moradia (ex.: `cod_linha = '33'` = "HAB / PROG DE APOIO
-- PRODUCAO DE HABITACOES", parente de MCMV Cidades) — por isso a bronze NÃO
-- filtra por `cod_linha` nem por qualquer outra coluna (D1 da change
-- frentes-restantes-mcmv-historico): é a prata de Pró-Moradia
-- (prata_pro_moradia_historico_contrato) que aplica `where cod_linha = '26'`.
--
-- dt_referencia = data de ingestão do arquivo (`_ingested_at`); não há
-- snapshot datado no nome. `dte_assinatura` vem em formato MM/DD/YY (não
-- BR) — parse dedicado na prata. Corpo em
-- macros/historico/corpos_bronze.sql.
-- change frentes-restantes-mcmv-historico.
{{ bronze_flat_shpt('sharepoint/fgts_canal_tab_ao_1_contratos_fgts.parquet') }}
