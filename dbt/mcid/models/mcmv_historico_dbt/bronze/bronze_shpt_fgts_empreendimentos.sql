{{ config(materialized="table") }}

-- BRONZE — empreendimentos do Canal FGTS,
-- `staging/sharepoint/fgts_canal_tab_ao_1_tab_empreendimentos.parquet`, cópia
-- fiel. 73.231 linhas.
--
-- Usada pela prata de Pró-Moradia (prata_hist_pro_moradia_contrato) para
-- enriquecer contratos via `cod_empreendimento` (left join) — fonte
-- compartilhada em potencial por outras linhas do Canal FGTS, sem filtro.
--
-- dt_referencia = data de ingestão do arquivo (`_ingested_at`). Corpo em
-- macros/historico/corpos_bronze.sql.
-- change frentes-restantes-mcmv-historico.
{{ bronze_flat_shpt('sharepoint/fgts_canal_tab_ao_1_tab_empreendimentos.parquet') }}
