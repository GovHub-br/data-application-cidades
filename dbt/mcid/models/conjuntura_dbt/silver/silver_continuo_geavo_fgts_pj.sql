{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: FGTS-PJ (Apoio à Produção), via sistema
-- GEAVO da Caixa. Página 2/5. Ver docs/conjuntura-fontes-dbt.md (local,
-- fora do repositório).
--
-- O indicador do boletim é "Financiamentos Habitacionais (UH)" —
-- contagem de UNIDADES HABITACIONAIS financiadas no trimestre, não R$.
-- Confirmado em 2026-08-25 contra os 3 boletins publicados que o Lucas
-- tem (3T2025/4T2025/1T2026): soma de `qt_unidades_financiadas` por
-- trimestre bate EXATO com o que o boletim publica como "FGTS-PJ" em 3
-- dos 4 trimestres comparáveis (1T2025: 81.622=81.622; 3T2025:
-- 65.690=65.690; 4T2025: 61.212=61.212); o 4º (1T2026: 59.836 vs 59.862,
-- 0,04% de diferença) é consistente com contrato registrado tardiamente
-- — mesmo padrão de revisão já visto no CBIC, não erro de método.
--
-- Versão anterior deste model (2026-08-25, mais cedo) tentava juntar com
-- `tab_desembolsos_fgts`/`tab_contratos_fgts` pra calcular valor
-- desembolsado em R$ — **descartado**: não é isso que o boletim publica
-- pra esse indicador (o header é literal "(UH)"), e `Base_PJ_FGTS` já é
-- só PJ "Apoio à Produção" por natureza do arquivo, não precisa cruzar
-- com `cod_linha` de outra tabela.
--
-- ATENÇÃO: o nome do arquivo muda todo mês (Base_PJ_FGTS_{data}.parquet).
-- Aponta pro mais recente conhecido em 2026-08-25 (07/07/2026) — precisa
-- atualizar o caminho manualmente até existir uma DAG que copie sempre o
-- arquivo mais novo pra um caminho fixo.

select
    extract(year from dt_assinatura::text::date)::int  as ano,
    (floor((extract(month from dt_assinatura::text::date)::int - 1) / 3))::int + 1 as trimestre,
    sum(replace(qt_unidades_financiadas::text, ',', '.')::numeric) as fgts_pj_uh,
    count(*)                as fgts_pj_qtd_contratos,
    current_timestamp       as dt_silver
from {{ ref('bronze_continuo_geavo_fgts_pj') }}
group by 1, 2
