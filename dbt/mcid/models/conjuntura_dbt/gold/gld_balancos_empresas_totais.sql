{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Lançamentos e Vendas TOTAIS (todas as empresas
-- monitoradas), com variações. Página 2, seção 2 (cards de totais).
-- Dado MANUAL (boletim.xlsx / conjuntura.bnz_manual_dados_trimestrais).

select
    data_referencia,
    periodo,
    edicao,
    ano,
    trimestre,
    lancamentos_totais,
    var_lancamentos_totais_tri_anterior,
    var_lancamentos_totais_mesmo_tri_ano_anterior,
    var_lancamentos_totais_acumulado_mesmo_periodo_ano_anterior,
    vendas_totais,
    var_vendas_totais_tri_anterior,
    var_vendas_totais_mesmo_tri_ano_anterior,
    var_vendas_totais_acumulado_mesmo_periodo_ano_anterior
from {{ ref('slv_manual_trimestrais') }}
order by ano desc, trimestre desc
