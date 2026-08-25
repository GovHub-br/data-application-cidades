{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Lançamentos e Vendas por construtora, com as
-- variações % que o boletim mostra (trim. anterior, mesmo trim. ano
-- anterior, acumulado mesmo período ano anterior). Página 2. Dado MANUAL
-- (boletim.xlsx / manual_conjuntura.dados_trimestrais) — as variações por
-- empresa já vêm calculadas na planilha oficial; substituiu a fonte antiga
-- (manual_conjuntura.empresas_balanco_lancamentos_vendas), que só tinha os
-- valores absolutos, sem as variações que o boletim de fato destaca.

with base as (
    select * from {{ ref('silver_continuo_manual_trimestrais') }}
),

unpivoted as (

    select periodo, ano, trimestre, 'MRV' as empresa,
        mrv_lancamentos as lancamentos,
        mrv_var_lancamentos_tri_anterior as var_lancamentos_tri_anterior,
        mrv_var_lancamentos_mesmo_tri_ano_anterior as var_lancamentos_mesmo_tri_ano_anterior,
        mrv_var_lancamento_acumulado_mesmo_periodo_ano_anterior as var_lancamentos_acumulado_periodo_ano_anterior,
        mrv_vendas as vendas,
        mrv_var_vendas_tri_anterior as var_vendas_tri_anterior,
        mrv_var_vendas_mesmo_tri_ano_anterior as var_vendas_mesmo_tri_ano_anterior,
        mrv_var_vendas_acumulado_mesmo_periodo_ano_anterior as var_vendas_acumulado_periodo_ano_anterior
    from base

    union all

    select periodo, ano, trimestre, 'Cury',
        cury_lancamentos,
        cury_var_lancamentos_tri_anterior,
        cury_var_lancamentos_mesmo_tri_ano_anterior,
        cury_var_lancamento_acumulado_mesmo_periodo_ano_anterior,
        cury_vendas,
        cury_var_vendas_tri_anterior,
        cury_var_vendas_mesmo_tri_ano_anterior,
        cury_var_vendas_acumulado_mesmo_periodo_ano_anterior
    from base

    union all

    select periodo, ano, trimestre, 'Tenda',
        tenda_lancamentos,
        tenda_var_lancamentos_tri_anterior,
        tenda_var_lancamentos_mesmo_tri_ano_anterior,
        tenda_var_lancamento_acumulado_mesmo_periodo_ano_anterior,
        tenda_vendas,
        tenda_var_vendas_tri_anterior,
        tenda_var_vendas_mesmo_tri_ano_anterior,
        tenda_var_vendas_acumulado_mesmo_periodo_ano_anterior
    from base

    union all

    select periodo, ano, trimestre, 'Direcional',
        direcional_lancamentos,
        direcional_var_lancamentos_tri_anterior,
        direcional_var_lancamentos_mesmo_tri_ano_anterior,
        direcional_var_lancamento_acumulado_mesmo_periodo_ano_anteri,
        direcional_vendas,
        direcional_var_vendas_tri_anterior,
        direcional_var_vendas_mesmo_tri_ano_anterior,
        direcional_var_vendas_acumulado_mesmo_periodo_ano_anterior
    from base

    union all

    select periodo, ano, trimestre, 'Pacaembu',
        pacaembu_lancamentos,
        pacaembu_var_lancamentos_tri_anterior,
        pacaembu_var_lancamentos_mesmo_tri_ano_anterior,
        pacaembu_var_lancamento_acumulado_mesmo_periodo_ano_anterior,
        pacaembu_vendas,
        pacaembu_var_vendas_tri_anterior,
        pacaembu_var_vendas_mesmo_tri_ano_anterior,
        pacaembu_var_vendas_acumulado_mesmo_periodo_ano_anterior
    from base

    union all

    select periodo, ano, trimestre, 'Plano & Plano',
        plano_plano_lancamentos,
        plano_plano_var_lancamentos_tri_anterior,
        plano_plano_var_lancamentos_mesmo_tri_ano_anterior,
        plano_plano_var_lancamento_acumulado_mesmo_periodo_ano_anter,
        plano_plano_vendas,
        plano_plano_var_vendas_tri_anterior,
        plano_plano_var_vendas_mesmo_tri_ano_anterior,
        plano_plano_var_vendas_acumulado_mesmo_periodo_ano_anterior
    from base

)

select
    periodo,
    ano,
    trimestre,
    empresa,
    lancamentos,
    var_lancamentos_tri_anterior,
    var_lancamentos_mesmo_tri_ano_anterior,
    var_lancamentos_acumulado_periodo_ano_anterior,
    vendas,
    var_vendas_tri_anterior,
    var_vendas_mesmo_tri_ano_anterior,
    var_vendas_acumulado_periodo_ano_anterior
from unpivoted
where lancamentos is not null or vendas is not null
order by ano desc, trimestre desc, empresa
