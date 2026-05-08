SELECT
    ano,
    trimestre,

    cbic_lancamentos_total,
    cbic_lancamentos_mcmv,
    cbic_lancamentos_demais,

    cbic_vendas_total,
    cbic_vendas_mcmv,
    cbic_vendas_demais,

    cbic_lancamentos_regiao_norte,
    cbic_lancamentos_regiao_nordeste,
    cbic_lancamentos_regiao_centro_oeste,
    cbic_lancamentos_regiao_sudeste,
    cbic_lancamentos_regiao_sul,

    cbic_lancamentos_mcmv_regiao_norte,
    cbic_lancamentos_mcmv_regiao_nordeste,
    cbic_lancamentos_mcmv_regiao_centro_oeste,
    cbic_lancamentos_mcmv_regiao_sudeste,
    cbic_lancamentos_mcmv_regiao_sul,

    cbic_vendas_regiao_norte,
    cbic_vendas_regiao_nordeste,
    cbic_vendas_regiao_centro_oeste,
    cbic_vendas_regiao_sudeste,
    cbic_vendas_regiao_sul,

    cbic_vendas_mcmv_regiao_norte,
    cbic_vendas_mcmv_regiao_nordeste,
    cbic_vendas_mcmv_regiao_centro_oeste,
    cbic_vendas_mcmv_regiao_sudeste,
    cbic_vendas_mcmv_regiao_sul,

    cbic_lancamentos_mcmv_perc_regiao_norte,
    cbic_lancamentos_mcmv_perc_regiao_nordeste,
    cbic_lancamentos_mcmv_perc_regiao_centro_oeste,
    cbic_lancamentos_mcmv_perc_regiao_sudeste,
    cbic_lancamentos_mcmv_perc_regiao_sul,

    cbic_vendas_mcmv_perc_regiao_norte,
    cbic_vendas_mcmv_perc_regiao_nordeste,
    cbic_vendas_mcmv_perc_regiao_centro_oeste,
    cbic_vendas_mcmv_perc_regiao_sudeste,
    cbic_vendas_mcmv_perc_regiao_sul

FROM {{ source('conjuntura_bronze', 'bronze_cbic_lancamentos_vendas') }}