{{ config(materialized='table') }}

-- Todos os quadros do boletim em formato LONGO: uma linha por célula.
--
-- Por que existe: a conferência contra os boletins publicados era um script
-- Python de 177 linhas que abria conexão própria, rodava 32 SQLs de um YAML e
-- escrevia relatório em Markdown. Com os quadros já materializados e uniformes
-- (`edicao` + rótulo + colunas), a comparação vira um join — e o script vira
-- um teste que roda no mesmo `dbt build` que já roda.
--
-- `valor` só é preenchido quando a célula é numérica; texto e vazio viram
-- NULL em vez de quebrar o cast.

    select
        'gold_boletim_p1_pib_construcao_civil_em_de_crescimento'::text                as modelo,
        1                            as pagina,
        'PIB Construção Civil (em % de Crescimento)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        '4 trim. antes'::text as coluna,
        case when "4 trim. antes"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "4 trim. antes"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_pib_construcao_civil_em_de_crescimento') }}
    union all
    select
        'gold_boletim_p1_pib_construcao_civil_em_de_crescimento'::text                as modelo,
        1                            as pagina,
        'PIB Construção Civil (em % de Crescimento)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        '3 trim. antes'::text as coluna,
        case when "3 trim. antes"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "3 trim. antes"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_pib_construcao_civil_em_de_crescimento') }}
    union all
    select
        'gold_boletim_p1_pib_construcao_civil_em_de_crescimento'::text                as modelo,
        1                            as pagina,
        'PIB Construção Civil (em % de Crescimento)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        '2 trim. antes'::text as coluna,
        case when "2 trim. antes"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "2 trim. antes"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_pib_construcao_civil_em_de_crescimento') }}
    union all
    select
        'gold_boletim_p1_pib_construcao_civil_em_de_crescimento'::text                as modelo,
        1                            as pagina,
        'PIB Construção Civil (em % de Crescimento)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'trim. anterior'::text as coluna,
        case when "trim. anterior"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "trim. anterior"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_pib_construcao_civil_em_de_crescimento') }}
    union all
    select
        'gold_boletim_p1_pib_construcao_civil_em_de_crescimento'::text                as modelo,
        1                            as pagina,
        'PIB Construção Civil (em % de Crescimento)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'trimestre selecionado'::text as coluna,
        case when "trimestre selecionado"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "trimestre selecionado"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_pib_construcao_civil_em_de_crescimento') }}
    union all
    select
        'gold_boletim_p1_lancamentos_por_regiao_cbic'::text                as modelo,
        1                            as pagina,
        'Lançamentos por Região (CBIC)'::text as quadro,
        edicao,
        "regiao"::text                as linha,
        'TOTAL'::text as coluna,
        case when "TOTAL"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "TOTAL"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_lancamentos_por_regiao_cbic') }}
    union all
    select
        'gold_boletim_p1_lancamentos_por_regiao_cbic'::text                as modelo,
        1                            as pagina,
        'Lançamentos por Região (CBIC)'::text as quadro,
        edicao,
        "regiao"::text                as linha,
        'MCMV'::text as coluna,
        case when "MCMV"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "MCMV"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_lancamentos_por_regiao_cbic') }}
    union all
    select
        'gold_boletim_p1_lancamentos_por_regiao_cbic'::text                as modelo,
        1                            as pagina,
        'Lançamentos por Região (CBIC)'::text as quadro,
        edicao,
        "regiao"::text                as linha,
        '% MCMV'::text as coluna,
        case when "% MCMV"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "% MCMV"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_lancamentos_por_regiao_cbic') }}
    union all
    select
        'gold_boletim_p1_vendas_por_regiao_cbic'::text                as modelo,
        1                            as pagina,
        'Vendas por Região (CBIC)'::text as quadro,
        edicao,
        "regiao"::text                as linha,
        'TOTAL'::text as coluna,
        case when "TOTAL"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "TOTAL"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_vendas_por_regiao_cbic') }}
    union all
    select
        'gold_boletim_p1_vendas_por_regiao_cbic'::text                as modelo,
        1                            as pagina,
        'Vendas por Região (CBIC)'::text as quadro,
        edicao,
        "regiao"::text                as linha,
        'MCMV'::text as coluna,
        case when "MCMV"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "MCMV"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_vendas_por_regiao_cbic') }}
    union all
    select
        'gold_boletim_p1_vendas_por_regiao_cbic'::text                as modelo,
        1                            as pagina,
        'Vendas por Região (CBIC)'::text as quadro,
        edicao,
        "regiao"::text                as linha,
        '% MCMV'::text as coluna,
        case when "% MCMV"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "% MCMV"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_vendas_por_regiao_cbic') }}
    union all
    select
        'gold_boletim_p1_cbic_lancamentos_e_vendas_totais'::text                as modelo,
        1                            as pagina,
        'CBIC — Lançamentos e Vendas (totais)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Lançamentos TOTAL'::text as coluna,
        case when "Lançamentos TOTAL"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Lançamentos TOTAL"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_cbic_lancamentos_e_vendas_totais') }}
    union all
    select
        'gold_boletim_p1_cbic_lancamentos_e_vendas_totais'::text                as modelo,
        1                            as pagina,
        'CBIC — Lançamentos e Vendas (totais)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Lançamentos MCMV'::text as coluna,
        case when "Lançamentos MCMV"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Lançamentos MCMV"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_cbic_lancamentos_e_vendas_totais') }}
    union all
    select
        'gold_boletim_p1_cbic_lancamentos_e_vendas_totais'::text                as modelo,
        1                            as pagina,
        'CBIC — Lançamentos e Vendas (totais)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Lançamentos DEMAIS'::text as coluna,
        case when "Lançamentos DEMAIS"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Lançamentos DEMAIS"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_cbic_lancamentos_e_vendas_totais') }}
    union all
    select
        'gold_boletim_p1_cbic_lancamentos_e_vendas_totais'::text                as modelo,
        1                            as pagina,
        'CBIC — Lançamentos e Vendas (totais)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Vendas TOTAL'::text as coluna,
        case when "Vendas TOTAL"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Vendas TOTAL"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_cbic_lancamentos_e_vendas_totais') }}
    union all
    select
        'gold_boletim_p1_cbic_lancamentos_e_vendas_totais'::text                as modelo,
        1                            as pagina,
        'CBIC — Lançamentos e Vendas (totais)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Vendas MCMV'::text as coluna,
        case when "Vendas MCMV"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Vendas MCMV"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_cbic_lancamentos_e_vendas_totais') }}
    union all
    select
        'gold_boletim_p1_cbic_lancamentos_e_vendas_totais'::text                as modelo,
        1                            as pagina,
        'CBIC — Lançamentos e Vendas (totais)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Vendas DEMAIS'::text as coluna,
        case when "Vendas DEMAIS"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Vendas DEMAIS"::text::numeric end as valor
    from {{ ref('gold_boletim_p1_cbic_lancamentos_e_vendas_totais') }}
    union all
    select
        'gold_boletim_p2_lancamentos_por_construtora_variacao'::text                as modelo,
        2                            as pagina,
        'Lançamentos por construtora (variação %)'::text as quadro,
        edicao,
        "empresa"::text                as linha,
        'vs. trim. anterior'::text as coluna,
        case when "vs. trim. anterior"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "vs. trim. anterior"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_lancamentos_por_construtora_variacao') }}
    union all
    select
        'gold_boletim_p2_lancamentos_por_construtora_variacao'::text                as modelo,
        2                            as pagina,
        'Lançamentos por construtora (variação %)'::text as quadro,
        edicao,
        "empresa"::text                as linha,
        'vs. mesmo trim. ano ant.'::text as coluna,
        case when "vs. mesmo trim. ano ant."::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "vs. mesmo trim. ano ant."::text::numeric end as valor
    from {{ ref('gold_boletim_p2_lancamentos_por_construtora_variacao') }}
    union all
    select
        'gold_boletim_p2_lancamentos_por_construtora_variacao'::text                as modelo,
        2                            as pagina,
        'Lançamentos por construtora (variação %)'::text as quadro,
        edicao,
        "empresa"::text                as linha,
        '12m atual / 12m anterior'::text as coluna,
        case when "12m atual / 12m anterior"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "12m atual / 12m anterior"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_lancamentos_por_construtora_variacao') }}
    union all
    select
        'gold_boletim_p2_lancamentos_por_construtora_variacao'::text                as modelo,
        2                            as pagina,
        'Lançamentos por construtora (variação %)'::text as quadro,
        edicao,
        "empresa"::text                as linha,
        '12m anterior / 12m retrasado'::text as coluna,
        case when "12m anterior / 12m retrasado"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "12m anterior / 12m retrasado"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_lancamentos_por_construtora_variacao') }}
    union all
    select
        'gold_boletim_p2_vendas_por_construtora_variacao'::text                as modelo,
        2                            as pagina,
        'Vendas por construtora (variação %)'::text as quadro,
        edicao,
        "empresa"::text                as linha,
        'vs. trim. anterior'::text as coluna,
        case when "vs. trim. anterior"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "vs. trim. anterior"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_vendas_por_construtora_variacao') }}
    union all
    select
        'gold_boletim_p2_vendas_por_construtora_variacao'::text                as modelo,
        2                            as pagina,
        'Vendas por construtora (variação %)'::text as quadro,
        edicao,
        "empresa"::text                as linha,
        'vs. mesmo trim. ano ant.'::text as coluna,
        case when "vs. mesmo trim. ano ant."::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "vs. mesmo trim. ano ant."::text::numeric end as valor
    from {{ ref('gold_boletim_p2_vendas_por_construtora_variacao') }}
    union all
    select
        'gold_boletim_p2_vendas_por_construtora_variacao'::text                as modelo,
        2                            as pagina,
        'Vendas por construtora (variação %)'::text as quadro,
        edicao,
        "empresa"::text                as linha,
        '12m atual / 12m anterior'::text as coluna,
        case when "12m atual / 12m anterior"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "12m atual / 12m anterior"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_vendas_por_construtora_variacao') }}
    union all
    select
        'gold_boletim_p2_vendas_por_construtora_variacao'::text                as modelo,
        2                            as pagina,
        'Vendas por construtora (variação %)'::text as quadro,
        edicao,
        "empresa"::text                as linha,
        '12m anterior / 12m retrasado'::text as coluna,
        case when "12m anterior / 12m retrasado"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "12m anterior / 12m retrasado"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_vendas_por_construtora_variacao') }}
    union all
    select
        'gold_boletim_p2_totais_das_empresas_levantadas_variacao'::text                as modelo,
        2                            as pagina,
        'Totais das empresas levantadas (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'vs. trim. anterior'::text as coluna,
        case when "vs. trim. anterior"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "vs. trim. anterior"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_totais_das_empresas_levantadas_variacao') }}
    union all
    select
        'gold_boletim_p2_totais_das_empresas_levantadas_variacao'::text                as modelo,
        2                            as pagina,
        'Totais das empresas levantadas (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'vs. mesmo trim. ano ant.'::text as coluna,
        case when "vs. mesmo trim. ano ant."::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "vs. mesmo trim. ano ant."::text::numeric end as valor
    from {{ ref('gold_boletim_p2_totais_das_empresas_levantadas_variacao') }}
    union all
    select
        'gold_boletim_p2_totais_das_empresas_levantadas_variacao'::text                as modelo,
        2                            as pagina,
        'Totais das empresas levantadas (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        '12m atual / 12m anterior'::text as coluna,
        case when "12m atual / 12m anterior"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "12m atual / 12m anterior"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_totais_das_empresas_levantadas_variacao') }}
    union all
    select
        'gold_boletim_p2_totais_das_empresas_levantadas_variacao'::text                as modelo,
        2                            as pagina,
        'Totais das empresas levantadas (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        '12m anterior / 12m retrasado'::text as coluna,
        case when "12m anterior / 12m retrasado"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "12m anterior / 12m retrasado"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_totais_das_empresas_levantadas_variacao') }}
    union all
    select
        'gold_boletim_p2_financiamentos_imobiliarios_bacen'::text                as modelo,
        2                            as pagina,
        'Financiamentos Imobiliários (BACEN)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'PF Concessões (R$ mi)'::text as coluna,
        case when "PF Concessões (R$ mi)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "PF Concessões (R$ mi)"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_financiamentos_imobiliarios_bacen') }}
    union all
    select
        'gold_boletim_p2_financiamentos_imobiliarios_bacen'::text                as modelo,
        2                            as pagina,
        'Financiamentos Imobiliários (BACEN)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'PF Taxa de Juros (%a.a)'::text as coluna,
        case when "PF Taxa de Juros (%a.a)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "PF Taxa de Juros (%a.a)"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_financiamentos_imobiliarios_bacen') }}
    union all
    select
        'gold_boletim_p2_financiamentos_imobiliarios_bacen'::text                as modelo,
        2                            as pagina,
        'Financiamentos Imobiliários (BACEN)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'PF Inadimplência (%)'::text as coluna,
        case when "PF Inadimplência (%)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "PF Inadimplência (%)"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_financiamentos_imobiliarios_bacen') }}
    union all
    select
        'gold_boletim_p2_financiamentos_imobiliarios_bacen'::text                as modelo,
        2                            as pagina,
        'Financiamentos Imobiliários (BACEN)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'PJ Concessões (R$ mi)'::text as coluna,
        case when "PJ Concessões (R$ mi)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "PJ Concessões (R$ mi)"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_financiamentos_imobiliarios_bacen') }}
    union all
    select
        'gold_boletim_p2_financiamentos_imobiliarios_bacen'::text                as modelo,
        2                            as pagina,
        'Financiamentos Imobiliários (BACEN)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'PJ Taxa de Juros (%a.a)'::text as coluna,
        case when "PJ Taxa de Juros (%a.a)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "PJ Taxa de Juros (%a.a)"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_financiamentos_imobiliarios_bacen') }}
    union all
    select
        'gold_boletim_p2_financiamentos_imobiliarios_bacen'::text                as modelo,
        2                            as pagina,
        'Financiamentos Imobiliários (BACEN)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'PJ Inadimplência (%)'::text as coluna,
        case when "PJ Inadimplência (%)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "PJ Inadimplência (%)"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_financiamentos_imobiliarios_bacen') }}
    union all
    select
        'gold_boletim_p2_financiamentos_habitacionais_uh'::text                as modelo,
        2                            as pagina,
        'Financiamentos Habitacionais (UH)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'FGTS-PJ'::text as coluna,
        case when "FGTS-PJ"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "FGTS-PJ"::text::numeric end as valor
    from {{ ref('gold_boletim_p2_financiamentos_habitacionais_uh') }}
    union all
    select
        'gold_boletim_p2_financiamentos_habitacionais_uh'::text                as modelo,
        2                            as pagina,
        'Financiamentos Habitacionais (UH)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'SBPE Const.'::text as coluna,
        case when "SBPE Const."::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "SBPE Const."::text::numeric end as valor
    from {{ ref('gold_boletim_p2_financiamentos_habitacionais_uh') }}
    union all
    select
        'gold_boletim_p3_empregos_construcao_caged'::text                as modelo,
        3                            as pagina,
        'Empregos Construção (CAGED)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Criação Líquida (Saldo)'::text as coluna,
        case when "Criação Líquida (Saldo)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Criação Líquida (Saldo)"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_empregos_construcao_caged') }}
    union all
    select
        'gold_boletim_p3_empregos_construcao_caged'::text                as modelo,
        3                            as pagina,
        'Empregos Construção (CAGED)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Total de Postos (Estoque)'::text as coluna,
        case when "Total de Postos (Estoque)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Total de Postos (Estoque)"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_empregos_construcao_caged') }}
    union all
    select
        'gold_boletim_p3_pnad_continua_ocupados_e_rendimento_medio_re'::text                as modelo,
        3                            as pagina,
        'PNAD Contínua — Ocupados e Rendimento Médio Real'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Ocupados Construção (mil)'::text as coluna,
        case when "Ocupados Construção (mil)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Ocupados Construção (mil)"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_pnad_continua_ocupados_e_rendimento_medio_re') }}
    union all
    select
        'gold_boletim_p3_pnad_continua_ocupados_e_rendimento_medio_re'::text                as modelo,
        3                            as pagina,
        'PNAD Contínua — Ocupados e Rendimento Médio Real'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Ocupados Total (mil)'::text as coluna,
        case when "Ocupados Total (mil)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Ocupados Total (mil)"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_pnad_continua_ocupados_e_rendimento_medio_re') }}
    union all
    select
        'gold_boletim_p3_pnad_continua_ocupados_e_rendimento_medio_re'::text                as modelo,
        3                            as pagina,
        'PNAD Contínua — Ocupados e Rendimento Médio Real'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Rendimento Construção (R$)'::text as coluna,
        case when "Rendimento Construção (R$)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Rendimento Construção (R$)"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_pnad_continua_ocupados_e_rendimento_medio_re') }}
    union all
    select
        'gold_boletim_p3_pnad_continua_ocupados_e_rendimento_medio_re'::text                as modelo,
        3                            as pagina,
        'PNAD Contínua — Ocupados e Rendimento Médio Real'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Rendimento Total (R$)'::text as coluna,
        case when "Rendimento Total (R$)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Rendimento Total (R$)"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_pnad_continua_ocupados_e_rendimento_medio_re') }}
    union all
    select
        'gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia'::text                as modelo,
        3                            as pagina,
        'Produção Industrial e Volume de Vendas (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'PROD mesmo mês ano ant.'::text as coluna,
        case when "PROD mesmo mês ano ant."::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "PROD mesmo mês ano ant."::text::numeric end as valor
    from {{ ref('gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia') }}
    union all
    select
        'gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia'::text                as modelo,
        3                            as pagina,
        'Produção Industrial e Volume de Vendas (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'PROD mês anterior'::text as coluna,
        case when "PROD mês anterior"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "PROD mês anterior"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia') }}
    union all
    select
        'gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia'::text                as modelo,
        3                            as pagina,
        'Produção Industrial e Volume de Vendas (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'PROD mês de referência'::text as coluna,
        case when "PROD mês de referência"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "PROD mês de referência"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia') }}
    union all
    select
        'gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia'::text                as modelo,
        3                            as pagina,
        'Produção Industrial e Volume de Vendas (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'VENDAS mesmo mês ano ant.'::text as coluna,
        case when "VENDAS mesmo mês ano ant."::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "VENDAS mesmo mês ano ant."::text::numeric end as valor
    from {{ ref('gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia') }}
    union all
    select
        'gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia'::text                as modelo,
        3                            as pagina,
        'Produção Industrial e Volume de Vendas (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'VENDAS mês anterior'::text as coluna,
        case when "VENDAS mês anterior"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "VENDAS mês anterior"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia') }}
    union all
    select
        'gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia'::text                as modelo,
        3                            as pagina,
        'Produção Industrial e Volume de Vendas (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'VENDAS mês de referência'::text as coluna,
        case when "VENDAS mês de referência"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "VENDAS mês de referência"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_producao_industrial_e_volume_de_vendas_varia') }}
    union all
    select
        'gold_boletim_p3_novos_financiamentos_imobiliarios_por_banco_'::text                as modelo,
        3                            as pagina,
        'Novos Financiamentos Imobiliários por Banco (acum. no ano)'::text as quadro,
        edicao,
        "banco"::text                as linha,
        'UH acum. ano'::text as coluna,
        case when "UH acum. ano"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "UH acum. ano"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_novos_financiamentos_imobiliarios_por_banco_') }}
    union all
    select
        'gold_boletim_p3_novos_financiamentos_imobiliarios_por_banco_'::text                as modelo,
        3                            as pagina,
        'Novos Financiamentos Imobiliários por Banco (acum. no ano)'::text as quadro,
        edicao,
        "banco"::text                as linha,
        'R$ bi acum. ano'::text as coluna,
        case when "R$ bi acum. ano"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "R$ bi acum. ano"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_novos_financiamentos_imobiliarios_por_banco_') }}
    union all
    select
        'gold_boletim_p3_novos_financiamentos_imobiliarios_por_banco_'::text                as modelo,
        3                            as pagina,
        'Novos Financiamentos Imobiliários por Banco (acum. no ano)'::text as quadro,
        edicao,
        "banco"::text                as linha,
        '% UH'::text as coluna,
        case when "% UH"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "% UH"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_novos_financiamentos_imobiliarios_por_banco_') }}
    union all
    select
        'gold_boletim_p3_novos_financiamentos_imobiliarios_por_banco_'::text                as modelo,
        3                            as pagina,
        'Novos Financiamentos Imobiliários por Banco (acum. no ano)'::text as quadro,
        edicao,
        "banco"::text                as linha,
        'fonte'::text as coluna,
        case when "fonte"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "fonte"::text::numeric end as valor
    from {{ ref('gold_boletim_p3_novos_financiamentos_imobiliarios_por_banco_') }}
    union all
    select
        'gold_boletim_p4_credito_imobiliario_pib'::text                as modelo,
        4                            as pagina,
        'Crédito Imobiliário / PIB (%)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Crédito Imobiliário / PIB'::text as coluna,
        case when "Crédito Imobiliário / PIB"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Crédito Imobiliário / PIB"::text::numeric end as valor
    from {{ ref('gold_boletim_p4_credito_imobiliario_pib') }}
    union all
    select
        'gold_boletim_p4_no_uh_por_condicao_de_uso'::text                as modelo,
        4                            as pagina,
        'Nº UH por Condição de Uso'::text as quadro,
        edicao,
        "fonte"::text                as linha,
        'Trim. ano anterior — UH Usadas'::text as coluna,
        case when "Trim. ano anterior — UH Usadas"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Trim. ano anterior — UH Usadas"::text::numeric end as valor
    from {{ ref('gold_boletim_p4_no_uh_por_condicao_de_uso') }}
    union all
    select
        'gold_boletim_p4_no_uh_por_condicao_de_uso'::text                as modelo,
        4                            as pagina,
        'Nº UH por Condição de Uso'::text as quadro,
        edicao,
        "fonte"::text                as linha,
        'Trim. ano anterior — UH Novas'::text as coluna,
        case when "Trim. ano anterior — UH Novas"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Trim. ano anterior — UH Novas"::text::numeric end as valor
    from {{ ref('gold_boletim_p4_no_uh_por_condicao_de_uso') }}
    union all
    select
        'gold_boletim_p4_no_uh_por_condicao_de_uso'::text                as modelo,
        4                            as pagina,
        'Nº UH por Condição de Uso'::text as quadro,
        edicao,
        "fonte"::text                as linha,
        'Trim. selecionado — UH Usadas'::text as coluna,
        case when "Trim. selecionado — UH Usadas"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Trim. selecionado — UH Usadas"::text::numeric end as valor
    from {{ ref('gold_boletim_p4_no_uh_por_condicao_de_uso') }}
    union all
    select
        'gold_boletim_p4_no_uh_por_condicao_de_uso'::text                as modelo,
        4                            as pagina,
        'Nº UH por Condição de Uso'::text as quadro,
        edicao,
        "fonte"::text                as linha,
        'Trim. selecionado — UH Novas'::text as coluna,
        case when "Trim. selecionado — UH Novas"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Trim. selecionado — UH Novas"::text::numeric end as valor
    from {{ ref('gold_boletim_p4_no_uh_por_condicao_de_uso') }}
    union all
    select
        'gold_boletim_p4_no_uh_por_condicao_de_uso'::text                as modelo,
        4                            as pagina,
        'Nº UH por Condição de Uso'::text as quadro,
        edicao,
        "fonte"::text                as linha,
        'Trim. selecionado — UH Total'::text as coluna,
        case when "Trim. selecionado — UH Total"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Trim. selecionado — UH Total"::text::numeric end as valor
    from {{ ref('gold_boletim_p4_no_uh_por_condicao_de_uso') }}
    union all
    select
        'gold_boletim_p5_sbpe_construcao_unidades_e_valor_acum_no_tri'::text                as modelo,
        5                            as pagina,
        'SBPE Construção — unidades e valor (acum. no trimestre)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'Trim. ano anterior'::text as coluna,
        case when "Trim. ano anterior"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Trim. ano anterior"::text::numeric end as valor
    from {{ ref('gold_boletim_p5_sbpe_construcao_unidades_e_valor_acum_no_tri') }}
    union all
    select
        'gold_boletim_p5_sbpe_construcao_unidades_e_valor_acum_no_tri'::text                as modelo,
        5                            as pagina,
        'SBPE Construção — unidades e valor (acum. no trimestre)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'Trim. selecionado'::text as coluna,
        case when "Trim. selecionado"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Trim. selecionado"::text::numeric end as valor
    from {{ ref('gold_boletim_p5_sbpe_construcao_unidades_e_valor_acum_no_tri') }}
    union all
    select
        'gold_boletim_p5_sbpe_construcao_unidades_e_valor_acum_no_tri'::text                as modelo,
        5                            as pagina,
        'SBPE Construção — unidades e valor (acum. no trimestre)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'Variação %'::text as coluna,
        case when "Variação %"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Variação %"::text::numeric end as valor
    from {{ ref('gold_boletim_p5_sbpe_construcao_unidades_e_valor_acum_no_tri') }}
    union all
    select
        'gold_boletim_p5_saldo_caderneta_de_poupanca_captacao_liquida'::text                as modelo,
        5                            as pagina,
        'Saldo Caderneta de Poupança — Captação Líquida (R$ bi)'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Cap. Líq. (Bi)'::text as coluna,
        case when "Cap. Líq. (Bi)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Cap. Líq. (Bi)"::text::numeric end as valor
    from {{ ref('gold_boletim_p5_saldo_caderneta_de_poupanca_captacao_liquida') }}
    union all
    select
        'gold_boletim_p5_financiamento_pf_mcmv_por_faixa'::text                as modelo,
        5                            as pagina,
        'Financiamento PF MCMV por faixa'::text as quadro,
        edicao,
        "faixa"::text                as linha,
        'Trim. ano anterior — Nº UH'::text as coluna,
        case when "Trim. ano anterior — Nº UH"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Trim. ano anterior — Nº UH"::text::numeric end as valor
    from {{ ref('gold_boletim_p5_financiamento_pf_mcmv_por_faixa') }}
    union all
    select
        'gold_boletim_p5_financiamento_pf_mcmv_por_faixa'::text                as modelo,
        5                            as pagina,
        'Financiamento PF MCMV por faixa'::text as quadro,
        edicao,
        "faixa"::text                as linha,
        'Trim. ano anterior — FIN (Bi R$)'::text as coluna,
        case when "Trim. ano anterior — FIN (Bi R$)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Trim. ano anterior — FIN (Bi R$)"::text::numeric end as valor
    from {{ ref('gold_boletim_p5_financiamento_pf_mcmv_por_faixa') }}
    union all
    select
        'gold_boletim_p5_financiamento_pf_mcmv_por_faixa'::text                as modelo,
        5                            as pagina,
        'Financiamento PF MCMV por faixa'::text as quadro,
        edicao,
        "faixa"::text                as linha,
        'Trim. selecionado — Nº UH'::text as coluna,
        case when "Trim. selecionado — Nº UH"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Trim. selecionado — Nº UH"::text::numeric end as valor
    from {{ ref('gold_boletim_p5_financiamento_pf_mcmv_por_faixa') }}
    union all
    select
        'gold_boletim_p5_financiamento_pf_mcmv_por_faixa'::text                as modelo,
        5                            as pagina,
        'Financiamento PF MCMV por faixa'::text as quadro,
        edicao,
        "faixa"::text                as linha,
        'Trim. selecionado — FIN (Bi R$)'::text as coluna,
        case when "Trim. selecionado — FIN (Bi R$)"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Trim. selecionado — FIN (Bi R$)"::text::numeric end as valor
    from {{ ref('gold_boletim_p5_financiamento_pf_mcmv_por_faixa') }}
    union all
    select
        'gold_boletim_p6_sinapi_brasil_e_incc_m'::text                as modelo,
        6                            as pagina,
        'SINAPI (Brasil) e INCC-M'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'SINAPI'::text as coluna,
        case when "SINAPI"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "SINAPI"::text::numeric end as valor
    from {{ ref('gold_boletim_p6_sinapi_brasil_e_incc_m') }}
    union all
    select
        'gold_boletim_p6_sinapi_brasil_e_incc_m'::text                as modelo,
        6                            as pagina,
        'SINAPI (Brasil) e INCC-M'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'INCC-M'::text as coluna,
        case when "INCC-M"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "INCC-M"::text::numeric end as valor
    from {{ ref('gold_boletim_p6_sinapi_brasil_e_incc_m') }}
    union all
    select
        'gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc'::text                as modelo,
        6                            as pagina,
        'Ticket médio das unidades lançadas vs. INCC'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'INCC trimestral'::text as coluna,
        case when "INCC trimestral"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "INCC trimestral"::text::numeric end as valor
    from {{ ref('gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc') }}
    union all
    select
        'gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc'::text                as modelo,
        6                            as pagina,
        'Ticket médio das unidades lançadas vs. INCC'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'MRV trimestral'::text as coluna,
        case when "MRV trimestral"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "MRV trimestral"::text::numeric end as valor
    from {{ ref('gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc') }}
    union all
    select
        'gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc'::text                as modelo,
        6                            as pagina,
        'Ticket médio das unidades lançadas vs. INCC'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Direcional trimestral'::text as coluna,
        case when "Direcional trimestral"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Direcional trimestral"::text::numeric end as valor
    from {{ ref('gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc') }}
    union all
    select
        'gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc'::text                as modelo,
        6                            as pagina,
        'Ticket médio das unidades lançadas vs. INCC'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Tenda trimestral'::text as coluna,
        case when "Tenda trimestral"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Tenda trimestral"::text::numeric end as valor
    from {{ ref('gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc') }}
    union all
    select
        'gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc'::text                as modelo,
        6                            as pagina,
        'Ticket médio das unidades lançadas vs. INCC'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'INCC acum. 4T20'::text as coluna,
        case when "INCC acum. 4T20"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "INCC acum. 4T20"::text::numeric end as valor
    from {{ ref('gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc') }}
    union all
    select
        'gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc'::text                as modelo,
        6                            as pagina,
        'Ticket médio das unidades lançadas vs. INCC'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'MRV acum. 4T20'::text as coluna,
        case when "MRV acum. 4T20"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "MRV acum. 4T20"::text::numeric end as valor
    from {{ ref('gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc') }}
    union all
    select
        'gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc'::text                as modelo,
        6                            as pagina,
        'Ticket médio das unidades lançadas vs. INCC'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Direcional acum. 4T20'::text as coluna,
        case when "Direcional acum. 4T20"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Direcional acum. 4T20"::text::numeric end as valor
    from {{ ref('gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc') }}
    union all
    select
        'gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc'::text                as modelo,
        6                            as pagina,
        'Ticket médio das unidades lançadas vs. INCC'::text as quadro,
        edicao,
        "periodo"::text                as linha,
        'Tenda acum. 4T20'::text as coluna,
        case when "Tenda acum. 4T20"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Tenda acum. 4T20"::text::numeric end as valor
    from {{ ref('gold_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc') }}
    union all
    select
        'gold_boletim_p7_indices_da_construcao_variacao'::text                as modelo,
        7                            as pagina,
        'Índices da Construção (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'Índice IMOB'::text as coluna,
        case when "Índice IMOB"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Índice IMOB"::text::numeric end as valor
    from {{ ref('gold_boletim_p7_indices_da_construcao_variacao') }}
    union all
    select
        'gold_boletim_p7_indices_da_construcao_variacao'::text                as modelo,
        7                            as pagina,
        'Índices da Construção (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'Índice ABRAMAT'::text as coluna,
        case when "Índice ABRAMAT"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Índice ABRAMAT"::text::numeric end as valor
    from {{ ref('gold_boletim_p7_indices_da_construcao_variacao') }}
    union all
    select
        'gold_boletim_p7_indices_da_construcao_variacao'::text                as modelo,
        7                            as pagina,
        'Índices da Construção (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'Índice FipeZap'::text as coluna,
        case when "Índice FipeZap"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Índice FipeZap"::text::numeric end as valor
    from {{ ref('gold_boletim_p7_indices_da_construcao_variacao') }}
    union all
    select
        'gold_boletim_p7_indices_da_construcao_variacao'::text                as modelo,
        7                            as pagina,
        'Índices da Construção (variação %)'::text as quadro,
        edicao,
        "indicador"::text                as linha,
        'Índice ICST'::text as coluna,
        case when "Índice ICST"::text ~ '^-?[0-9]*\.?[0-9]+$'
             then "Índice ICST"::text::numeric end as valor
    from {{ ref('gold_boletim_p7_indices_da_construcao_variacao') }}
