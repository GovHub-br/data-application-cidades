{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 7: Índices da Construção (variação %)
-- Seção do impresso: 8. Índices da Construção
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "indicador", "Índice IMOB", "Índice ABRAMAT", "Índice FipeZap", "Índice ICST"
from (

    with 
edicoes as (
    select distinct periodo as edicao,
           (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
           right(periodo, 4)::int as ano_ed,
           left(periodo, 1)::int  as tri_ed
    from {{ ref('gold_continuo_pib_construcao_civil_pct') }}
    where right(periodo, 4)::int >= 2025
),
    ref as (select edicao, ano_ed, tri_ed, ano_ed * 12 + tri_ed * 3 as m0 from edicoes),
    imob as (
        select (left(periodo, 4)::int * 12 + right(periodo, 2)::int) as m,
               indice_imob_var_mes a, indice_imob_var_mes_vs_mes_ano_ant b, indice_imob_var_acum_ano c
        from {{ ref('gold_continuo_indice_imob') }}
    ),
    fipe as (
        select (left(periodo, 4)::int * 12 + right(periodo, 2)::int) as m,
               indice_fipezap_locacao_var_mes a, indice_fipezap_locacao_var_mes_vs_mes_ano_ant b,
               indice_fipezap_locacao_acum_ano c
        from {{ ref('gold_continuo_fipezap') }}
    ),
    abramat as (
        select (ano::int * 12 + mes::int) as m,
               indice_abramat_var_mes a, indice_abramat_var_mes_vs_mes_ano_ant b,
               indice_abramat_var_acum_ano c
        from manual_conjuntura.dados_mensais
    ),
    icst as (
        select (right(periodo, 4)::int * 12 + left(periodo, 2)::int) as m,
               indice_icst_var_mes_com_ajuste a, indice_icst_var_mes_vs_mes_ano_ant_com_ajuste b,
               icst_com_ajuste_sazonal ix
        from {{ ref('gold_continuo_icst') }}
    )
    select r.edicao, 'Mês de ref. vs. mês anterior' as indicador,
           round((select a from imob where m = r.m0)::numeric * 100, 1) as "Índice IMOB",
           round((select a from abramat where m = r.m0)::numeric * 100, 1) as "Índice ABRAMAT",
           round((select a from fipe where m = r.m0)::numeric * 100, 1) as "Índice FipeZap",
           round((select a from icst where m = r.m0)::numeric * 100, 1) as "Índice ICST", 1 as ordem
    from ref r
    union all
    select r.edicao, 'Mês de ref. vs. mesmo mês do ano ant.',
           round((select b from imob where m = r.m0)::numeric * 100, 1),
           round((select b from abramat where m = r.m0)::numeric * 100, 1),
           round((select b from fipe where m = r.m0)::numeric * 100, 1),
           round((select b from icst where m = r.m0)::numeric * 100, 1), 2
    from ref r
    union all
    select r.edicao, 'Acumulado no ano',
           round((select c from imob where m = r.m0)::numeric * 100, 1),
           round((select c from abramat where m = r.m0)::numeric * 100, 1),
           round((select c from fipe where m = r.m0)::numeric * 100, 1),
           round(((select ix from icst where m = r.m0)
                  / nullif((select ix from icst where m = r.ano_ed * 12), 0) - 1)::numeric * 100, 1), 3
    from ref r
    union all
    select r.edicao, 'Acumulado no ano anterior',
           round((select c from imob where m = r.m0 - 12)::numeric * 100, 1),
           round((select c from abramat where m = r.m0 - 12)::numeric * 100, 1),
           round((select c from fipe where m = r.m0 - 12)::numeric * 100, 1),
           round(((select ix from icst where m = r.m0 - 12)
                  / nullif((select ix from icst where m = (r.ano_ed - 1) * 12), 0) - 1)::numeric * 100, 1), 4
    from ref r
    
) q
order by edicao, ordem
