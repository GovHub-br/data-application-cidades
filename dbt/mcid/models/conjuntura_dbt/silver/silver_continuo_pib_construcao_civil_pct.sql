{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: PIB da construção civil em % de crescimento.
-- Página 1 do boletim (trim/trim imediatamente anterior, acumulada no ano,
-- acumulada em 4 trimestres).
--
-- Vem do SIDRA (agregado 5932, Contas Nacionais Trimestrais, categoria
-- Construção), não da tabela manual que alimentava este modelo até 01/09/2026.
--
-- A tabela manual existia porque o boletim usa a série DESSAZONALIZADA, e se
-- supunha que ela não viesse da API. Vem: a variável 6564, "Taxa trimestre
-- contra trimestre imediatamente anterior", é justamente a dessazonalizada —
-- é assim que as Contas Nacionais Trimestrais publicam essa comparação. As
-- duas acumuladas conferem período a período com o que estava digitado.
--
-- A trim/trim difere em alguns trimestres (4T2024: 1,9 na manual, 2,8 no
-- SIDRA) porque o IBGE revisa a dessazonalização a cada divulgação, e a
-- tabela manual congelou vintages antigos. Ficamos com a série corrente; o
-- que cada edição publicou continua nos snapshots.
--
-- Mantém os nomes de coluna da tabela manual: o gold já os consome, e
-- renomear aqui espalharia a mudança sem ganho nenhum.

select
    (trimestre::text || 'T' || ano::text)                       as periodo,
    trimestre,
    ano,
    tri                                                         as pib_const_trimestre_anterior,
    acum_ano                                                    as pib_const_taxa_acumulada_ano,
    acum_4t                                                     as pib_const_taxa_acumulada_4_trimestres
from (
    select
        left(periodo, 4)::int                                   as ano,
        right(periodo, 2)::int                                  as trimestre,
        max(valor) filter (where variavel like 'Taxa trimestre contra%')          as tri,
        max(valor) filter (where variavel like 'Taxa acumulada ao longo do ano%') as acum_ano,
        max(valor) filter (where variavel like 'Taxa acumulada em quatro%')       as acum_4t
    from {{ ref('silver_continuo_ibge_pib_construcao_civil') }}
    group by 1, 2
) s
