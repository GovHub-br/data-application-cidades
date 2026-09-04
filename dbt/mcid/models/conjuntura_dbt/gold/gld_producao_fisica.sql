{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Produção Física (PIM-PF) e Vendas no varejo de
-- material de construção (PMC). Variações publicadas pelo próprio IBGE (não
-- calculadas por nós). Página 3, seção 5.
-- Fonte: IBGE API (automatizado). PIM-PF: agregado 8886 (insumos típicos da
-- construção civil) — tabela dedicada, sem variante com ajuste sazonal, por
-- isso pim_pf_var_mes usa 11602 (mês/mesmo mês do ano anterior), a única
-- variação mensal que essa série publica. PMC: agregado 8757, categoria
-- 56732 (volume de vendas) — usa 11708, com ajuste sazonal, pois essa tabela
-- publica a variante sazonalizada mês a mês.

with pim as (
    select
        periodo,
        max(case when variavel_id = 11602 then valor end) as pim_pf_var_mes,
        max(case when variavel_id = 11603 then valor end) as pim_pf_var_acum_ano,
        max(case when variavel_id = 11604 then valor end) as pim_pf_var_12_meses
    from {{ ref('slv_ibge_pim_pf_brasil') }}
    group by periodo
),

pmc as (
    select
        periodo,
        max(case when variavel_id = 11708 then valor end) as pmc_var_mes,
        max(case when variavel_id = 11710 then valor end) as pmc_var_acum_ano,
        max(case when variavel_id = 11711 then valor end) as pmc_var_12_meses
    from {{ ref('slv_ibge_pmc_construcao') }}
    -- 56732 = "Índice de volume de vendas de materiais de construção",
    -- do agregado 8757 ("...vendas de materiais de construção").
    --
    -- ⚠️ ESTE FILTRO NÃO CASA NADA HOJE, e isso é proposital: a ingestão está
    -- puxando o agregado ERRADO do SIDRA, e as colunas do PMC ficam nulas.
    -- Nulo é o comportamento correto enquanto a fonte estiver errada —
    -- preencher com outra categoria produziria número plausível e falso.
    --
    -- Em 2026-08-30 eu troquei este filtro para 56734 achando que 56732 era
    -- erro de digitação, porque 56732 não existia na silver. Estava errado:
    -- 56732 é o código certo, e some da silver justamente porque vem de outro
    -- agregado. A troca fez as colunas preencherem com volume de vendas do
    -- VAREJO GERAL — trocou falha visível por dado errado silencioso.
    -- Revertido no mesmo dia.
    --
    -- Correção de verdade: acertar o agregado na Variable
    -- `IBGE_CONFIGURACOES` (8757, categoria 56732) — ver a descrição da
    -- fonte `ibge_pmc_construcao` em models/sources.yml.
    where categoria_id = 56732
    group by periodo
)

select
    coalesce(pim.periodo, pmc.periodo) as periodo,
    to_date(coalesce(pim.periodo, pmc.periodo), 'YYYYMM') as data_referencia,
    pim.pim_pf_var_mes,
    pim.pim_pf_var_acum_ano,
    pim.pim_pf_var_12_meses,
    pmc.pmc_var_mes,
    pmc.pmc_var_acum_ano,
    pmc.pmc_var_12_meses
from pim
full outer join pmc on pim.periodo = pmc.periodo
order by periodo desc
