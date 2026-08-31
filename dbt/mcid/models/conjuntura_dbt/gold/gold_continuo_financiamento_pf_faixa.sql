{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Financiamento PF MCMV por faixa de renda —
-- UH e R$. Página 5 do boletim ("Fonte: Canal FGTS").
--
-- 🔄 REESCRITO em 2026-08-30. A versão anterior lia `Base_PF_FGTS` e **não
-- reproduzia o publicado**: dava Faixa 1 pela metade (30.515 contra 61.082) e
-- Faixa 2 uma vez e meia (65.727 contra 42.514). O `Base_PF_FGTS` é outro
-- recorte do mesmo Canal FGTS; quem fecha é o CCI + CCA analítico, que traz
-- a faixa do MCMV pronta em `compatibilidade_faixa_novo_mcmv` — sem precisar
-- deduzir por faixa de renda, como a versão antiga fazia com G1/G2/G3.
--
-- Validado contra o boletim 1T2026 (ver silver). A linha "Faixa 3 Fundo
-- Social" do boletim NÃO entra aqui: é outra fonte (GEFUS), e está em
-- `gold_continuo_fundo_social`.

with base as (
    select ano, mes, data_referencia, faixa, uh, valor
    from {{ ref('silver_continuo_geavo_cci_cca') }}
)

select
    ano || '-' || lpad(mes::text, 2, '0')                         as periodo,
    ano,
    mes,
    data_referencia,
    (floor((mes - 1) / 3))::int + 1                               as trimestre,

    sum(uh)                                                       as financiamento_pf_uh_total,
    sum(valor)                                                    as financiamento_pf_valor_total,

    sum(uh)    filter (where faixa = 'Faixa 1')                   as financiamento_pf_uh_faixa_1,
    sum(valor) filter (where faixa = 'Faixa 1')                   as financiamento_pf_valor_faixa_1,
    sum(uh)    filter (where faixa = 'Faixa 2')                   as financiamento_pf_uh_faixa_2,
    sum(valor) filter (where faixa = 'Faixa 2')                   as financiamento_pf_valor_faixa_2,
    sum(uh)    filter (where faixa = 'Faixa 3')                   as financiamento_pf_uh_faixa_3,
    sum(valor) filter (where faixa = 'Faixa 3')                   as financiamento_pf_valor_faixa_3,
    sum(uh)    filter (where faixa = 'Classe Média')              as financiamento_pf_uh_classe_media,
    sum(valor) filter (where faixa = 'Classe Média')              as financiamento_pf_valor_classe_media,

    -- contratos sem faixa MCMV atribuída: ficam visíveis em vez de sumirem
    -- numa soma, porque entram no total e o boletim não os detalha
    sum(uh)    filter (where faixa = 'Não classificado')          as financiamento_pf_uh_nao_classificado,
    sum(valor) filter (where faixa = 'Não classificado')          as financiamento_pf_valor_nao_classificado
from base
group by 1, 2, 3, 4, 5
order by ano desc, mes desc
