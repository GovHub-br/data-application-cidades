{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Faixa 3 Fundo Social (MCMV) — UH e valor,
-- mensal e trimestral. Página 5 do boletim, linha "Faixa 3 Fundo Social".
--
-- Fonte automatizada em 2026-08-30 (remessas semanais da GEFUS). Até então o
-- indicador não existia no pipeline: o comentário do
-- `gld_financiamento_pf_faixa` registrava que a base do GEAVO "não
-- distingue Fundo Social" — de fato não distingue, porque é **outra fonte**.
--
-- Validado contra o boletim 1T2026: 29.094 UH e R$ 6,03 bi, contra 29.093 e
-- R$ 6,03 bi publicados. A diferença de 1 UH é revisão de safra já observada
-- pelo Codex (a remessa de 22/05 trazia 19.437 usadas; a de 21/08 traz
-- 19.438).
--
-- ⚠️ Fica em gold SEPARADO, e não somado ao `gld_financiamento_pf_faixa`,
-- de propósito: as duas fontes são complementares e a interseção de contratos
-- é zero, mas juntá-las num só model convidaria alguém a deduplicar ou somar
-- indevidamente. Quem precisar da tabela completa da Página 5 combina as duas
-- no dashboard, com a decisão explícita.

with mensal as (
    select
        ano, mes, data_referencia, condicao_imovel, uh, valor
    from {{ ref('slv_gefus_fundo_social') }}
)

select
    ano || '-' || lpad(mes::text, 2, '0')                as periodo,
    ano,
    mes,
    data_referencia,
    (floor((mes - 1) / 3))::int + 1                      as trimestre,

    sum(uh)                                              as fundo_social_uh_total,
    sum(valor)                                           as fundo_social_valor_total,

    sum(uh)    filter (where condicao_imovel = 'Novo')   as fundo_social_uh_novos,
    sum(valor) filter (where condicao_imovel = 'Novo')   as fundo_social_valor_novos,
    sum(uh)    filter (where condicao_imovel = 'Usado')  as fundo_social_uh_usados,
    sum(valor) filter (where condicao_imovel = 'Usado')  as fundo_social_valor_usados,

    -- mantido visível de propósito: entra no total publicado, mas não tem
    -- regra que permita chamá-lo de novo ou usado
    sum(uh)    filter (where condicao_imovel = 'Não classificado') as fundo_social_uh_nao_classificado,
    sum(valor) filter (where condicao_imovel = 'Não classificado') as fundo_social_valor_nao_classificado
from mensal
group by 1, 2, 3, 4, 5
order by ano desc, mes desc
