{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Financiamento PF por Faixa de Renda (UH e R$)
-- — Faixa 1, 2, 3, Classe Média. Páginas 4 e 5.
--
-- Fonte AUTOMATIZADA (2026-08-25): sistema GEAVO da Caixa, via
-- silver_continuo_geavo_fgts_pf — substitui a planilha manual (CEAG).
--
-- Diferenças conscientes em relação à versão manual antiga:
--   - Não reproduz o corte "Faixa 3 com/sem Fundo Social" (a fonte nova
--     não distingue isso no campo `faixa`) nem a linha "Fora MCMV (Apoio
--     e CCI)" (excluída de propósito — ver comentário na silver, incluiria
--     ~2,3 milhões de contratos de crédito individual comum, sem relação
--     com faixa MCMV). Se precisar desses cortes de volta, é dado
--     adicional a investigar, não só reescrever a query.
--   - `faixa` = G1/G2/G3 mapeado pra Faixa 1/2/3 — leitura mais óbvia do
--     campo, ainda não confirmada contra uma tabela de domínio oficial.

with base as (
    select
        ano, mes, faixa, tipo_imovel, qtd_unidades, valor_emprestimo
    from {{ ref('silver_continuo_geavo_fgts_pf') }}
)

select
    ano || '-' || lpad(mes::text, 2, '0')                              as periodo,
    ano,
    mes,
    make_date(ano, mes, 1)                                             as data_referencia,

    sum(qtd_unidades)                                                  as financiamento_pf_uh_total_geral,
    sum(valor_emprestimo)                                              as financiamento_pf_valor_total_geral,

    sum(qtd_unidades) filter (where faixa = 'Faixa 1')                 as financiamento_pf_uh_total_faixa_1,
    sum(valor_emprestimo) filter (where faixa = 'Faixa 1')             as financiamento_pf_valor_total_faixa_1,
    sum(qtd_unidades) filter (where faixa = 'Faixa 2')                 as financiamento_pf_uh_total_faixa_2,
    sum(valor_emprestimo) filter (where faixa = 'Faixa 2')             as financiamento_pf_valor_total_faixa_2,
    sum(qtd_unidades) filter (where faixa = 'Faixa 3')                 as financiamento_pf_uh_total_faixa_3,
    sum(valor_emprestimo) filter (where faixa = 'Faixa 3')             as financiamento_pf_valor_total_faixa_3,
    sum(qtd_unidades) filter (where faixa = 'Classe Média')            as financiamento_pf_uh_total_classe_media,
    sum(valor_emprestimo) filter (where faixa = 'Classe Média')        as financiamento_pf_valor_total_classe_media,

    sum(qtd_unidades) filter (where tipo_imovel = 'Novo')              as financiamento_pf_uh_novo,
    sum(qtd_unidades) filter (where tipo_imovel = 'Usado')             as financiamento_pf_uh_usado
from base
group by ano, mes
order by ano desc, mes desc
