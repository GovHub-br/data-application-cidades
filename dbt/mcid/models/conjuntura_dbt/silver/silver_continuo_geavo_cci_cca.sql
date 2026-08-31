{{ config(materialized='table') }}

-- Silver: Financiamento PF por faixa do MCMV — Carta de Crédito Individual
-- (CCI) + Associativo (CCA), do Canal FGTS/GEAVO. Página 5 do boletim.
--
-- ⚠️ Esta é a fonte CERTA do indicador. Até 2026-08-30 o gold usava
-- `Base_PF_FGTS`, que é outro recorte do mesmo Canal FGTS e **não reproduz o
-- publicado** (dava Faixa 1 pela metade e Faixa 2 uma vez e meia). O boletim
-- declara "Fonte: Canal FGTS", e é o CCI+CCA que fecha.
--
-- O campo que resolve é `compatibilidade_faixa_novo_mcmv`, que já traz a
-- faixa do MCMV — não é preciso deduzir por faixa de renda. Os códigos têm
-- sufixo (`1D`, `1DE`, `2`, `2E`), então o agrupamento é pelo PRIMEIRO
-- caractere.
--
-- Validado contra o boletim 1T2026:
--   Faixa 1  61.069 / R$ 8,20 bi   (publicado 61.082 / 8,21)
--   Faixa 2  42.520 / R$ 7,23 bi   (publicado 42.514 / 7,23)
--   Faixa 3  26.879 / R$ 6,12 bi   (publicado 26.903 / 6,12)
--   Classe M 11.519 / R$ 3,06 bi   (publicado 11.664 / 3,10)
-- As diferenças de 6 a 145 UH são revisão entre a safra do boletim (jun/26) e
-- a nossa (21/08/26).
--
-- `datadacontratacao` vem em MM/DD/YY na origem e é inconsistente; por isso o
-- período sai de `anomescontratacao` (AAAAMM), que é confiável.

with unificado as (
    select
        anomescontratacao::text                                  as ano_mes,
        compatibilidade_faixa_novo_mcmv::text                     as faixa_codigo,
        pmcmv::text                                               as e_mcmv,
        modalidade::text                                          as modalidade,
        replace(vlrdofinanciamento::text, ',', '.')::numeric       as valor_financiamento,
        'CCI'                                                     as carteira
    from {{ ref('bronze_continuo_geavo_cci_analitico') }}

    union all

    select
        anomescontratacao::text,
        compatibilidade_faixa_novo_mcmv::text,
        pmcmv::text,
        modalidade::text,
        replace(vlrdofinanciamento::text, ',', '.')::numeric,
        'CCA'
    from {{ ref('bronze_continuo_geavo_cca_analitico') }}
)

select
    left(ano_mes, 4)::int                    as ano,
    right(ano_mes, 2)::int                   as mes,
    make_date(left(ano_mes, 4)::int, right(ano_mes, 2)::int, 1) as data_referencia,
    carteira,
    faixa_codigo,
    case left(faixa_codigo, 1)
        when '1' then 'Faixa 1'
        when '2' then 'Faixa 2'
        when '3' then 'Faixa 3'
        when '4' then 'Classe Média'
        else 'Não classificado'
    end                                       as faixa,
    e_mcmv,
    modalidade,
    count(*)                                  as uh,
    sum(valor_financiamento)                  as valor,
    current_timestamp                         as dt_silver
from unificado
where ano_mes is not null and ano_mes <> ''
group by 1, 2, 3, 4, 5, 6, 7, 8
