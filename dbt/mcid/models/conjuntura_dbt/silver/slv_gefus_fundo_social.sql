{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: Faixa 3 Fundo Social (MCMV), remessas GEFUS.
-- Linha "Faixa 3 Fundo Social" da tabela de Financiamento PF do boletim
-- (Página 5).
--
-- Agrega por mês e condição do imóvel. `dt_evento` chega como DD/MM/YYYY e
-- `vr_evento` em formato pt-BR, por isso a conversão explícita.
--
-- Sobre `tipo_imovel`: a origem usa 1 = novo, 2 = usado e 5 = um terceiro
-- código SEM regra de negócio formal conhecida. O 5 NÃO é classificado como
-- novo nem usado — mas **entra no total**, e isso não é escolha nossa: o
-- boletim publica 29.093 UH no 1T2026, e 9.102 (tipo 1) + 19.438 (tipo 2)
-- + 554 (tipo 5) = 29.094. Sem o tipo 5 o total não fecha.
--
-- ⚠️ Esta fonte é COMPLEMENTAR à Base PF do GEAVO, nunca duplicada: a
-- verificação de chaves feita em 2026-08-29 mostrou interseção ZERO entre as
-- duas. Não deduplicar uma contra a outra.

with base as (
    select
        strptime(dt_evento::text, '%d/%m/%Y')::date        as data_evento,
        tipo_imovel::text                                  as tipo_codigo,
        replace(vr_evento::text, ',', '.')::numeric        as valor_evento
    from {{ ref('bnz_gefus_fundo_social') }}
    where dt_evento is not null
)

select
    extract(year from data_evento)::int   as ano,
    extract(month from data_evento)::int  as mes,
    make_date(extract(year from data_evento)::int,
              extract(month from data_evento)::int, 1) as data_referencia,
    case tipo_codigo
        when '1' then 'Novo'
        when '2' then 'Usado'
        else 'Não classificado'
    end                                    as condicao_imovel,
    tipo_codigo,
    count(*)                               as uh,
    sum(valor_evento)                      as valor,
    current_timestamp                      as dt_silver
from base
group by 1, 2, 3, 4, 5
