{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: OGU — dotação, empenho, pagamento, restos a
-- pagar (inscrito e pago) por ação orçamentária, no mesmo formato da tabela
-- do boletim (página 6), com linha SOMA. Fonte: SIAFI/Tesouro Gerencial
-- (e-mail), full-refresh — reflete sempre a posição (YTD) da extração mais
-- recente recebida. Filtra as 7 ações orçamentárias do boletim (habitação
-- MCMV/FAR/FDS). Validado vs boletim 4T25 (posição 02/01/26): Empenho e
-- Pagamento batem próximo (~2% de diferença). Dotação da ação 00XF fica
-- zerada — é operação de crédito reembolsável (MCMV/FGTS) fora do OGU
-- `dt_referencia_extracao`: data do dt_ingest do SIAFI — como a tabela é
-- "posição mais recente" (não um corte fixo por trimestre), sem essa data
-- os valores parecem discrepantes de qualquer boletim antigo (pedido da
-- reunião de 2026-08-24: "colocar a data pro OGU execução orçamentária").
-- tradicional, sem linha de dotação orçamentária no SIAFI/MCID (não é bug).

with acoes_boletim as (

    select *
    from {{ ref('silver_continuo_siafi_dotacao_execucao') }}
    where acao_governo_codigo in ('00AF', '00CY', '00CX', '00TI', '00CW', '0E64', '00XF')

),

parsed as (

    select
        acao_governo_codigo,
        case acao_governo_codigo
            when '00AF' then 'FAR'
            when '00CY' then 'FDS'
            when '00CX' then 'PNHR'
            when '00TI' then 'FNHIS'
            when '00CW' then 'PNHU'
            when '0E64' then 'OFERTA P.'
            when '00XF' then 'FUNDO SOC.'
        end as acao_nome,
        case acao_governo_codigo
            when '00AF' then 1 when '00CY' then 2 when '00CX' then 3
            when '00TI' then 4 when '00CW' then 5 when '0E64' then 6
            when '00XF' then 7
        end as ordem,
        sum(nullif(replace(replace(dotacao_atualizada, '.', ''), ',', '.'), '')::numeric)      as dotacao_atualizada,
        sum(nullif(replace(replace(despesas_empenhadas, '.', ''), ',', '.'), '')::numeric)      as empenho,
        sum(nullif(replace(replace(despesas_pagas, '.', ''), ',', '.'), '')::numeric)           as pagamento,
        sum(nullif(replace(replace(restos_a_pagar_inscritos, '.', ''), ',', '.'), '')::numeric)  as rap_inscrito,
        sum(nullif(replace(replace(restos_a_pagar_pagos, '.', ''), ',', '.'), '')::numeric)      as pag_rap,
        max(dt_ingest::timestamp)::date                                                          as dt_referencia_extracao
    from acoes_boletim
    group by acao_governo_codigo

),

com_total as (

    select
        acao_governo_codigo, acao_nome, ordem,
        dotacao_atualizada, empenho, pagamento, rap_inscrito, pag_rap,
        coalesce(pagamento, 0) + coalesce(pag_rap, 0) as pag_total,
        dt_referencia_extracao
    from parsed

    union all

    select
        'SOMA', 'SOMA', 8,
        sum(dotacao_atualizada), sum(empenho), sum(pagamento), sum(rap_inscrito), sum(pag_rap),
        sum(coalesce(pagamento, 0) + coalesce(pag_rap, 0)),
        max(dt_referencia_extracao)
    from parsed

)

select
    acao_governo_codigo,
    acao_nome,
    dotacao_atualizada,
    empenho,
    pagamento,
    rap_inscrito,
    pag_rap,
    pag_total,
    dt_referencia_extracao
from com_total
order by ordem
