{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 6: OGU — execução orçamentária
-- Seção do impresso: 6. OGU
--
-- ATENÇÃO — este quadro é diferente dos outros 21.
--
-- Todos os demais são SÉRIE: uma linha por edição, e o filtro do Superset
-- escolhe o trimestre. O OGU não é: `gold_continuo_ogu` guarda UMA extração,
-- identificada por `dt_referencia_extracao`, sem histórico por trimestre.
--
-- Por isso a edição aqui é um cross join, não um recorte: o mesmo retrato
-- aparece em qualquer edição selecionada. Fingir série faria o filtro parecer
-- funcionar e devolver sempre o mesmo número sem dizer por quê.
--
-- A data da extração vai NA TABELA, como coluna, para quem lê saber de quando
-- é o retrato. No impresso ela aparece no cabeçalho ("Dados de 02/01/26").
--
-- Valores em milhões de reais, como no boletim.

with edicoes as (
    select distinct edicao
    from {{ ref('gold_boletim_p1_pib_construcao_civil_em_de_crescimento') }}
),

execucao as (
    select
        acao_governo_codigo,
        acao_nome,
        round(dotacao_atualizada / 1000000.0, 1)  as dotacao_atual,
        round(empenho / 1000000.0, 1)             as empenho,
        round(pagamento / 1000000.0, 1)           as pagamento,
        round(rap_inscrito / 1000000.0, 1)        as rap_inscrito,
        round(pag_rap / 1000000.0, 1)             as pag_rap,
        round(pag_total / 1000000.0, 1)           as pag_total,
        dt_referencia_extracao,
        -- SOMA por último; o resto na ordem em que o boletim imprime
        case when acao_governo_codigo = 'SOMA' then 1 else 0 end as e_total
    from {{ ref('gold_continuo_ogu') }}
)

select
    e.edicao                                              as "edicao",
    x.acao_governo_codigo                                 as "Ação",
    x.acao_nome                                           as "Projeto / Atividade",
    x.dotacao_atual                                       as "Dotação atual",
    x.empenho                                             as "Empenho",
    x.pagamento                                           as "Pagamento",
    x.rap_inscrito                                        as "RAP inscrito",
    x.pag_rap                                             as "Pag. RAP",
    x.pag_total                                           as "Pag. total",
    x.dt_referencia_extracao                              as "Extração"
from execucao x
cross join edicoes e
order by e.edicao, x.e_total, x.acao_governo_codigo
