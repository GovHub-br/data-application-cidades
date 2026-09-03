{{ config(materialized="table") }}

-- Gold: Execução Física x Financeira (Rural) — uma linha por APF × mês da série financeira.
--
-- A execução física do Rural NÃO é uma série: a bronze é full refresh do arquivo mensal
-- mais recente, então existe UMA medição por empreendimento, com data de referência. Ela
-- sai repetida em todos os meses, para ser desenhada como linha de referência sobre a
-- série financeira — a única das duas que tem história.
--
-- A física sai da coluna consolidada da silver_empreendimento, a mesma que alimenta a
-- ficha, para as duas telas não discordarem.
--
-- Ausência de dado é NULL, nunca 0,0%: não há série financeira para ~89% da carteira, e
-- afirmar zero ali contradiz a ficha. Empreendimento sem liberação registrada aparece com
-- uma linha só, no mês da medição física, com o financeiro nulo.

with
    -- Série financeira: esta sim é temporal, uma linha por APF × mês de liberação.
    financeira as (
        select
            apf,
            mes,
            pct_executado_financeiro,
            vr_acumulado
        from {{ ref("gold_evolucao_financeira") }}
    ),

    -- Medição física consolidada: uma linha por APF, com a procedência.
    fisica as (
        select
            apf,
            municipio,
            uf,
            empreendimento_nome,
            percentual_execucao_fisica,
            fonte_execucao_fisica,
            dt_referencia_execucao_fisica,
            -- Posição de desembolso informada pelos prioritários (ESTOQUE). A série
            -- financeira desta gold é FLUXO e pode estar incompleta — no 29712236 o
            -- estoque diz 100% e a série soma 75,7%. As duas ficam no mesmo registro
            -- para o gráfico poder mostrar a série sem contradizer o card da ficha.
            case
                when coalesce(valor_contratado, 0) > 0
                then round((valor_desembolsado / valor_contratado) * 100, 2)
            end as pct_financeiro_estoque
        from {{ ref("silver_empreendimento") }}
    ),

    -- Eixo de meses por APF. Quem tem série financeira usa os meses dela; quem não tem
    -- entra com um mês só, o da própria medição física, para não desaparecer do gráfico
    -- nem aparecer com zero.
    meses as (
        select apf, mes from financeira

        union

        select
            f.apf,
            date_trunc('month', f.dt_referencia_execucao_fisica) as mes
        from fisica f
        where f.dt_referencia_execucao_fisica is not null
          and not exists (select 1 from financeira x where x.apf = f.apf)
    )

select
    m.apf,
    concat(
        m.apf,
        ' - ',
        f.municipio, '/', f.uf,
        ' - ',
        upper(f.empreendimento_nome)
    ) as apf_municipio_empreendimento,
    to_char(m.mes, 'YYYY-MM-DD') as mes,

    -- Financeiro: acumulado até o mês. NULL onde não há liberação registrada — é o que
    -- desenha buraco no gráfico em vez de mentir zero.
    fin.pct_executado_financeiro,
    fin.vr_acumulado,

    -- Física: a medição corrente, constante ao longo do eixo, com a data que a produziu.
    f.percentual_execucao_fisica as pct_obra_realizada,
    f.pct_financeiro_estoque,
    f.fonte_execucao_fisica,
    f.dt_referencia_execucao_fisica,

    -- Permite o dashboard distinguir "não desembolsou" de "não sabemos se desembolsou".
    exists (select 1 from financeira x where x.apf = m.apf) as tem_serie_financeira

from meses m
inner join fisica f on m.apf = f.apf
left join financeira fin on fin.apf = m.apf and fin.mes = m.mes
