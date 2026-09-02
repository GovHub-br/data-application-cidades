{{ config(materialized="table") }}

-- Gold: Execução Física x Financeira (Rural) — uma linha por APF × mês da série financeira.
--
-- O QUE MUDOU E POR QUÊ
--
-- A versão anterior tinha três defeitos que se somavam para produzir números falsos:
--
-- 1. `coalesce(pct, 0.0)` nos dois eixos. Quando não há série financeira para o APF — e não
--    há para ~89% da carteira, porque o MONIT_MOV_FINANC_RURAL cobre pouco e o INT055 só
--    cobre PNHR — o gráfico mostrava "0,0% executado" ao lado de uma ficha dizendo
--    R$ 280.686 desembolsados. Ausência de dado virava afirmação de zero.
--
-- 2. A física vinha direto da silver_obra_mensal, enquanto a ficha vinha da
--    silver_empreendimento. Duas fontes para o mesmo indicador, e elas discordavam: 100%
--    aqui, 33,9% lá, no mesmo empreendimento e na mesma tela. Agora as duas leem a mesma
--    coluna consolidada, que escolhe a fonte pela data de medição.
--
-- 3. O mês da física era `date_trunc('month', dt_alteracao_situacao)` — a data em que a
--    SITUAÇÃO mudou, não a competência da medição. No 63665048 isso jogava uma medição de
--    agosto/2026 em março/2026, num mês onde não havia liberação nenhuma, e o full outer
--    join gerava uma linha órfã que o coalesce então zerava.
--
-- COMO É AGORA
--
-- A execução física do Rural NÃO é uma série: a bronze é full refresh do arquivo mensal
-- mais recente, então existe UMA medição por empreendimento, com data de referência. Ela
-- sai repetida em todos os meses, para ser desenhada como linha de referência sobre a
-- série financeira — que é a única das duas que tem história de verdade.
--
-- ATENÇÃO NO SUPERSET: a física deixa de ser uma barra num mês e passa a ser um valor
-- constante ao longo do eixo, e o financeiro passa a ter buracos onde não há dado (antes
-- vinham zeros). Empreendimento sem nenhuma liberação registrada aparece com uma linha só,
-- no mês da medição física, com o financeiro nulo.

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
