{{ config(materialized="table") }}

-- GOLD — serie historica mensal de EMPREENDIMENTOS por SITUACAO DE OBRA.
--
-- Une as 3 silvers historicas por frente (FAR / FDS / Rural) e agrega por
-- (mes, frente_mcmv, situacao_canonica, nivel_geografico, uf, regiao_sigla)
-- com nivel_geografico in {nacional, regiao, uf} via GROUPING SETS.
--
-- Duas familias de metrica no mesmo grao (D4 da change
-- serie-historica-situacao-obra-regiao):
-- ESTOQUE  n_empreendimentos (APF distintos na situacao naquele mes) e uh
-- (soma de quantidade_uh);
-- FLUXO    entradas / saidas da situacao no mes, por
-- lag(situacao_canonica) over (partition by frente_mcmv, apf
-- order by mes). A primeira observacao de um APF NAO conta como
-- entrada (nao ha situacao anterior). entradas de `paralisada` e a
-- metrica-chave da #59.
--
-- CUIDADO: a virada de feed SFTP -> SNH (2024-06+) desloca o estoque de
-- `paralisada` para baixo e gera rajada de `saidas` sem retomada real. O gold
-- NAO suaviza; expoe `fonte_serie` predominante do mes (por frente) para o
-- consumidor tratar 2024-06..2024-11 como quebra de serie. Ver o doc de entrega.
--
-- NAO somar `entradas` / `saidas` entre `nivel_geografico` (triplica: cada
-- transicao de APF acontece numa unica UF).
--
-- Cobertura: 2019-12 -> ultimo mes das silvers por frente. Serie pre-2019
-- (situacao_derivada) fica FORA nesta fase (Open Question 4); todas as linhas
-- sao fonte_situacao = 'reportada'.
--
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`.
with

    unioned as (
        select
            frente_mcmv,
            apf,
            dt_referencia,
            date_trunc('month', dt_referencia)::date as mes,
            coalesce(upper(nullif(trim(uf), '')), 'ND') as uf,
            coalesce(regiao_sigla, 'ND') as regiao_sigla,
            coalesce(regiao_nome, 'ND') as regiao_nome,
            situacao_canonica,
            quantidade_uh,
            fonte_serie
        from {{ ref('silver_mcmv_historico_empreendimento_far') }}
        where dt_referencia >= date '2019-12-01'
        union all
        select
            frente_mcmv,
            apf,
            dt_referencia,
            date_trunc('month', dt_referencia)::date as mes,
            coalesce(upper(nullif(trim(uf), '')), 'ND') as uf,
            coalesce(regiao_sigla, 'ND') as regiao_sigla,
            coalesce(regiao_nome, 'ND') as regiao_nome,
            situacao_canonica,
            quantidade_uh,
            fonte_serie
        from {{ ref('silver_mcmv_historico_empreendimento_fds') }}
        where dt_referencia >= date '2019-12-01'
        union all
        select
            frente_mcmv,
            apf,
            dt_referencia,
            date_trunc('month', dt_referencia)::date as mes,
            coalesce(upper(nullif(trim(uf), '')), 'ND') as uf,
            coalesce(regiao_sigla, 'ND') as regiao_sigla,
            coalesce(regiao_nome, 'ND') as regiao_nome,
            situacao_canonica,
            quantidade_uh,
            fonte_serie
        from {{ ref('silver_mcmv_historico_empreendimento_rural') }}
        where dt_referencia >= date '2019-12-01'
    ),

    -- Colapsa ao grao (frente, apf, mes): na janela sobreposta 2024-06..2024-11
    -- a silver mantem a linha SFTP (fim do mes) E a SNH (dia 1) com
    -- dt_referencia distintos. Prevalece a SNH (mesma precedencia D6/D8), para o
    -- mes ter 1 linha por APF — senao a lag() cria transicao falsa e o
    -- estoque/uh dobra.
    silvers as (
        select
            frente_mcmv,
            apf,
            mes,
            uf,
            regiao_sigla,
            regiao_nome,
            situacao_canonica,
            quantidade_uh,
            fonte_serie
        from unioned
        qualify
            row_number() over (
                partition by frente_mcmv, apf, mes
                order by case fonte_serie when 'snh' then 0 else 1 end, dt_referencia desc
            )
            = 1
    ),

    -- transicao por APF: a silver ja e grao empreendimento x mes (1 linha).
    transicoes as (
        select
            *,
            lag(situacao_canonica) over (
                partition by frente_mcmv, apf order by mes
            ) as situacao_anterior
        from silvers
    ),

    -- 1 linha de ESTOQUE por (mes, frente, situacao atual, geo) + 1 linha de
    -- SAIDA para a situacao anterior quando houve mudanca no mes.
    eventos as (
        select
            mes,
            frente_mcmv,
            uf,
            regiao_sigla,
            regiao_nome,
            situacao_canonica as situacao,
            apf,
            quantidade_uh,
            case
                when
                    situacao_anterior is not null
                    and situacao_canonica is distinct from situacao_anterior
                then 1
                else 0
            end as entrada,
            0 as saida
        from transicoes
        where situacao_canonica is not null
        union all
        select
            mes,
            frente_mcmv,
            uf,
            regiao_sigla,
            regiao_nome,
            situacao_anterior as situacao,
            cast(null as varchar) as apf,
            cast(null as bigint) as quantidade_uh,
            0 as entrada,
            1 as saida
        from transicoes
        where
            situacao_anterior is not null
            and situacao_canonica is distinct from situacao_anterior
    ),

    -- fonte_serie predominante do mes, por frente (para anotar a virada de feed).
    fonte_contagem as (
        select mes, frente_mcmv, fonte_serie, count(*) as n from silvers group by 1, 2, 3
    ),

    fonte_mes as (
        select
            mes,
            frente_mcmv,
            (array_agg(fonte_serie order by n desc, fonte_serie))[
                1
            ] as fonte_serie_predominante
        from fonte_contagem
        group by 1, 2
    ),

    agg as (
        select
            mes,
            frente_mcmv,
            situacao as situacao_canonica,
            case
                when grouping(uf) = 0
                then 'uf'
                when grouping(regiao_sigla) = 0
                then 'regiao'
                else 'nacional'
            end as nivel_geografico,
            case when grouping(uf) = 0 then uf else 'BR' end as uf,
            case
                when grouping(regiao_sigla) = 0
                then regiao_sigla
                when grouping(uf) = 0
                then max(regiao_sigla)
                else 'BR'
            end as regiao_sigla,
            case
                when grouping(regiao_sigla) = 0 or grouping(uf) = 0 then max(regiao_nome)
            end as regiao_nome,
            count(distinct apf) as n_empreendimentos,
            sum(quantidade_uh) as uh,
            sum(entrada) as entradas,
            sum(saida) as saidas
        from eventos
        group by
            grouping sets (
                (mes, frente_mcmv, situacao),
                (mes, frente_mcmv, situacao, regiao_sigla),
                (mes, frente_mcmv, situacao, uf)
            )
    )

select
    agg.mes,
    agg.frente_mcmv,
    agg.situacao_canonica,
    agg.nivel_geografico,
    agg.uf,
    agg.regiao_sigla,
    agg.regiao_nome,
    agg.n_empreendimentos,
    agg.uh,
    agg.entradas,
    agg.saidas,
    'reportada'::text as fonte_situacao,
    fonte_mes.fonte_serie_predominante as fonte_serie,
    current_timestamp as dt_gold
from agg
left join fonte_mes on agg.mes = fonte_mes.mes and agg.frente_mcmv = fonte_mes.frente_mcmv
order by
    agg.mes,
    agg.frente_mcmv,
    agg.situacao_canonica,
    agg.nivel_geografico,
    agg.regiao_sigla,
    agg.uf
