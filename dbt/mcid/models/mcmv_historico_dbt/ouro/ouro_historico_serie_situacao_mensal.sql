{{ config(materialized="table") }}

-- GOLD — serie historica mensal de EMPREENDIMENTOS por SITUACAO DE OBRA.
--
-- Une as 3 silvers historicas por frente (FAR / FDS / Rural) e agrega por
-- (mes, frente_mcmv, situacao_canonica, nivel_geografico, uf, regiao_sigla)
-- com nivel_geografico in {nacional, regiao, uf} via GROUPING SETS.
--
-- Duas familias de metrica no mesmo grao (D4 da change
-- serie-historica-situacao-obra-regiao):
-- ESTOQUE  n_empreendimentos (empreendimentos distintos na situacao naquele
-- mes, por chave_empreendimento = coalesce(id_empreendimento, apf) -- no FDS um
-- multi-fase conta 1; FAR/Rural chave = apf; change
-- id-empreendimento-eixo-historico) e uh (soma de quantidade_uh);
-- FLUXO    entradas / saidas da situacao no mes, por
-- lag(situacao_canonica) over (partition by frente_mcmv, chave_empreendimento
-- order by mes). A primeira observacao de um empreendimento NAO conta como
-- entrada (nao ha situacao anterior). entradas de `paralisada` e a
-- metrica-chave da #59.
--
-- CUIDADO: a virada de feed SFTP -> SNH (2024-06+) desloca o estoque de
-- `paralisada` para baixo e gera rajada de `saidas` sem retomada real. O gold
-- NAO suaviza; expoe `fonte_serie` predominante do mes (por frente) para o
-- consumidor tratar 2024-06..2024-11 como quebra de serie. Ver o doc de entrega.
--
-- CARRY-FORWARD (change enriquecer-quantidades-uh-e-sinais-obra-historico, D6):
-- as silvers por frente agora emitem linhas `fonte_valor = 'carregado'` nos
-- meses sem snapshot SNH (arrastam a ultima observacao ate 3 meses). Essas
-- linhas REPETEM `situacao_canonica` -> a lag() nao ve transicao -> nao geram
-- `entradas`/`saidas` falsas; so entram no ESTOQUE (n_empreendimentos / uh),
-- eliminando o serrote de +-128 k UH da serie agregada FAR. Por isso NAO se
-- filtra `fonte_valor = 'observado'` aqui.
--
-- NAO somar `entradas` / `saidas` entre `nivel_geografico` (triplica: cada
-- transicao de APF acontece numa unica UF).
--
-- Cobertura: 2019-12 -> ultimo mes das silvers por frente. Serie pre-2019
-- (situacao_derivada) fica FORA nesta fase (Open Question 4); todas as linhas
-- sao fonte_situacao = 'reportada'.
--
-- fonte_serie = 'obra_mensal' EXCLUIDA (change consolidar-schemas-historico-reloginho,
-- D2): as linhas so-de-obra (2026-04..07) nao tem situacao_canonica (o
-- co_situacao_obra e codigo cru, dominio a decodificar em follow-up) e gerariam
-- `saidas` falsas na lag(). O teto desta serie continua 2026-03 ate a decodificacao.
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
            fonte_serie,
            coalesce(id_empreendimento, apf) as chave_empreendimento,
            fase_empreendimento
        from {{ ref('prata_far_historico_empreendimento') }}
        where dt_referencia >= date '2019-12-01' and fonte_serie <> 'obra_mensal'
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
            fonte_serie,
            coalesce(id_empreendimento, apf) as chave_empreendimento,
            fase_empreendimento
        from {{ ref('prata_fds_historico_empreendimento') }}
        where dt_referencia >= date '2019-12-01' and fonte_serie <> 'obra_mensal'
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
            fonte_serie,
            coalesce(id_empreendimento, apf) as chave_empreendimento,
            fase_empreendimento
        from {{ ref('prata_rural_historico_empreendimento') }}
        where dt_referencia >= date '2019-12-01' and fonte_serie <> 'obra_mensal'
    ),

    -- Colapsa ao grao (frente, chave_empreendimento, mes). A re-dedup SFTP x SNH
    -- POR APF saiu daqui (change dedup-fonte-silver-historico, D4): a silver por
    -- frente ja entrega 1 linha por (frente, apf, mes), dt_referencia no 1o do
    -- mes, precedencia SNH resolvida. Para FAR/Rural (chave = apf, fase nula) o
    -- row_number() vira no-op.
    --
    -- O que RESTA e o desempate ENTRE APF-fases distintos do mesmo empreendimento
    -- FDS multi-fase (2-3 APFs de fase no mesmo mes). NAO e re-dedup de fonte --
    -- e a escolha de qual APF-fase representa o empreendimento no mes:
    --   1. o APF cuja observacao do mes vem do SNH (dados prioritarios por
    --      agente) prevalece sobre o APF que so o SFTP GEFUS reportou -- o
    --      `case fonte_serie` continua necessario AQUI (a silver so resolve
    --      fonte DENTRO de um APF, nao entre APFs). Sem ele o estoque de
    --      `nao_iniciada` do FDS cai e `em_obras` sobe (a fase Obra de um APF
    --      SFTP passa a ganhar da fase Projeto do APF SNH) -- muda a serie.
    --   2. depois, fase mais avancada (Desligamento > Obra > Projeto);
    --   3. depois, dt_referencia desc.
    -- Sem esse colapso a lag() cria transicao falsa (troca de APF Projeto->Obra)
    -- e o estoque/uh dobra.
    silvers as (
        select
            frente_mcmv,
            chave_empreendimento,
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
                partition by frente_mcmv, chave_empreendimento, mes
                order by
                    case fonte_serie when 'snh' then 0 else 1 end,
                    case fase_empreendimento
                        when 'Desligamento' then 0
                        when 'Obra' then 1
                        when 'Projeto' then 2
                        else 3
                    end,
                    dt_referencia desc
            )
            = 1
    ),

    -- transicao por empreendimento: silvers ja e grao empreendimento x mes.
    transicoes as (
        select
            *,
            lag(situacao_canonica) over (
                partition by frente_mcmv, chave_empreendimento order by mes
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
            chave_empreendimento,
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
            cast(null as varchar) as chave_empreendimento,
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
            count(distinct chave_empreendimento) as n_empreendimentos,
            -- cast p/ bigint: sum(bigint) -> HUGEINT (int128) no DuckDB, sem
            -- tipo no Postgres (modo C).
            -- Change: verificar-tipagem-silver-gold-historico.
            cast(sum(quantidade_uh) as bigint) as uh,
            cast(sum(entrada) as bigint) as entradas,
            cast(sum(saida) as bigint) as saidas
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
