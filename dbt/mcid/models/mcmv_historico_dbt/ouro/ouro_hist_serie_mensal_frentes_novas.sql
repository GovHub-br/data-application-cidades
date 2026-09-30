{{ config(materialized="table") }}

-- OURO — série mensal cross-frente de Classe Média, Reforma Casa Brasil,
-- MCMV Cidades e Pró-Moradia, com quebra nacional/região/UF (change
-- criar-ouro-serie-historica-frentes-novas, design.md D1/D2/D3).
--
-- DEDUP POR CHAVE DE NEGÓCIO ANTES DE AGRUPAR POR MÊS (D1), mesma disciplina
-- já provada em prata_relog_frentes_novas_resumo_mes (indicadores_mcmv_dbt):
-- as pratas de origem gravam dt_referencia como a data do SNAPSHOT semanal, e
-- o mesmo contrato reaparece em dezenas de snapshots sucessivos. Cada CTE
-- abaixo primeiro deduplica para 1 linha por contrato (row_number() por
-- dt_referencia desc) e só então deriva mes_referencia de dt_contratacao.
-- NÃO agrupar por dt_referencia sem reler o design.md D1 — é o mesmo bug que
-- prata_relog_frentes_novas_resumo_mes existe para evitar (Classe Média
-- infla ~24,8×, Reforma ~19,9× sem essa dedup prévia).
--
-- MCMV Cidades particiona a dedup também por fonte_bronze (D1, herdado de
-- prata_hist_mcmv_cidades_contrato) — GEFUS e SharePoint/emendas nunca
-- são casadas linha a linha entre si.
--
-- quantidade_uh só existe no contrato de Pró-Moradia — nulo para as demais
-- 3 frentes por ausência na fonte, não erro de cálculo (D2, task 1.7).
--
-- FNHIS fica de fora (Non-Goal desta change — dt_contratacao é NULL para a
-- maioria das linhas e há só 1 dt_referencia no dataset, sem série real).
{% set classe_media = ref('prata_hist_classe_media_contrato') %}
{% set reforma = ref('prata_hist_reforma_casa_brasil_contrato') %}
{% set mcmv_cidades = ref('prata_hist_mcmv_cidades_contrato') %}
{% set pro_moradia = ref('prata_hist_pro_moradia_contrato') %}
{% set regiao_uf = ref('dominio_regiao_uf') %}

with

    classe_media_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            uf,
            row_number() over (
                partition by nu_contrato order by dt_referencia desc
            ) as rn
        from {{ classe_media }}
    ),

    classe_media_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            upper(trim(uf)) as uf,
            valor_contratado,
            cast(null as bigint) as quantidade_uh
        from classe_media_rn
        where rn = 1
    ),

    reforma_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            uf,
            row_number() over (
                partition by nu_contrato order by dt_referencia desc
            ) as rn
        from {{ reforma }}
    ),

    reforma_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            upper(trim(uf)) as uf,
            valor_contratado,
            cast(null as bigint) as quantidade_uh
        from reforma_rn
        where rn = 1
    ),

    -- MCMV Cidades: janela curta documentada (D3) — 6 snapshots (jan-ago/2026)
    -- no ambiente em que esta change foi implementada. Sem teste de cobertura
    -- mínima por mês (schema.yml).
    mcmv_cidades_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            uf,
            row_number() over (
                partition by fonte_bronze, nu_contrato order by dt_referencia desc
            ) as rn
        from {{ mcmv_cidades }}
    ),

    mcmv_cidades_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            upper(trim(uf)) as uf,
            valor_contratado,
            cast(null as bigint) as quantidade_uh
        from mcmv_cidades_rn
        where rn = 1
    ),

    pro_moradia_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            uf,
            quantidade_uh,
            row_number() over (
                partition by codigo_contrato order by dt_referencia desc
            ) as rn
        from {{ pro_moradia }}
    ),

    pro_moradia_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            upper(trim(uf)) as uf,
            valor_contratado,
            quantidade_uh
        from pro_moradia_rn
        where rn = 1
    ),

    unioned as (
        select * from classe_media_dedup
        union all
        select * from reforma_dedup
        union all
        select * from mcmv_cidades_dedup
        union all
        select * from pro_moradia_dedup
    ),

    -- Contratos sem dt_contratacao não entram na série (D1, mesma decisão do
    -- reloginho). UF sem correspondência no seed não é descartada — só fica
    -- sem regiao_sigla/regiao_nome (D2).
    com_regiao as (
        select
            u.frente_mcmv,
            u.mes_referencia,
            u.uf,
            u.valor_contratado,
            u.quantidade_uh,
            r.regiao_sigla,
            r.regiao_nome
        from unioned as u
        left join {{ regiao_uf }} as r on u.uf = upper(trim(r.uf))
        where u.mes_referencia is not null
    ),

    agg as (
        select
            frente_mcmv,
            mes_referencia,
            case
                when grouping(uf) = 0
                then 'uf'
                when grouping(regiao_sigla) = 0
                then 'regiao'
                else 'nacional'
            end as nivel_agregacao,
            -- uf carrega a chave geográfica do PRÓPRIO nível desta linha (UF
            -- real no nível 'uf', sigla de região no nível 'regiao', 'BR' no
            -- 'nacional') — precisa ser assim para (frente_mcmv,
            -- mes_referencia, nivel_agregacao, uf) ser de fato único: com
            -- 'BR' fixo em 'regiao' e 'nacional', as 5 linhas de região do
            -- mesmo mês colidiriam na mesma combinação (regiao_sigla não
            -- está no grão do teste). nivel_agregacao sempre desambigua uma
            -- eventual colisão de sigla entre UF e região (ex. 'SE' é Sergipe
            -- e também Sudeste).
            case
                when grouping(uf) = 0
                then uf
                when grouping(regiao_sigla) = 0
                then regiao_sigla
                else 'BR'
            end as uf,
            case
                when grouping(regiao_sigla) = 0
                then regiao_sigla
                when grouping(uf) = 0
                then max(regiao_sigla)
                else 'BR'
            end as regiao_sigla,
            case
                when grouping(regiao_sigla) = 0 or grouping(uf) = 0
                then max(regiao_nome)
            end as regiao_nome,
            count(*) as n_contratos,
            sum(valor_contratado) as valor_contratado,
            cast(sum(quantidade_uh) as bigint) as quantidade_uh
        from com_regiao
        group by
            grouping sets (
                (frente_mcmv, mes_referencia),
                (frente_mcmv, mes_referencia, regiao_sigla),
                (frente_mcmv, mes_referencia, uf)
            )
    )

select *
from agg
order by frente_mcmv, mes_referencia, nivel_agregacao, regiao_sigla, uf
